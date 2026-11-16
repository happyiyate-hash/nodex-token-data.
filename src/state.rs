use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use tokio::sync::{broadcast, Mutex};
use tracing::{info, warn};

use crate::{
    config::AppConfig,
    market::MarketDataClient,
    market_engine::MarketEngine,
    models::Trade,
    providers::{ProviderClient, ProviderManager},
    storage::MarketStore,
    stream::StreamEvent,
};

const MAX_ACTIVE_MARKET_MINTS: usize = 100;
const MONITOR_GRACE: Duration = Duration::from_secs(300);
const CACHE_TTL: Duration = Duration::from_secs(600);

#[derive(Clone, Debug)]
struct MintLease {
    watchers: usize,
    inactive_at: Option<Instant>,
}
const STABLE_PROVIDER_CONNECTION: Duration = Duration::from_secs(60);

fn should_reset_provider_backoff(connection_duration: Duration) -> bool {
    connection_duration >= STABLE_PROVIDER_CONNECTION
}

#[derive(Clone)]
pub struct AppState {
    pub providers: ProviderManager,
    pub provider_client: ProviderClient,
    pub market_client: MarketDataClient,
    pub market_store: MarketStore,
    active_mints: Arc<Mutex<HashMap<String, MintLease>>>,
    market_engine: Arc<Mutex<MarketEngine>>,
    seeded_market_mints: Arc<Mutex<HashSet<String>>>,
    stream_sequence: Arc<AtomicU64>,
    pub stream_tx: broadcast::Sender<StreamEvent>,
}

impl AppState {
    pub async fn new(config: &AppConfig) -> Result<Self> {
        let providers = ProviderManager::new(&config.providers);
        let provider_client = ProviderClient::new(providers.clone())?;
        let market_client = MarketDataClient::new()?;
        let market_store = MarketStore::connect(&config.database_url).await?;
        let (stream_tx, _) = broadcast::channel(2048);

        Ok(Self {
            providers,
            provider_client,
            market_client,
            market_store,
            active_mints: Arc::new(Mutex::new(HashMap::new())),
            market_engine: Arc::new(Mutex::new(MarketEngine::default())),
            seeded_market_mints: Arc::new(Mutex::new(HashSet::new())),
            stream_sequence: Arc::new(AtomicU64::new(0)),
            stream_tx,
        })
    }

    /// Reserve a slot for a mint. When full, evict the oldest inactive
    /// mint (the one nearest its normal ten-minute cache expiry). Active
    /// watchers are never evicted. The old cache is deleted before the slot
    /// is reused so a returning client cannot race with cache deletion.
    pub async fn track_mint(&self, mint: &str) -> bool {
        let mut active = self.active_mints.lock().await;

        if let Some(lease) = active.get_mut(mint) {
            lease.watchers += 1;
            lease.inactive_at = None;
            return true;
        }

        if active.len() >= MAX_ACTIVE_MARKET_MINTS {
            let eviction_candidate = active
                .iter()
                .filter(|(_, lease)| lease.watchers == 0)
                .filter_map(|(candidate, lease)| {
                    lease
                        .inactive_at
                        .map(|inactive_at| (candidate.clone(), inactive_at))
                })
                .min_by_key(|(_, inactive_at)| *inactive_at)
                .map(|(candidate, _)| candidate);

            let Some(evicted_mint) = eviction_candidate else {
                return false;
            };

            if let Err(error) = self.market_store.delete_mint(&evicted_mint).await {
                warn!(
                    mint = %evicted_mint,
                    error = %error,
                    "Could not evict inactive market-data cache; keeping the slot reserved"
                );
                return false;
            }

            active.remove(&evicted_mint);
            self.seeded_market_mints.lock().await.remove(&evicted_mint);
            self.market_engine.lock().await.remove_token(&evicted_mint);
            info!(mint = %evicted_mint, replacement = %mint, "Evicted oldest inactive market-data token to free a slot");
        }

        active.insert(
            mint.to_owned(),
            MintLease {
                watchers: 1,
                inactive_at: None,
            },
        );
        true
    }

    pub async fn untrack_mint(&self, mint: &str) {
        let mut active = self.active_mints.lock().await;
        if let Some(lease) = active.get_mut(mint) {
            lease.watchers = lease.watchers.saturating_sub(1);
            if lease.watchers == 0 && lease.inactive_at.is_none() {
                lease.inactive_at = Some(Instant::now());
            }
        }
    }

    /// Return watched tokens and recently unwatched tokens still in their
    /// five-minute monitoring grace period.
    pub async fn subscribed_mints(&self) -> Vec<String> {
        let now = Instant::now();
        self.active_mints
            .lock()
            .await
            .iter()
            .filter(|(_, lease)| {
                lease.watchers > 0
                    || lease
                        .inactive_at
                        .is_some_and(|at| now.duration_since(at) < MONITOR_GRACE)
            })
            .map(|(mint, _)| mint.clone())
            .collect()
    }

    async fn cleanup_expired_mints(&self) {
        // Keep the lease lock while deleting so a reconnect cannot recreate
        // the same mint between slot removal and cache cleanup.
        let mut active = self.active_mints.lock().await;
        let expired = active
            .iter()
            .filter(|(_, lease)| {
                lease.watchers == 0
                    && lease
                        .inactive_at
                        .is_some_and(|at| Instant::now().duration_since(at) >= CACHE_TTL)
            })
            .map(|(mint, _)| mint.clone())
            .collect::<Vec<_>>();

        for mint in expired {
            match self.market_store.delete_mint(&mint).await {
                Ok(()) => {
                    active.remove(&mint);
                    self.seeded_market_mints.lock().await.remove(&mint);
                    self.market_engine.lock().await.remove_token(&mint);
                }
                Err(error) => {
                    warn!(mint, error = %error, "Could not remove expired market-data cache");
                }
            }
        }
    }

    /// Refreshes slower external reference metrics as a fallback. Confirmed
    /// on-chain swaps, not this worker, are the primary source of live price ticks.
    /// The shared service monitors at most 100 distinct token mints globally.
    pub async fn run_market_stream(self) {
        let mut interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            interval.tick().await;
            self.cleanup_expired_mints().await;
            let mints = self.subscribed_mints().await;

            for batch in mints.chunks(30) {
                match self.market_client.fetch_quotes(batch).await {
                    Ok(quotes) => {
                        for quote in quotes.into_values() {
                            let timestamp_ms = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as u64;
                            let sequence = self.stream_sequence.fetch_add(1, Ordering::Relaxed) + 1;
                            let mut data = match serde_json::to_value(&quote) {
                                Ok(value) => value,
                                Err(_) => continue,
                            };
                            // External quotes refresh slower reference fields, but must
                            // never overwrite a more recent confirmed on-chain trade price.
                            let (onchain_price, onchain_market_cap) = {
                                let engine = self.market_engine.lock().await;
                                (
                                    engine.latest_price(&quote.mint),
                                    engine.market_cap_usd(&quote.mint),
                                )
                            };
                            if let Some(onchain_price) = onchain_price {
                                data["price_usd"] = serde_json::json!(onchain_price);
                                if let Some(market_cap) = onchain_market_cap {
                                    data["market_cap_usd"] = serde_json::json!(market_cap);
                                }
                                data["price_source"] = serde_json::json!("confirmed_onchain");
                            } else {
                                data["price_source"] = serde_json::json!("external_reference");
                            }

                            self.publish(StreamEvent {
                                event: "market_update".to_owned(),
                                mint: quote.mint,
                                topic: "token.market".to_owned(),
                                sequence,
                                timestamp_ms,
                                data,
                            });
                        }
                    }
                    Err(error) => {
                        warn!(error = %error, "Market-data polling cycle failed");
                    }
                }
            }
        }
    }

    async fn seed_market_engine(&self, mint: &str) -> Result<()> {
        let mut seeded = self.seeded_market_mints.lock().await;
        if seeded.contains(mint) {
            return Ok(());
        }
        let mut history = Vec::new();
        for timeframe in ["1S", "5S", "10S", "1M", "5M", "15M", "30M", "1H", "24H"] {
            let candles = self.market_store.load_candles(mint, timeframe, 1).await?;
            if let Some(candle) = candles.into_iter().next() {
                history.push((timeframe, candle));
            }
        }
        let total_supply = self.provider_client.fetch_token_supply(mint).await.ok();
        let mut engine = self.market_engine.lock().await;
        for (timeframe, candle) in history {
            engine.seed_candle(mint, timeframe, candle);
        }
        if let Some(supply) = total_supply {
            engine.set_total_supply(mint, supply);
        }
        seeded.insert(mint.to_owned());
        Ok(())
    }

    pub async fn publish_confirmed_trade(&self, mint: &str, trade: Trade) -> Result<()> {
        let mut trade = trade;
        if trade.price_usd.is_none() {
            if let (Some(token_amount), Some(volume_usd)) = (trade.token_amount, trade.volume_usd) {
                let derived_price = volume_usd / token_amount;
                if token_amount.is_finite()
                    && token_amount > 0.0
                    && derived_price.is_finite()
                    && derived_price > 0.0
                {
                    trade.price_usd = Some(derived_price);
                }
            }
        }
        if trade.volume_usd.is_none() {
            if let (Some(token_amount), Some(price_usd)) = (trade.token_amount, trade.price_usd) {
                let derived_volume = token_amount * price_usd;
                if token_amount.is_finite()
                    && token_amount > 0.0
                    && derived_volume.is_finite()
                    && derived_volume > 0.0
                {
                    trade.volume_usd = Some(derived_volume);
                }
            }
        }
        self.seed_market_engine(mint).await?;
        if !self
            .market_store
            .record_confirmed_trade(mint, &trade)
            .await?
        {
            return Ok(());
        }
        let (candle_updates, market_cap_usd) = {
            let mut engine = self.market_engine.lock().await;
            let candles = engine.apply_trade(mint, &trade);
            (candles, engine.market_cap_usd(mint))
        };
        let timestamp_ms = trade.timestamp_ms.max(0) as u64;
        let trade_cursor = self.market_store
            .load_trade_cursor(mint, &trade.signature, trade.event_index)
            .await?;
        let mut data = serde_json::to_value(&trade)?;
        data["cursor"] = serde_json::json!(trade_cursor);
        let sequence = self.stream_sequence.fetch_add(1, Ordering::Relaxed) + 1;
        self.publish(StreamEvent {
            event: "trade_update".into(),
            mint: mint.into(),
            topic: "token.trades".into(),
            sequence,
            timestamp_ms,
            data: data.clone(),
        });
        let sequence = self.stream_sequence.fetch_add(1, Ordering::Relaxed) + 1;
        self.publish(StreamEvent {
            event: "activity_update".into(),
            mint: mint.into(),
            topic: "token.activity".into(),
            sequence,
            timestamp_ms,
            data,
        });
        // A confirmed swap is the lowest-latency trustworthy price observation.
        // Publish it immediately with rolling confirmed buy/sell totals rather than
        // waiting for the slower public quote refresh worker.
        if let Ok((buy_volume, sell_volume, buy_count, sell_count)) =
            self.market_store.load_trade_totals_24h(mint).await
        {
            let buy_pressure_pct = match (buy_volume, sell_volume) {
                (Some(buy), Some(sell)) if buy + sell > 0.0 => Some(buy / (buy + sell) * 100.0),
                (Some(buy), None) if sell_count == 0 && buy > 0.0 => Some(100.0),
                (None, Some(sell)) if buy_count == 0 && sell > 0.0 => Some(0.0),
                _ => None,
            };
            let mut market = serde_json::json!({
                "buy_volume_usd": buy_volume,
                "sell_volume_usd": sell_volume,
                "buy_pressure_pct": buy_pressure_pct,
                "confirmed_buy_count_24h": buy_count,
                "confirmed_sell_count_24h": sell_count,
                "confirmed_transactions_24h": buy_count + sell_count,
                "volume_24h_usd": match (buy_volume, sell_volume) {
                    (Some(buy), Some(sell)) => Some(buy + sell),
                    (Some(buy), None) if sell_count == 0 => Some(buy),
                    (None, Some(sell)) if buy_count == 0 => Some(sell),
                    _ => None,
                },
                "market_cap_usd": market_cap_usd,
            });
            if let Some(price) = trade
                .price_usd
                .filter(|price| price.is_finite() && *price > 0.0)
            {
                market["price_usd"] = serde_json::json!(price);
            }
            let sequence = self.stream_sequence.fetch_add(1, Ordering::Relaxed) + 1;
            self.publish(StreamEvent {
                event: "market_update".into(),
                mint: mint.into(),
                topic: "token.market".into(),
                sequence,
                timestamp_ms,
                data: market,
            });
        }
        for (timeframe, candle) in candle_updates {
            let sequence = self.stream_sequence.fetch_add(1, Ordering::Relaxed) + 1;
            self.publish(StreamEvent {
                event: "candle_update".into(),
                mint: mint.into(),
                topic: "token.chart".into(),
                sequence,
                timestamp_ms,
                data: serde_json::json!({"timeframe": timeframe, "candle": candle}),
            });
        }
        Ok(())
    }

    pub async fn publish_chart_snapshot(&self, mint: &str, timeframe: &str) -> Result<()> {
        // Timeframe changes are served from the local durable candle store.
        // They must not trigger another third-party history-provider request.
        let points = self
            .market_store
            .load_candles(mint, timeframe, 5000)
            .await?;
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let sequence = self.stream_sequence.fetch_add(1, Ordering::Relaxed) + 1;
        self.publish(StreamEvent {
            event: "chart_snapshot".into(),
            mint: mint.into(),
            topic: "token.chart".into(),
            sequence,
            timestamp_ms,
            data: serde_json::json!({"timeframe": timeframe, "points": points}),
        });
        Ok(())
    }

    /// Refreshes holder concentration for currently subscribed token details.
    /// The interval is intentionally slow to avoid turning holder snapshots into
    /// a high-volume per-account RPC subscription.
    pub async fn run_holder_stream(self) {
        let mut refresh = tokio::time::interval(Duration::from_secs(300));
        loop {
            refresh.tick().await;
            for mint in self.subscribed_mints().await {
                match self.provider_client.fetch_holders_for_mint(&mint).await {
                    Ok(holders) => {
                        let timestamp_ms = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;
                        let sequence = self.stream_sequence.fetch_add(1, Ordering::Relaxed) + 1;
                        match serde_json::to_value(holders) {
                            Ok(data) => self.publish(StreamEvent {
                                event: "holders_update".into(),
                                mint: mint.clone(),
                                topic: "token.holders".into(),
                                sequence,
                                timestamp_ms,
                                data,
                            }),
                            Err(error) => {
                                warn!(mint, error = %error, "Could not serialize holder update")
                            }
                        }
                    }
                    Err(error) => warn!(mint, error = %error, "Holder refresh failed"),
                }
            }
        }
    }

    /// Keeps one provider stream active at a time. Helius is preferred; FluxRPC
    /// is attempted immediately when every Helius socket fails. A failed sweep
    /// backs off exponentially before retrying the preferred provider.
    pub async fn run_provider_streams(self) {
        let mut retry_delay = Duration::from_secs(1);
        const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

        loop {
            let mut connected_attempt = false;

            for provider in ["helius", "fluxrpc"] {
                let endpoints = self.providers.websocket_endpoints(provider);
                for endpoint in endpoints {
                    connected_attempt = true;
                    info!(provider, "Attempting provider WebSocket connection");
                    let connected_at = Instant::now();
                    let result = match provider {
                        "helius" => crate::providers::helius_ws::run_once(&self, &endpoint).await,
                        "fluxrpc" => crate::providers::fluxrpc_ws::run_once(&self, &endpoint).await,
                        _ => continue,
                    };
                    let connection_duration = connected_at.elapsed();
                    if should_reset_provider_backoff(connection_duration) {
                        retry_delay = Duration::from_secs(1);
                        info!(
                            provider,
                            duration_seconds = connection_duration.as_secs(),
                            "Provider connection was stable; resetting reconnect backoff"
                        );
                    }
                    if let Err(error) = result {
                        warn!(provider, error = %error, "Provider WebSocket ended; trying next configured endpoint");
                    }
                }
            }

            if !connected_attempt {
                warn!("No provider WebSocket endpoints are configured");
            } else {
                warn!(
                    delay_seconds = retry_delay.as_secs(),
                    "All provider WebSocket attempts ended; retrying from primary"
                );
            }
            tokio::time::sleep(retry_delay).await;
            retry_delay = (retry_delay * 2).min(MAX_RETRY_DELAY);
        }
    }

    pub fn publish(&self, event: StreamEvent) {
        let _ = self.stream_tx.send(event);
    }
}

#[cfg(test)]
mod tests {
    use super::{AppState, MAX_ACTIVE_MARKET_MINTS};
    use crate::config::AppConfig;
    use std::{net::SocketAddr, time::Duration};

    async fn test_state() -> AppState {
        let config = AppConfig {
            bind_addr: "127.0.0.1:8081".parse::<SocketAddr>().unwrap(),
            database_url: "sqlite::memory:".to_owned(),
            providers: Vec::new(),
        };
        AppState::new(&config).await.unwrap()
    }

    #[test]
    fn provider_backoff_resets_only_after_stable_connection() {
        assert!(!super::should_reset_provider_backoff(Duration::from_secs(
            59
        )));
        assert!(super::should_reset_provider_backoff(Duration::from_secs(
            60
        )));
        assert!(super::should_reset_provider_backoff(Duration::from_secs(
            120
        )));
    }

    #[tokio::test]
    async fn tracked_mints_use_reference_counts() {
        let state = test_state().await;
        assert!(state.track_mint("mint-a").await);
        assert!(state.track_mint("mint-a").await);
        assert_eq!(state.subscribed_mints().await, vec!["mint-a".to_owned()]);

        state.untrack_mint("mint-a").await;
        assert_eq!(state.subscribed_mints().await, vec!["mint-a".to_owned()]);

        state.untrack_mint("mint-a").await;
        assert_eq!(state.subscribed_mints().await, vec!["mint-a".to_owned()]);
    }

    #[tokio::test]
    async fn untracking_unknown_mint_is_safe() {
        let state = test_state().await;
        state.untrack_mint("unknown").await;
        assert!(state.subscribed_mints().await.is_empty());
    }

    #[tokio::test]
    async fn evicts_oldest_inactive_mint_when_capacity_is_full() {
        let state = test_state().await;
        for index in 0..MAX_ACTIVE_MARKET_MINTS {
            assert!(state.track_mint(&format!("mint-{index}")).await);
        }

        // This mint is inactive but still within its normal cache grace period.
        state.untrack_mint("mint-0").await;
        assert_eq!(
            state.subscribed_mints().await.len(),
            MAX_ACTIVE_MARKET_MINTS
        );

        assert!(state.track_mint("mint-replacement").await);
        let remaining = state.subscribed_mints().await;
        assert_eq!(remaining.len(), MAX_ACTIVE_MARKET_MINTS);
        assert!(!remaining.iter().any(|mint| mint == "mint-0"));
        assert!(remaining.iter().any(|mint| mint == "mint-replacement"));

        // Existing active mints remain protected.
        assert!(!state.track_mint("mint-another").await);
    }

    #[tokio::test]
    async fn caps_unique_active_mints_at_one_hundred() {
        let state = test_state().await;
        for index in 0..MAX_ACTIVE_MARKET_MINTS {
            assert!(state.track_mint(&format!("mint-{index}")).await);
        }
        assert!(!state.track_mint("mint-over-limit").await);
        assert_eq!(
            state.subscribed_mints().await.len(),
            MAX_ACTIVE_MARKET_MINTS
        );
    }
}
