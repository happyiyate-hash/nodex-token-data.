use std::{
    collections::HashMap,
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
    models::Trade,
    providers::{ProviderClient, ProviderManager},
    storage::MarketStore,
    stream::StreamEvent,
};

const MAX_ACTIVE_MARKET_MINTS: usize = 10;
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
                    lease.inactive_at.map(|inactive_at| (candidate.clone(), inactive_at))
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
        let expired = {
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
            for mint in &expired {
                active.remove(mint);
            }
            expired
        };

        for mint in expired {
            if let Err(error) = self.market_store.delete_mint(&mint).await {
                warn!(mint, error = %error, "Could not remove expired market-data cache");
            }
        }
    }

    /// Polls the public market-data endpoint in batches and republishes each
    /// verified quote through the app's existing multiplexed WebSocket bus.
    /// The shared service monitors at most ten distinct token mints globally.
    pub async fn run_market_stream(self) {
        let mut interval = tokio::time::interval(Duration::from_secs(5));

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
                            let data = match serde_json::to_value(&quote) {
                                Ok(value) => value,
                                Err(_) => continue,
                            };

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

    pub async fn publish_confirmed_trade(&self, mint: &str, trade: Trade) -> Result<()> {
        if !self
            .market_store
            .record_confirmed_trade(mint, &trade)
            .await?
        {
            return Ok(());
        }
        let timestamp_ms = trade.timestamp_ms.max(0) as u64;
        let data = serde_json::to_value(&trade)?;
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
        for timeframe in ["1S", "5S", "10S", "1M", "5M", "15M", "30M", "1H", "24H"] {
            if let Ok(points) = self.market_store.load_candles(mint, timeframe, 1).await {
                if let Some(candle) = points.last() {
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
            }
        }
        Ok(())
    }

    pub async fn publish_chart_snapshot(&self, mint: &str, timeframe: &str) -> Result<()> {
        let quote = self.market_client.fetch_quote(mint).await.ok().flatten();
        let pair = quote
            .as_ref()
            .and_then(|value| value.pair_address.as_deref());
        let points = crate::components::chart_data::fetch(
            mint,
            timeframe,
            pair,
            &self.market_client,
            &self.market_store,
        )
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
        assert_eq!(state.subscribed_mints(), vec!["mint-a".to_owned()]);
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
        assert_eq!(state.subscribed_mints().await.len(), MAX_ACTIVE_MARKET_MINTS);

        assert!(state.track_mint("mint-replacement").await);
        let remaining = state.subscribed_mints().await;
        assert_eq!(remaining.len(), MAX_ACTIVE_MARKET_MINTS);
        assert!(!remaining.iter().any(|mint| mint == "mint-0"));
        assert!(remaining.iter().any(|mint| mint == "mint-replacement"));

        // Existing active mints remain protected.
        assert!(!state.track_mint("mint-another").await);
    }

    #[tokio::test]
    async fn caps_unique_active_mints_at_ten() {
        let state = test_state().await;
        for index in 0..MAX_ACTIVE_MARKET_MINTS {
            assert!(state.track_mint(&format!("mint-{index}")).await);
        }
        assert!(!state.track_mint("mint-over-limit").await);
        assert_eq!(state.subscribed_mints().len(), MAX_ACTIVE_MARKET_MINTS);
    }
}
