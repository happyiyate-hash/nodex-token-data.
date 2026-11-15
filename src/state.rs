use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use tokio::sync::broadcast;
use tracing::warn;

use crate::{
    config::AppConfig,
    market::MarketDataClient,
    providers::{ProviderClient, ProviderManager},
    storage::MarketStore,
    stream::StreamEvent,
};

const MAX_ACTIVE_MARKET_MINTS: usize = 500;

#[derive(Clone)]
pub struct AppState {
    pub providers: ProviderManager,
    pub provider_client: ProviderClient,
    pub market_client: MarketDataClient,
    pub market_store: MarketStore,
    pub api_key: Arc<String>,
    pub tickets: Arc<Mutex<HashMap<String, Instant>>>,
    active_mints: Arc<Mutex<HashMap<String, usize>>>,
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
            api_key: Arc::new(config.api_key.clone()),
            tickets: Arc::new(Mutex::new(HashMap::new())),
            active_mints: Arc::new(Mutex::new(HashMap::new())),
            stream_sequence: Arc::new(AtomicU64::new(0)),
            stream_tx,
        })
    }

    pub fn issue_ws_ticket(&self) -> String {
        let ticket = uuid::Uuid::new_v4().to_string();
        if let Ok(mut tickets) = self.tickets.lock() {
            tickets.retain(|_, expires| *expires > Instant::now());
            tickets.insert(ticket.clone(), Instant::now() + Duration::from_secs(30));
        }
        ticket
    }

    pub fn consume_ws_ticket(&self, ticket: &str) -> bool {
        self.tickets
            .lock()
            .ok()
            .and_then(|mut tickets| tickets.remove(ticket))
            .is_some_and(|expires| expires > Instant::now())
    }

    pub fn track_mint(&self, mint: &str) -> bool {
        if let Ok(mut active) = self.active_mints.lock() {
            if let Some(count) = active.get_mut(mint) {
                *count += 1;
                return true;
            }
            if active.len() >= MAX_ACTIVE_MARKET_MINTS {
                return false;
            }
            active.insert(mint.to_owned(), 1);
            return true;
        }
        false
    }

    pub fn untrack_mint(&self, mint: &str) {
        if let Ok(mut active) = self.active_mints.lock() {
            let should_remove = if let Some(count) = active.get_mut(mint) {
                *count = count.saturating_sub(1);
                *count == 0
            } else {
                false
            };
            if should_remove {
                active.remove(mint);
            }
        }
    }

    fn active_mints(&self) -> Vec<String> {
        self.active_mints
            .lock()
            .map(|active| active.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Polls the public market-data endpoint in batches and republishes each
    /// verified quote through the app's existing multiplexed WebSocket bus.
    /// 500 active tokens require at most 17 batched requests per cycle.
    pub async fn run_market_stream(self) {
        let mut interval = tokio::time::interval(Duration::from_secs(5));

        loop {
            interval.tick().await;
            let mints = self.active_mints();

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

    pub fn publish(&self, event: StreamEvent) {
        let _ = self.stream_tx.send(event);
    }
}

#[cfg(test)]
mod tests {
    use super::{AppState, MAX_ACTIVE_MARKET_MINTS};
    use crate::config::AppConfig;
    use std::net::SocketAddr;

    async fn test_state() -> AppState {
        let config = AppConfig {
            bind_addr: "127.0.0.1:8081".parse::<SocketAddr>().unwrap(),
            api_key: "a".repeat(32),
            database_url: "sqlite::memory:".to_owned(),
            providers: Vec::new(),
        };
        AppState::new(&config).await.unwrap()
    }

    #[tokio::test]
    async fn tracked_mints_use_reference_counts() {
        let state = test_state().await;
        assert!(state.track_mint("mint-a"));
        assert!(state.track_mint("mint-a"));
        assert_eq!(state.active_mints(), vec!["mint-a".to_owned()]);

        state.untrack_mint("mint-a");
        assert_eq!(state.active_mints(), vec!["mint-a".to_owned()]);

        state.untrack_mint("mint-a");
        assert!(state.active_mints().is_empty());
    }

    #[tokio::test]
    async fn untracking_unknown_mint_is_safe() {
        let state = test_state().await;
        state.untrack_mint("unknown");
        assert!(state.active_mints().is_empty());
    }

    #[tokio::test]
    async fn caps_unique_active_mints_at_five_hundred() {
        let state = test_state().await;
        for index in 0..MAX_ACTIVE_MARKET_MINTS {
            assert!(state.track_mint(&format!("mint-{index}")));
        }
        assert!(!state.track_mint("mint-over-limit"));
        assert_eq!(state.active_mints().len(), MAX_ACTIVE_MARKET_MINTS);
    }
}
