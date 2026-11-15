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
    stream::StreamEvent,
};

#[derive(Clone)]
pub struct AppState {
    pub providers: ProviderManager,
    pub provider_client: ProviderClient,
    pub market_client: MarketDataClient,
    pub api_key: Arc<String>,
    pub tickets: Arc<Mutex<HashMap<String, Instant>>>,
    active_mints: Arc<Mutex<HashMap<String, usize>>>,
    stream_sequence: Arc<AtomicU64>,
    pub stream_tx: broadcast::Sender<StreamEvent>,
}

impl AppState {
    pub fn new(config: &AppConfig) -> Result<Self> {
        let providers = ProviderManager::new(&config.providers);
        let provider_client = ProviderClient::new(providers.clone())?;
        let market_client = MarketDataClient::new()?;
        let (stream_tx, _) = broadcast::channel(2048);

        Ok(Self {
            providers,
            provider_client,
            market_client,
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

    pub fn track_mint(&self, mint: &str) {
        if let Ok(mut active) = self.active_mints.lock() {
            *active.entry(mint.to_owned()).or_insert(0) += 1;
        }
    }

    pub fn untrack_mint(&self, mint: &str) {
        if let Ok(mut active) = self.active_mints.lock() {
            if let Some(count) = active.get_mut(mint) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    active.remove(mint);
                }
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
    use super::AppState;
    use crate::config::AppConfig;
    use std::net::SocketAddr;

    fn test_state() -> AppState {
        let config = AppConfig {
            bind_addr: "127.0.0.1:8081".parse::<SocketAddr>().unwrap(),
            api_key: "a".repeat(32),
            providers: Vec::new(),
        };
        AppState::new(&config).unwrap()
    }

    #[test]
    fn tracked_mints_use_reference_counts() {
        let state = test_state();
        state.track_mint("mint-a");
        state.track_mint("mint-a");
        assert_eq!(state.active_mints(), vec!["mint-a".to_owned()]);

        state.untrack_mint("mint-a");
        assert_eq!(state.active_mints(), vec!["mint-a".to_owned()]);

        state.untrack_mint("mint-a");
        assert!(state.active_mints().is_empty());
    }

    #[test]
    fn untracking_unknown_mint_is_safe() {
        let state = test_state();
        state.untrack_mint("unknown");
        assert!(state.active_mints().is_empty());
    }
}
