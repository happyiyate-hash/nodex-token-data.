use std::{collections::HashMap, sync::{Arc, Mutex}, time::{Duration, Instant}};
use anyhow::Result;
use tokio::sync::broadcast;
use crate::{config::AppConfig, providers::{ProviderClient, ProviderManager}, stream::StreamEvent};

#[derive(Clone)]
pub struct AppState {
    pub providers: ProviderManager,
    pub provider_client: ProviderClient,
    pub api_key: Arc<String>,
    pub tickets: Arc<Mutex<HashMap<String, Instant>>>,
    pub stream_tx: broadcast::Sender<StreamEvent>,
}
impl AppState {
    pub fn new(config: &AppConfig) -> Result<Self> {
        let providers = ProviderManager::new(&config.providers);
        let provider_client = ProviderClient::new(providers.clone())?;
        let (stream_tx, _) = broadcast::channel(2048);
        Ok(Self {
            providers, provider_client, api_key: Arc::new(config.api_key.clone()),
            tickets: Arc::new(Mutex::new(HashMap::new())), stream_tx,
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
        self.tickets.lock().ok().and_then(|mut tickets| tickets.remove(ticket))
            .is_some_and(|expires| expires > Instant::now())
    }
    #[allow(dead_code)] // Called by provider/indexer workers as those pipelines are added.\n    pub fn publish(&self, event: StreamEvent) {
        let _ = self.stream_tx.send(event);
    }
}
