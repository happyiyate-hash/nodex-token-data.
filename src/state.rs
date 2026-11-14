use crate::{config::AppConfig, providers::ProviderManager};

#[derive(Clone)]
pub struct AppState {
    pub providers: ProviderManager,
}

impl AppState {
    pub fn new(config: &AppConfig) -> Self {
        Self {
            providers: ProviderManager::new(&config.providers),
        }
    }
}
