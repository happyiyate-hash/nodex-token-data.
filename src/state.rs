use anyhow::Result;

use crate::{
    config::AppConfig,
    providers::{ProviderClient, ProviderManager},
};

#[derive(Clone)]
pub struct AppState {
    pub providers: ProviderManager,
    pub provider_client: ProviderClient,
}

impl AppState {
    pub fn new(config: &AppConfig) -> Result<Self> {
        let providers = ProviderManager::new(&config.providers);
        let provider_client = ProviderClient::new(providers.clone())?;

        Ok(Self {
            providers,
            provider_client,
        })
    }
}
