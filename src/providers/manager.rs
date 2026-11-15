use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use crate::config::ProviderConfig;

#[derive(Clone)]
pub struct ProviderManager {
    providers: Arc<Vec<ProviderPool>>,
}

#[allow(dead_code)]
struct ProviderPool {
    name: &'static str,
    endpoints: Vec<String>,
    cursor: AtomicUsize,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct SelectedEndpoint {
    pub provider: &'static str,
    pub url: String,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct ProviderStatus {
    pub name: &'static str,
    pub configured_keys: usize,
}

impl ProviderManager {
    pub fn new(configs: &[ProviderConfig]) -> Self {
        let providers = configs
            .iter()
            .filter(|config| !config.endpoints.is_empty())
            .map(|config| ProviderPool {
                name: config.name,
                endpoints: config.endpoints.clone(),
                cursor: AtomicUsize::new(0),
            })
            .collect();

        Self {
            providers: Arc::new(providers),
        }
    }

    /// Selects the next configured key for a provider using atomic round-robin
    /// rotation. Callers should avoid logging the returned URL because it
    /// contains the provider credential.
    #[allow(dead_code)]
    pub fn next_endpoint(&self, provider_name: &str) -> Option<SelectedEndpoint> {
        let pool = self
            .providers
            .iter()
            .find(|pool| pool.name == provider_name)?;
        let index = pool.cursor.fetch_add(1, Ordering::Relaxed) % pool.endpoints.len();

        Some(SelectedEndpoint {
            provider: pool.name,
            url: pool.endpoints[index].clone(),
        })
    }

    pub fn statuses(&self) -> Vec<ProviderStatus> {
        self.providers
            .iter()
            .map(|pool| ProviderStatus {
                name: pool.name,
                configured_keys: pool.endpoints.len(),
            })
            .collect()
    }

    pub fn has_providers(&self) -> bool {
        !self.providers.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rotates_keys_in_round_robin_order() {
        let manager = ProviderManager::new(&[ProviderConfig {
            name: "helius",
            endpoints: vec!["key-a".into(), "key-b".into()],
        }]);

        assert_eq!(manager.next_endpoint("helius").unwrap().url, "key-a");
        assert_eq!(manager.next_endpoint("helius").unwrap().url, "key-b");
        assert_eq!(manager.next_endpoint("helius").unwrap().url, "key-a");
    }

    #[test]
    fn unknown_provider_returns_none() {
        let manager = ProviderManager::new(&[]);
        assert!(manager.next_endpoint("helius").is_none());
    }
}
