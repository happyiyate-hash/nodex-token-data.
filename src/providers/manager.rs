use crate::config::ProviderConfig;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Clone)]
pub struct ProviderManager {
    providers: Arc<Vec<ProviderPool>>,
}
struct ProviderPool {
    name: &'static str,
    endpoints: Vec<String>,
    websocket_endpoints: Vec<String>,
    rpc_cursor: AtomicUsize,
    websocket_cursor: AtomicUsize,
}
#[derive(Clone, Debug)]
pub struct SelectedEndpoint {
    pub url: String,
}
#[derive(Clone, Debug, serde::Serialize)]
pub struct ProviderStatus {
    pub name: &'static str,
    pub configured_keys: usize,
    pub websocket_keys: usize,
}

impl ProviderManager {
    pub fn new(configs: &[ProviderConfig]) -> Self {
        let providers = configs
            .iter()
            .filter(|c| !c.endpoints.is_empty())
            .map(|c| ProviderPool {
                name: c.name,
                endpoints: c.endpoints.clone(),
                websocket_endpoints: c.websocket_endpoints.clone(),
                rpc_cursor: AtomicUsize::new(0),
                websocket_cursor: AtomicUsize::new(0),
            })
            .collect();
        Self {
            providers: Arc::new(providers),
        }
    }

    pub fn next_endpoint(&self, name: &str) -> Option<SelectedEndpoint> {
        let p = self.providers.iter().find(|p| p.name == name)?;
        let i = p.rpc_cursor.fetch_add(1, Ordering::Relaxed) % p.endpoints.len();
        Some(SelectedEndpoint {
            url: p.endpoints[i].clone(),
        })
    }

    /// Returns configured WebSocket endpoints starting at the next key in the
    /// provider's independent round-robin sequence.
    pub fn websocket_endpoints(&self, name: &str) -> Vec<String> {
        let Some(provider) = self.providers.iter().find(|p| p.name == name) else {
            return Vec::new();
        };
        if provider.websocket_endpoints.is_empty() {
            return Vec::new();
        }
        let start = provider
            .websocket_cursor
            .fetch_add(1, Ordering::Relaxed)
            % provider.websocket_endpoints.len();
        (0..provider.websocket_endpoints.len())
            .map(|offset| {
                provider.websocket_endpoints
                    [(start + offset) % provider.websocket_endpoints.len()]
                    .clone()
            })
            .collect()
    }

    pub fn provider_names(&self) -> Vec<&'static str> {
        self.providers.iter().map(|p| p.name).collect()
    }

    pub fn configured_key_count(&self, name: &str) -> usize {
        self.providers
            .iter()
            .find(|p| p.name == name)
            .map_or(0, |p| p.endpoints.len())
    }

    pub fn statuses(&self) -> Vec<ProviderStatus> {
        self.providers
            .iter()
            .map(|p| ProviderStatus {
                name: p.name,
                configured_keys: p.endpoints.len(),
                websocket_keys: p.websocket_endpoints.len(),
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
    fn preserves_provider_priority() {
        let manager = ProviderManager::new(&[
            ProviderConfig {
                name: "helius",
                endpoints: vec!["h".into()],
                websocket_endpoints: vec!["hw".into()],
            },
            ProviderConfig {
                name: "fluxrpc",
                endpoints: vec!["f".into()],
                websocket_endpoints: vec!["fw".into()],
            },
        ]);
        assert_eq!(manager.provider_names(), vec!["helius", "fluxrpc"]);
        assert_eq!(manager.websocket_endpoints("fluxrpc"), vec!["fw"]);
    }

    #[test]
    fn websocket_keys_rotate_independently_and_cover_all_keys() {
        let manager = ProviderManager::new(&[ProviderConfig {
            name: "helius",
            endpoints: vec!["h1".into(), "h2".into(), "h3".into()],
            websocket_endpoints: vec!["w1".into(), "w2".into(), "w3".into()],
        }]);
        assert_eq!(
            manager.websocket_endpoints("helius"),
            vec!["w1", "w2", "w3"]
        );
        assert_eq!(
            manager.websocket_endpoints("helius"),
            vec!["w2", "w3", "w1"]
        );
    }
}
