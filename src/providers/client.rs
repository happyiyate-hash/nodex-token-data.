use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use reqwest::Client;
use serde::Serialize;
use serde_json::{json, Value};
use tracing::warn;

use super::manager::ProviderManager;

#[derive(Clone)]
pub struct ProviderClient {
    http: Client,
    manager: ProviderManager,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProviderCheck {
    pub name: &'static str,
    pub configured_keys: usize,
    pub reachable: bool,
    pub slot: Option<u64>,
    pub latency_ms: Option<u128>,
    pub error: Option<String>,
}

#[derive(serde::Deserialize)]
struct RpcResponse {
    result: Option<u64>,
    error: Option<Value>,
}

impl ProviderClient {
    pub fn new(manager: ProviderManager) -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(8))
            .user_agent("NodeX-Token-Data/0.1")
            .build()?;

        Ok(Self { http, manager })
    }

    /// Checks every configured provider independently. Provider configuration
    /// order is retained; Helius should be configured before FluxRPC.
    pub async fn check_all(&self) -> Vec<ProviderCheck> {
        let mut checks = Vec::new();

        for name in self.manager.provider_names() {
            checks.push(self.check_provider(name).await);
        }

        checks
    }

    async fn check_provider(&self, name: &'static str) -> ProviderCheck {
        let configured_keys = self.manager.configured_key_count(name);
        let mut last_error = None;

        // Try every configured key once. A failed key does not prevent another
        // key or the next provider from being used.
        for _ in 0..configured_keys {
            let Some(endpoint) = self.manager.next_endpoint(name) else {
                break;
            };

            let started = Instant::now();
            match self.request_slot(&endpoint.url).await {
                Ok(slot) => {
                    return ProviderCheck {
                        name,
                        configured_keys,
                        reachable: true,
                        slot: Some(slot),
                        latency_ms: Some(started.elapsed().as_millis()),
                        error: None,
                    };
                }
                Err(error) => {
                    warn!(provider = name, error = %error, "Solana RPC provider key failed");
                    last_error = Some(error.to_string());
                }
            }
        }

        ProviderCheck {
            name,
            configured_keys,
            reachable: false,
            slot: None,
            latency_ms: None,
            error: last_error.or_else(|| Some("No provider keys are configured".to_owned())),
        }
    }

    /// Uses Solana's read-only getSlot RPC method as a lightweight connectivity
    /// check. The endpoint URL is never included in errors or logs.
    async fn request_slot(&self, endpoint: &str) -> Result<u64> {
        let response = self
            .http
            .post(endpoint)
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getSlot",
                "params": []
            }))
            .send()
            .await
            .map_err(|_| anyhow!("RPC request failed or timed out"))?;

        if !response.status().is_success() {
            return Err(anyhow!("RPC returned HTTP {}", response.status().as_u16()));
        }

        let payload = response
            .json::<RpcResponse>()
            .await
            .map_err(|_| anyhow!("RPC returned an invalid JSON response"))?;

        if let Some(error) = payload.error {
            let code = error.get("code").and_then(Value::as_i64);
            return Err(match code {
                Some(code) => anyhow!("RPC returned error code {code}"),
                None => anyhow!("RPC returned an error response"),
            });
        }

        payload
            .result
            .ok_or_else(|| anyhow!("RPC response did not contain a slot result"))
    }
}
