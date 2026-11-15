use std::time::Duration;

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

#[derive(Clone, Debug)]
pub struct TokenChainData {
    pub decimals: u8,
    pub total_supply: String,
    pub token_program: String,
    pub name: Option<String>,
    pub symbol: Option<String>,
    pub logo_url: Option<String>,
    pub creator_address: Option<String>,
}

impl ProviderClient {
    pub fn new(manager: ProviderManager) -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(8))
            .user_agent("NodeX-Token-Data/0.1")
            .build()?;

        Ok(Self { http, manager })
    }

    /// Checks every configured provider independently using read-only getSlot.
    pub async fn check_all(&self) -> Vec<ProviderCheck> {
        let mut checks = Vec::new();

        for name in self.manager.provider_names() {
            checks.push(self.check_provider(name).await);
        }

        checks
    }

    /// Retrieves on-chain mint supply and token-program ownership. Each RPC
    /// operation tries Helius first, then FluxRPC, rotating through configured
    /// keys on failure. Optional Helius DAS metadata never blocks chain data.
    pub async fn fetch_token_chain_data(&self, mint: &str) -> Result<TokenChainData> {
        let supply = self
            .request_rpc(
                "getTokenSupply",
                json!([mint, { "commitment": "confirmed" }]),
            )
            .await?;

        let supply_value = supply
            .get("value")
            .ok_or_else(|| anyhow!("RPC token-supply response has no value"))?;
        let decimals = supply_value
            .get("decimals")
            .and_then(Value::as_u64)
            .and_then(|value| u8::try_from(value).ok())
            .ok_or_else(|| anyhow!("RPC token-supply response has invalid decimals"))?;
        let total_supply = supply_value
            .get("amount")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("RPC token-supply response has no raw amount"))?
            .to_owned();

        let account = self
            .request_rpc(
                "getAccountInfo",
                json!([mint, { "encoding": "base64", "commitment": "confirmed" }]),
            )
            .await?;
        let token_program = account
            .get("value")
            .and_then(|value| value.get("owner"))
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("RPC mint account response has no owner program"))?
            .to_owned();

        let metadata = self.fetch_helius_asset(mint).await.unwrap_or(Value::Null);
        let name = metadata
            .pointer("/content/metadata/name")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let symbol = metadata
            .pointer("/content/metadata/symbol")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let logo_url = metadata
            .pointer("/content/links/image")
            .or_else(|| metadata.pointer("/content/files/0/uri"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        // Do not label an update authority as the token creator. Only accept
        // an explicit creator field when the metadata source provides one.
        let creator_address = metadata
            .pointer("/creators/0/address")
            .or_else(|| metadata.pointer("/content/metadata/creators/0/address"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);

        Ok(TokenChainData {
            decimals,
            total_supply,
            token_program,
            name,
            symbol,
            logo_url,
            creator_address,
        })
    }

    async fn fetch_helius_asset(&self, mint: &str) -> Result<Value> {
        if self.manager.configured_key_count("helius") == 0 {
            return Err(anyhow!("Helius DAS is not configured"));
        }

        self.request_rpc_for_provider("helius", "getAsset", json!([{ "id": mint }]))
            .await
    }

    async fn request_rpc(&self, method: &str, params: Value) -> Result<Value> {
        let mut last_error = None;

        for provider in self.manager.provider_names() {
            match self
                .request_rpc_for_provider(provider, method, params.clone())
                .await
            {
                Ok(result) => return Ok(result),
                Err(error) => {
                    warn!(provider, method, error = %error, "Solana RPC provider request failed");
                    last_error = Some(error);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("No Solana RPC providers are configured")))
    }

    async fn request_rpc_for_provider(
        &self,
        provider: &'static str,
        method: &str,
        params: Value,
    ) -> Result<Value> {
        let configured_keys = self.manager.configured_key_count(provider);
        let mut last_error = None;

        for _ in 0..configured_keys {
            let Some(endpoint) = self.manager.next_endpoint(provider) else {
                break;
            };

            match self.send_rpc(&endpoint.url, method, params.clone()).await {
                Ok(result) => return Ok(result),
                Err(error) => last_error = Some(error),
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("No keys configured for provider {provider}")))
    }

    async fn send_rpc(&self, endpoint: &str, method: &str, params: Value) -> Result<Value> {
        let response = self
            .http
            .post(endpoint)
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params
            }))
            .send()
            .await
            .map_err(|_| anyhow!("RPC request failed or timed out"))?;

        if !response.status().is_success() {
            return Err(anyhow!("RPC returned HTTP {}", response.status().as_u16()));
        }

        let payload: Value = response
            .json()
            .await
            .map_err(|_| anyhow!("RPC returned invalid JSON"))?;

        if let Some(error) = payload.get("error") {
            let code = error.get("code").and_then(Value::as_i64);
            return Err(match code {
                Some(code) => anyhow!("RPC returned error code {code}"),
                None => anyhow!("RPC returned an error response"),
            });
        }

        payload
            .get("result")
            .cloned()
            .ok_or_else(|| anyhow!("RPC response did not contain a result"))
    }

    async fn check_provider(&self, name: &'static str) -> ProviderCheck {
        let configured_keys = self.manager.configured_key_count(name);
        let started = std::time::Instant::now();

        match self
            .request_rpc_for_provider(name, "getSlot", json!([]))
            .await
        {
            Ok(result) => ProviderCheck {
                name,
                configured_keys,
                reachable: true,
                slot: result.as_u64(),
                latency_ms: Some(started.elapsed().as_millis()),
                error: None,
            },
            Err(error) => ProviderCheck {
                name,
                configured_keys,
                reachable: false,
                slot: None,
                latency_ms: None,
                error: Some(error.to_string()),
            },
        }
    }
}
