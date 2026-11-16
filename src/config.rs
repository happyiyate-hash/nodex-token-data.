use anyhow::{anyhow, Context, Result};
use std::{env, net::SocketAddr};

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub bind_addr: SocketAddr,
    pub database_url: String,
    pub providers: Vec<ProviderConfig>,
}
#[derive(Clone, Debug)]
pub struct ProviderConfig {
    pub name: &'static str,
    pub endpoints: Vec<String>,
    pub websocket_endpoints: Vec<String>,
}
impl AppConfig {
    pub fn from_env() -> Result<Self> {
        let bind_addr = env::var("NODEX_BIND_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:8081".into())
            .parse()
            .context("Invalid NODEX_BIND_ADDR")?;
        let mut providers = Vec::new();
        let helius = parse_csv("HELIUS_API_KEYS");
        if !helius.is_empty() {
            let rpc = env::var("HELIUS_RPC_URL_TEMPLATE")
                .unwrap_or_else(|_| "https://mainnet.helius-rpc.com/?api-key={api_key}".into());
            let ws = env::var("HELIUS_WS_URL_TEMPLATE")
                .unwrap_or_else(|_| "wss://mainnet.helius-rpc.com/?api-key={api_key}".into());
            providers.push(build_provider("helius", &helius, &rpc, &ws)?);
        }
        let fluxrpc = parse_csv("FLUXRPC_API_KEYS");
        if !fluxrpc.is_empty() {
            let rpc = env::var("FLUXRPC_RPC_URL_TEMPLATE")
                .unwrap_or_else(|_| "https://eu.fluxrpc.com/?key={api_key}".into());
            let ws = env::var("FLUXRPC_WS_URL_TEMPLATE")
                .unwrap_or_else(|_| "wss://ws.eu.fluxrpc.com?key={api_key}".into());
            providers.push(build_provider("fluxrpc", &fluxrpc, &rpc, &ws)?);
        }
        let database_url = env::var("NODEX_DATABASE_URL")
            .unwrap_or_else(|_| "sqlite://./data/nodex-token-data.db".into());
        Ok(Self {
            bind_addr,
            database_url,
            providers,
        })
    }
}
fn parse_csv(name: &str) -> Vec<String> {
    env::var(name)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}
fn build_provider(
    name: &'static str,
    keys: &[String],
    rpc: &str,
    ws: &str,
) -> Result<ProviderConfig> {
    if !rpc.contains("{api_key}") || !ws.contains("{api_key}") {
        return Err(anyhow!(
            "{name} RPC and WebSocket templates must contain {{api_key}}"
        ));
    }
    Ok(ProviderConfig {
        name,
        endpoints: keys
            .iter()
            .map(|key| rpc.replace("{api_key}", key))
            .collect(),
        websocket_endpoints: keys
            .iter()
            .map(|key| ws.replace("{api_key}", key))
            .collect(),
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn expands_rpc_and_websocket_endpoints() {
        let p = build_provider(
            "helius",
            &["abc".into()],
            "https://rpc/?key={api_key}",
            "wss://ws/?key={api_key}",
        )
        .unwrap();
        assert_eq!(p.endpoints[0], "https://rpc/?key=abc");
        assert_eq!(p.websocket_endpoints[0], "wss://ws/?key=abc");
    }
}
