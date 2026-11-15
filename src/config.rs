use std::{env, net::SocketAddr};

use anyhow::{anyhow, Context, Result};

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub bind_addr: SocketAddr,
    pub providers: Vec<ProviderConfig>,
}

#[derive(Clone, Debug)]
pub struct ProviderConfig {
    pub name: &'static str,
    pub endpoints: Vec<String>,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        let bind_addr = env::var("NODEX_BIND_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:8081".to_owned())
            .parse()
            .context("NODEX_BIND_ADDR must be a valid socket address")?;

        let mut providers = Vec::new();

        let helius_keys = parse_csv_env("HELIUS_API_KEYS");
        if !helius_keys.is_empty() {
            let template = env::var("HELIUS_RPC_URL_TEMPLATE")
                .unwrap_or_else(|_| "https://mainnet.helius-rpc.com/?api-key={api_key}".to_owned());
            providers.push(build_provider("helius", &helius_keys, &template)?);
        }

        let fluxrpc_keys = parse_csv_env("FLUXRPC_API_KEYS");
        if !fluxrpc_keys.is_empty() {
            let template = env::var("FLUXRPC_RPC_URL_TEMPLATE")
                .context("FLUXRPC_RPC_URL_TEMPLATE is required when FLUXRPC_API_KEYS is set")?;
            providers.push(build_provider("fluxrpc", &fluxrpc_keys, &template)?);
        }

        Ok(Self {
            bind_addr,
            providers,
        })
    }
}

fn parse_csv_env(name: &str) -> Vec<String> {
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
    template: &str,
) -> Result<ProviderConfig> {
    if !template.contains("{api_key}") {
        return Err(anyhow!("{name} URL template must contain {{api_key}}"));
    }

    let endpoints = keys
        .iter()
        .map(|key| template.replace("{api_key}", key))
        .collect();

    Ok(ProviderConfig { name, endpoints })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_template_expands_each_key() {
        let keys = vec!["first".to_owned(), "second".to_owned()];
        let provider =
            build_provider("helius", &keys, "https://rpc.example/?key={api_key}").unwrap();

        assert_eq!(provider.endpoints.len(), 2);
        assert_eq!(provider.endpoints[0], "https://rpc.example/?key=first");
        assert_eq!(provider.endpoints[1], "https://rpc.example/?key=second");
    }

    #[test]
    fn provider_template_requires_key_placeholder() {
        let result = build_provider("fluxrpc", &["secret".to_owned()], "https://rpc.example");
        assert!(result.is_err());
    }
}
