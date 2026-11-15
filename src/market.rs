use std::collections::HashMap;

use anyhow::{anyhow, Result};
use reqwest::Client;
use serde::Serialize;
use serde_json::Value;
use tracing::warn;

#[derive(Clone)]
pub struct MarketDataClient {
    http: Client,
}

#[derive(Clone, Debug, Serialize)]
pub struct MarketQuote {
    pub mint: String,
    pub pair_address: Option<String>,
    pub dex_id: Option<String>,
    pub price_usd: Option<f64>,
    pub market_cap_usd: Option<f64>,
    pub liquidity_usd: Option<f64>,
    pub change_5m_pct: Option<f64>,
    pub change_24h_pct: Option<f64>,
    pub volume_24h_usd: Option<f64>,
    pub transactions_24h: Option<u64>,
    pub pair_url: Option<String>,
}

impl MarketDataClient {
    pub fn new() -> Result<Self> {
        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(8))
            .user_agent("NodeX-Token-Data/0.1")
            .build()?;
        Ok(Self { http })
    }

    /// Uses the public DexScreener token-pairs endpoint. Requests can contain
    /// up to 30 Solana mint addresses, allowing the streaming worker to batch
    /// active tokens instead of making one request per token.
    pub async fn fetch_quotes(&self, mints: &[String]) -> Result<HashMap<String, MarketQuote>> {
        if mints.is_empty() {
            return Ok(HashMap::new());
        }
        if mints.len() > 30 {
            return Err(anyhow!("DexScreener batches are limited to 30 token addresses"));
        }

        let addresses = mints.join(",");
        let url = format!("https://api.dexscreener.com/tokens/v1/solana/{addresses}");
        let response = self.http.get(url).send().await
            .map_err(|_| anyhow!("Market-data request failed or timed out"))?;
        if !response.status().is_success() {
            return Err(anyhow!("Market-data provider returned HTTP {}", response.status().as_u16()));
        }

        let pairs: Vec<Value> = response.json().await
            .map_err(|_| anyhow!("Market-data provider returned invalid JSON"))?;
        let mut quotes = HashMap::new();

        for pair in pairs {
            let Some(mint) = pair.pointer("/baseToken/address").and_then(Value::as_str) else {
                continue;
            };
            if !mints.iter().any(|candidate| candidate == mint) {
                continue;
            }

            let quote = MarketQuote {
                mint: mint.to_owned(),
                pair_address: pair.get("pairAddress").and_then(Value::as_str).map(ToOwned::to_owned),
                dex_id: pair.get("dexId").and_then(Value::as_str).map(ToOwned::to_owned),
                price_usd: parse_number(pair.get("priceUsd")),
                market_cap_usd: parse_number(pair.get("marketCap")),
                liquidity_usd: parse_number(pair.pointer("/liquidity/usd")),
                change_5m_pct: parse_number(pair.pointer("/priceChange/m5")),
                change_24h_pct: parse_number(pair.pointer("/priceChange/h24")),
                volume_24h_usd: parse_number(pair.pointer("/volume/h24")),
                transactions_24h: transaction_count(&pair),
                pair_url: pair.get("url").and_then(Value::as_str).map(ToOwned::to_owned),
            };

            let replace = quotes.get(mint)
                .and_then(|current: &MarketQuote| current.liquidity_usd)
                .unwrap_or(0.0)
                < quote.liquidity_usd.unwrap_or(0.0);
            if replace || !quotes.contains_key(mint) {
                quotes.insert(mint.to_owned(), quote);
            }
        }

        Ok(quotes)
    }

    pub async fn fetch_quote(&self, mint: &str) -> Result<Option<MarketQuote>> {
        let mints = vec![mint.to_owned()];
        match self.fetch_quotes(&mints).await {
            Ok(quotes) => Ok(quotes.get(mint).cloned()),
            Err(error) => {
                warn!(error = %error, "Market-data snapshot lookup failed");
                Err(error)
            }
        }
    }
}

fn parse_number(value: Option<&Value>) -> Option<f64> {
    value.and_then(|value| {
        value.as_f64().or_else(|| value.as_str().and_then(|text| text.parse().ok()))
    })
}

fn transaction_count(pair: &Value) -> Option<u64> {
    let buys = pair.pointer("/txns/h24/buys").and_then(Value::as_u64)?;
    let sells = pair.pointer("/txns/h24/sells").and_then(Value::as_u64)?;
    Some(buys.saturating_add(sells))
}
