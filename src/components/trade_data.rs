use std::collections::HashMap;
use anyhow::Result;
use crate::{market::MarketDataClient, models::Trade, storage::MarketStore};

pub async fn fetch(
    mint: &str,
    pair_address: Option<&str>,
    market: &MarketDataClient,
    store: &MarketStore,
) -> Result<Vec<Trade>> {
    let mut trades = HashMap::<String, Trade>::new();
    for trade in store.load_recent_trades(mint, 100).await? {
        trades.insert(trade.signature.clone(), trade);
    }
    if let Some(pool) = pair_address {
        if let Ok(provider_trades) = market.fetch_trades(pool).await {
            for trade in provider_trades {
                trades.entry(trade.signature.clone()).or_insert(trade);
            }
        }
    }
    let mut result = trades.into_values().collect::<Vec<_>>();
    result.sort_by(|a, b| b.timestamp_ms.cmp(&a.timestamp_ms));
    result.truncate(100);
    Ok(result)
}
