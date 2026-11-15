use anyhow::Result;
use crate::{market::MarketDataClient, models::ChartPoint, storage::MarketStore};

pub async fn fetch(
    mint: &str,
    timeframe: &str,
    pair_address: Option<&str>,
    market: &MarketDataClient,
    store: &MarketStore,
) -> Result<Vec<ChartPoint>> {
    if let Some(pool) = pair_address {
        if let Ok(history) = market.fetch_chart(pool, timeframe).await {
            if !history.is_empty() {
                store.seed_historical_candles(mint, timeframe, &history).await?;
            }
        }
    }
    store.load_candles(mint, timeframe, 5000).await
}
