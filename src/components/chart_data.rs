use crate::{market::MarketDataClient, models::ChartPoint, storage::MarketStore};
use anyhow::Result;

pub async fn fetch(
    mint: &str,
    timeframe: &str,
    pair_address: Option<&str>,
    market: &MarketDataClient,
    store: &MarketStore,
) -> Result<Vec<ChartPoint>> {
    if let Some(pool) = pair_address {
        match market.fetch_chart(pool, timeframe).await {
            Ok(history) if !history.is_empty() => {
                store
                    .seed_historical_candles(mint, timeframe, &history)
                    .await?;
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(mint, timeframe, error = %error, "Historical chart provider unavailable; using locally recorded candles");
            }
        }
    }
    store.load_candles(mint, timeframe, 5000).await
}
