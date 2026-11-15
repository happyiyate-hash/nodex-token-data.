use crate::market::{MarketDataClient, MarketQuote};
use anyhow::Result;

pub async fn fetch(mint: &str, client: &MarketDataClient) -> Result<Option<MarketQuote>> {
    client.fetch_quote(mint).await
}
