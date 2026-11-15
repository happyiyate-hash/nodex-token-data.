use anyhow::Result;
use crate::market::{MarketDataClient, MarketQuote};

pub async fn fetch(mint: &str, client: &MarketDataClient) -> Result<Option<MarketQuote>> {
    client.fetch_quote(mint).await
}
