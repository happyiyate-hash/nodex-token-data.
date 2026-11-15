use anyhow::Result;
use crate::{models::HoldersSnapshot, providers::ProviderClient};

pub async fn fetch(
    mint: &str,
    total_supply_raw: &str,
    decimals: u8,
    providers: &ProviderClient,
) -> Result<HoldersSnapshot> {
    providers.fetch_largest_holders(mint, total_supply_raw, decimals).await
}
