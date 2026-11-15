use crate::{components::coordinator, state::AppState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct SnapshotQuery {
    pub timeframe: Option<String>,
}
const TIMEFRAMES: &[&str] = &["1S", "5S", "10S", "1M", "5M", "15M", "30M", "1H", "24H"];
const BASE58: &str = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
fn is_valid_mint(mint: &str) -> bool {
    (32..=44).contains(&mint.len())
        && mint.chars().all(|c| BASE58.contains(c))
        && bs58::decode(mint).into_vec().is_ok_and(|bytes| bytes.len() == 32)
}
pub async fn get_snapshot(
    Path(mint): Path<String>,
    Query(query): Query<SnapshotQuery>,
    State(state): State<AppState>,
) -> Result<Json<crate::models::TokenSnapshot>, (StatusCode, String)> {
    if !is_valid_mint(&mint) {
        return Err((
            StatusCode::BAD_REQUEST,
            "Invalid Solana mint address".to_owned(),
        ));
    }
    let timeframe = query
        .timeframe
        .unwrap_or_else(|| "1S".to_owned())
        .to_uppercase();
    if !TIMEFRAMES.contains(&timeframe.as_str()) {
        return Err((
            StatusCode::BAD_REQUEST,
            "Unsupported chart timeframe".to_owned(),
        ));
    }
    coordinator::load_snapshot(&mint, &timeframe, &state)
        .await
        .map(Json)
}
#[cfg(test)]
mod tests {
    use super::is_valid_mint;
    #[test]
    fn rejects_invalid_characters() {
        assert!(!is_valid_mint("0000000000000000000000000000000!"));
    }
    #[test]
    fn accepts_sol_mint() {
        assert!(is_valid_mint("So11111111111111111111111111111111111111112"));
    }
}
