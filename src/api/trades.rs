use crate::state::AppState;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct MissedTradesQuery {
    pub after: Option<i64>,
    pub limit: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct MissedTrade {
    pub cursor: i64,
    #[serde(flatten)]
    pub trade: crate::models::Trade,
}

#[derive(Debug, Serialize)]
pub struct MissedTradesResponse {
    pub mint: String,
    pub after_cursor: i64,
    pub head_cursor: i64,
    pub next_cursor: i64,
    pub has_more: bool,
    pub cursor_reset: bool,
    pub trades: Vec<MissedTrade>,
}

const BASE58: &str = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

fn is_valid_mint(mint: &str) -> bool {
    (32..=44).contains(&mint.len())
        && mint.chars().all(|c| BASE58.contains(c))
        && bs58::decode(mint)
            .into_vec()
            .is_ok_and(|bytes| bytes.len() == 32)
}

pub async fn get_missed_trades(
    Path(mint): Path<String>,
    Query(query): Query<MissedTradesQuery>,
    State(state): State<AppState>,
) -> Result<Json<MissedTradesResponse>, (StatusCode, String)> {
    if !is_valid_mint(&mint) {
        return Err((StatusCode::BAD_REQUEST, "Invalid Solana mint address".to_owned()));
    }

    let requested_after = query.after.unwrap_or(0).max(0);
    let limit = query.limit.unwrap_or(500).clamp(1, 500);
    let head_cursor = state.market_store.trade_cursor_head(&mint).await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    // A per-mint cache can expire and be recreated. A cursor beyond its new
    // head means the client must restart from the retained trade history.
    let cursor_reset = requested_after > head_cursor;
    let effective_after = if cursor_reset { 0 } else { requested_after };
    let rows = state.market_store.load_trades_after_cursor(&mint, effective_after, limit).await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let next_cursor = rows.last().map(|(cursor, _)| *cursor).unwrap_or(effective_after);
    let has_more = next_cursor < head_cursor;

    Ok(Json(MissedTradesResponse {
        mint,
        after_cursor: requested_after,
        head_cursor,
        next_cursor,
        has_more,
        cursor_reset,
        trades: rows.into_iter().map(|(cursor, trade)| MissedTrade { cursor, trade }).collect(),
    }))
}

#[cfg(test)]
mod tests {
    use super::is_valid_mint;

    #[test]
    fn rejects_invalid_mints() {
        assert!(!is_valid_mint("not-a-solana-mint"));
    }

    #[test]
    fn accepts_sol_mint() {
        assert!(is_valid_mint("So11111111111111111111111111111111111111112"));
    }
}
