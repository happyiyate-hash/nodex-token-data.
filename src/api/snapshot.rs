use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;

use crate::{models::TokenSnapshot, state::AppState};

#[derive(Debug, Deserialize)]
pub struct SnapshotQuery {
    pub timeframe: Option<String>,
}

const TIMEFRAMES: &[&str] = &["1S", "5S", "10S", "1M", "5M", "15M", "30M", "1H", "24H"];

pub async fn get_snapshot(
    Path(mint): Path<String>,
    Query(query): Query<SnapshotQuery>,
    State(_state): State<AppState>,
) -> Result<Json<TokenSnapshot>, (StatusCode, String)> {
    if mint.trim().is_empty() || mint.len() > 64 {
        return Err((StatusCode::BAD_REQUEST, "Invalid token mint address".to_owned()));
    }

    let timeframe = query.timeframe.unwrap_or_else(|| "1S".to_owned()).to_uppercase();
    if !TIMEFRAMES.contains(&timeframe.as_str()) {
        return Err((StatusCode::BAD_REQUEST, "Unsupported chart timeframe".to_owned()));
    }

    // Honest placeholder until the provider/indexing pipeline is connected.
    Ok(Json(TokenSnapshot::unavailable(mint, timeframe)))
}
