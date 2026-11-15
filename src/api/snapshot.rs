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
const BASE58: &str = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

pub async fn get_snapshot(
    Path(mint): Path<String>,
    Query(query): Query<SnapshotQuery>,
    State(state): State<AppState>,
) -> Result<Json<TokenSnapshot>, (StatusCode, String)> {
    if !(32..=44).contains(&mint.len()) || !mint.chars().all(|character| BASE58.contains(character)) {
        return Err((StatusCode::BAD_REQUEST, "Invalid Solana mint address".to_owned()));
    }

    let timeframe = query.timeframe.unwrap_or_else(|| "1S".to_owned()).to_uppercase();
    if !TIMEFRAMES.contains(&timeframe.as_str()) {
        return Err((StatusCode::BAD_REQUEST, "Unsupported chart timeframe".to_owned()));
    }

    let chain_data = state
        .provider_client
        .fetch_token_chain_data(&mint)
        .await
        .map_err(|_| {
            (
                StatusCode::BAD_GATEWAY,
                "Token data could not be retrieved from configured Solana RPC providers".to_owned(),
            )
        })?;

    let mut snapshot = TokenSnapshot::unavailable(mint.clone(), timeframe);
    snapshot.identity.name = chain_data.name.clone();
    snapshot.identity.symbol = chain_data.symbol.clone();
    snapshot.identity.logo_url = chain_data.logo_url;
    snapshot.identity.creator_address = chain_data.creator_address.clone();
    snapshot.info.decimals = Some(chain_data.decimals);
    snapshot.info.total_supply = Some(chain_data.total_supply);
    snapshot.info.token_standard = Some(match chain_data.token_program.as_str() {
        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" => "SPL Token".to_owned(),
        "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb" => "Token-2022".to_owned(),
        _ => "Unknown token program".to_owned(),
    });
    snapshot.info.creator_address = chain_data.creator_address;
    snapshot.data_status.identity = if snapshot.identity.name.is_some() || snapshot.identity.symbol.is_some() {
        "partial"
    } else {
        "unavailable"
    };
    snapshot.data_status.info = "available";

    Ok(Json(snapshot))
}
