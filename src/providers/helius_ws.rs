use crate::state::AppState;
use anyhow::Result;

/// Helius standard Solana WebSocket adapter (primary source).
pub async fn run_once(state: &AppState, endpoint: &str) -> Result<()> {
    super::stream::run_connection(state, endpoint, "helius").await
}
