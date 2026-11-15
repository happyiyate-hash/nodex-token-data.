use crate::state::AppState;
use anyhow::Result;

/// FluxRPC standard Solana WebSocket adapter (fallback source).
pub async fn run_once(state: &AppState, endpoint: &str) -> Result<()> {
    super::stream::run_connection(state, endpoint, "fluxrpc").await
}
