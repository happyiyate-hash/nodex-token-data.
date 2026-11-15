use axum::{extract::State, Json};

use crate::{providers::ProviderCheck, state::AppState};

/// Performs a live, read-only getSlot check against each configured provider.
/// Helius is checked first when it is first in the provider configuration.
pub async fn status(State(state): State<AppState>) -> Json<Vec<ProviderCheck>> {
    Json(state.provider_client.check_all().await)
}
