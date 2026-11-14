use axum::{extract::State, Json};
use serde::Serialize;

use crate::state::AppState;

#[derive(Serialize)]
pub struct HealthResponse {
    service: &'static str,
    status: &'static str,
    providers_configured: bool,
    providers: Vec<crate::providers::ProviderStatus>,
}

pub async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        service: "nodex-token-data",
        status: "ok",
        providers_configured: state.providers.has_providers(),
        providers: state.providers.statuses(),
    })
}
