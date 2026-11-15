use crate::state::AppState;
use axum::{extract::State, Json};
use serde::Serialize;

#[derive(Serialize)]
pub struct TicketResponse {
    pub ticket: String,
    pub expires_in_seconds: u64,
    pub websocket_path: &'static str,
}

pub async fn issue(State(state): State<AppState>) -> Json<TicketResponse> {
    Json(TicketResponse {
        ticket: state.issue_ws_ticket(),
        expires_in_seconds: 30,
        websocket_path: "/ws/tokens",
    })
}
