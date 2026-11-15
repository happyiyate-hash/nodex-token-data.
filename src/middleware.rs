use axum::{body::Body, extract::State, http::{Request, StatusCode}, middleware::Next, response::{IntoResponse, Response}};
use crate::state::AppState;
pub async fn require_api_key(State(state): State<AppState>, request: Request<Body>, next: Next) -> Response {
    let supplied = request.headers().get("x-api-key").and_then(|v| v.to_str().ok());
    if supplied.is_some_and(|value| value == state.api_key.as_str()) {
        next.run(request).await
    } else {
        (StatusCode::UNAUTHORIZED, "Missing or invalid API key").into_response()
    }
}
