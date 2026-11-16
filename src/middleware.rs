use axum::{
    body::Body,
    extract::State,
    http::{Method, Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};

use crate::state::AppState;

pub async fn require_api_key(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    // Browser clients send an unauthenticated CORS preflight before the
    // authenticated request. Let the outer CorsLayer answer it; API routes
    // themselves remain protected by the API key check below.
    if request.method() == Method::OPTIONS {
        return StatusCode::NO_CONTENT.into_response();
    }

    let supplied = request
        .headers()
        .get("x-api-key")
        .and_then(|value| value.to_str().ok());

    if supplied.is_some_and(|value| value == state.api_key.as_str()) {
        next.run(request).await
    } else {
        (StatusCode::UNAUTHORIZED, "Missing or invalid API key").into_response()
    }
}
