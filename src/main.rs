mod api;
mod config;
mod market;
mod middleware;
mod models;
mod providers;
mod state;
mod storage;
mod stream;
mod ws;

use anyhow::Result;
use axum::{
    middleware::from_fn_with_state,
    routing::{get, post},
    Router,
};
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::{config::AppConfig, middleware::require_api_key, state::AppState};

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = AppConfig::from_env()?;
    let state = AppState::new(&config).await?;

    let market_stream_state = state.clone();
    tokio::spawn(async move {
        market_stream_state.run_market_stream().await;
    });

    let provider_stream_state = state.clone();
    tokio::spawn(async move {
        provider_stream_state.run_provider_streams().await;
    });

    let protected = Router::new()
        .route("/providers/status", get(api::providers::status))
        .route(
            "/api/v1/tokens/{mint}/snapshot",
            get(api::snapshot::get_snapshot),
        )
        .route("/api/v1/ws-ticket", post(api::ws_ticket::issue))
        .route_layer(from_fn_with_state(state.clone(), require_api_key));

    let app = Router::new()
        .route("/health", get(api::health::health))
        // The WebSocket uses a short-lived, single-use ticket.
        .route("/ws/tokens", get(ws::connect))
        .merge(protected)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    info!(address = %config.bind_addr, "NodeX Token Data service listening");
    axum::serve(listener, app).await?;
    Ok(())
}
