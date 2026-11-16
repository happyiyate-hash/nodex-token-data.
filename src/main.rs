mod api;
mod components;
mod config;
mod market;
mod models;
mod providers;
mod state;
mod storage;
mod stream;
mod ws;

use anyhow::Result;
use axum::{
    http::{header, Method},
    routing::get,
    Router,
};
use tower_http::cors::{Any, CorsLayer};
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::{config::AppConfig, state::AppState};

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

    let holder_stream_state = state.clone();
    tokio::spawn(async move {
        holder_stream_state.run_holder_stream().await;
    });

    let provider_stream_state = state.clone();
    tokio::spawn(async move {
        provider_stream_state.run_provider_streams().await;
    });

    let app = Router::new()
        .route("/health", get(api::health::health))
        .route("/providers/status", get(api::providers::status))
        .route(
            "/api/v1/tokens/{mint}/snapshot",
            get(api::snapshot::get_snapshot),
        )
        .route("/ws/tokens", get(ws::connect))
        .with_state(state)
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
                .allow_headers([header::CONTENT_TYPE]),
        );

    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    info!(address = %config.bind_addr, "NodeX Token Data service listening");
    axum::serve(listener, app).await?;
    Ok(())
}
