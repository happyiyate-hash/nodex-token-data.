mod api;
mod config;
mod providers;
mod state;

use anyhow::Result;
use axum::{routing::get, Router};
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::{config::AppConfig, state::AppState};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = AppConfig::from_env()?;
    let state = AppState::new(&config);

    let app = Router::new()
        .route("/health", get(api::health::health))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    info!(address = %config.bind_addr, "NodeX Token Data service listening");

    axum::serve(listener, app).await?;
    Ok(())
}
