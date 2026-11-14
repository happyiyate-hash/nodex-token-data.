# NodeX Token Data

Independent Rust market-data service for the NodeX Token Details experience.

This service is intentionally separate from the NodeX trading engine. It is
responsible for token market data and detail-page subscriptions; it does not
execute trades or create, modify, or close positions.

## Current foundation

- Axum HTTP service
- Environment-based configuration
- Helius and FluxRPC provider endpoint configuration
- Atomic round-robin API-key selection per provider
- Health endpoint that reports provider configuration without exposing keys
- GitHub Actions CI for formatting, Clippy, tests, and release builds
- Docker image for deployment

## Run locally

1. Copy `.env.example` to `.env`.
2. Add provider keys and, when using FluxRPC, its RPC URL template.
3. Start the service:

```bash
cargo run
```

Health check: `GET http://localhost:8081/health`.

The health endpoint reports whether providers are configured. It does not yet
make live provider requests; provider connectivity checks and token-data
features will be added in subsequent steps.

## Provider configuration

- `HELIUS_API_KEYS`: comma-separated Helius API keys.
- `HELIUS_RPC_URL_TEMPLATE`: URL template containing `{api_key}`.
- `FLUXRPC_API_KEYS`: comma-separated FluxRPC API keys.
- `FLUXRPC_RPC_URL_TEMPLATE`: FluxRPC URL template containing `{api_key}`.

Keep credentials in deployment secrets or an untracked local `.env` file.
Never commit real API keys.

## CI

GitHub Actions runs on pushes and pull requests targeting `main`. It checks
formatting, runs Clippy with warnings denied, executes tests, and builds the
release binary.
