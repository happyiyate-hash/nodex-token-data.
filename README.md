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
- Live, read-only Solana RPC connectivity checks using `getSlot`
- Provider status endpoint that reports reachability, observed slot and latency
- Provider priority follows configuration order: Helius first, FluxRPC second
- Provider errors do not expose endpoint URLs or API keys
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

Live provider check: `GET http://localhost:8081/providers/status`.

The provider status endpoint sends a read-only `getSlot` request to every
configured provider. It tries each configured key for that provider once,
using round-robin key selection. It reports configured keys separately from
actual connectivity. Provider order is preserved; configure Helius before
FluxRPC to make Helius the primary provider and FluxRPC the fallback priority.

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

## Not yet implemented

- Public token/market-data REST endpoints
- Backend API-key authentication and rate limiting
- WebSocket routes, authenticated subscriptions and live streaming
- Provider-specific token/account data workflows
- Production TLS/reverse-proxy configuration and deployment verification
