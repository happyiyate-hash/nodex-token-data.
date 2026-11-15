# NodeX Token Data

Independent Rust service for NodeX token metadata, market data, charts, trades and holder information. It is separate from the NodeX trading engine and never creates, modifies, or closes positions.

## API design

The frontend loads a token once using the REST snapshot, then subscribes to changing values over one authenticated WebSocket connection.

### REST: request once when opening a token

- `GET /api/v1/tokens/{mint}/snapshot?timeframe=1S` — initial Token Details data for identity, market, chart history, trades, holders and token information.
- `POST /api/v1/ws-ticket` — issue a 30-second, single-use WebSocket ticket.
- `GET /providers/status` — live provider connectivity diagnostics.

All routes except `/health` require `x-api-key`. The WebSocket handshake uses the short-lived ticket as a query parameter because browser WebSocket clients cannot attach arbitrary authorization headers.

### WebSocket: subscribe once, receive updates

Connect to `/ws/tokens?ticket={ticket}`, then send:

```json
{
  "command": "subscribe",
  "mint": "TOKEN_MINT_ADDRESS",
  "topics": [
    "token.market",
    "token.chart",
    "token.trades",
    "token.activity",
    "token.holders",
    "token.info",
    "token.creator"
  ]
}
```

Supported topics:

- `token.market`: price, market cap, percentage changes, liquidity, volume and buy/sell totals.
- `token.chart`: live candle/point updates for the selected timeframe.
- `token.trades`: confirmed buy/sell transactions.
- `token.activity`: floating buy/sell activity beside the chart.
- `token.holders`: holder count, largest holders and ownership concentration when refreshed by the indexer.
- `token.info`: mutable on-chain token information.
- `token.creator`: creator-related updates when a reliable source is available.

Commands also include `unsubscribe` and `set_chart_timeframe`. The server acknowledges subscriptions and rejects unsupported topics/timeframes. A single multiplexed connection is used per Token Details session, not one physical socket per metric.

### Position data is not served here

The existing NodeX trading engine remains authoritative for paper positions and portfolio state. The frontend must query/subscribe to that service separately. If it reports no position, show the existing empty state. This service must never fabricate position values.

## Snapshot semantics

The snapshot is a point-in-time response, not a permanent server-side object. The frontend can keep it in memory while the token page remains open and refresh it on page reopen, reconnect, timeframe change requiring history, or an explicit refresh. Static identity fields can be cached longer; market fields must be updated from the live stream.

Missing data is represented as `null` or an empty list, with a `data_status` value. Zero is reserved for a verified zero. Never substitute demo values.

## Current implementation status

Implemented:
- Axum HTTP service and health endpoint.
- Helius/FluxRPC endpoint configuration and per-provider key rotation.
- Read-only provider `getSlot` checks.
- API-key middleware for protected REST endpoints.
- Token snapshot response schema and route, including live `getTokenSupply` and `getAccountInfo` retrieval with Helius-first / FluxRPC fallback.
- Optional Helius DAS `getAsset` metadata enrichment (name, symbol, image and creator authority when returned).
- Short-lived, single-use WebSocket tickets.
- Authenticated WebSocket handshake, topic subscription acknowledgements, unsubscribe and chart-timeframe command.
- In-process broadcast event bus for provider/indexer workers to publish typed topic events.
- CI checks for format, Clippy, tests and release build.

Not yet implemented:
- Price/market-cap calculation, trade decoding, OHLCV history and holder indexing.
- Metadata enrichment when Helius DAS is not configured or does not return asset metadata.
- A persistent event/indexing pipeline and event producers. The WebSocket currently provides the subscription transport; no live market events are emitted until those workers are connected.
- Durable historical storage, retention and candle aggregation.
- Production deployment and TLS verification.

**Important:** The snapshot now returns verified on-chain mint decimals, raw total supply and token-program identity when RPC providers are available. Helius DAS metadata is optional. Market values, charts, trades and holders still return unavailable/null values until their data sources, indexers and persistence are implemented. Do not present this as a complete or production-ready market-data service.

## Configuration

Copy `.env.example` to `.env`. Set:
- `NODEX_API_KEY`: required random secret of at least 32 characters.
- `HELIUS_API_KEYS`: comma-separated Helius keys.
- `FLUXRPC_API_KEYS` and `FLUXRPC_RPC_URL_TEMPLATE`: optional fallback provider configuration.

Keep all secrets in deployment secret storage. Never commit real keys. Do not embed the backend's shared administrative API key in a publicly distributed APK; before public release, replace it with per-user/device-issued credentials or a token exchange.

## Run

```bash
cargo run
```

Health: `GET http://localhost:8081/health`.
