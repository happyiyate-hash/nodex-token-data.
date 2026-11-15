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
- Market snapshot fields from the most liquid matching Solana pair returned by DexScreener.
- Batched market quote polling for active WebSocket subscriptions, publishing `token.market` events through the existing broadcast bus.
- Optional Helius DAS `getAsset` metadata enrichment (name, symbol, image and explicitly identified creator fields when returned).
- Short-lived, single-use WebSocket tickets.
- Authenticated WebSocket handshake, topic subscription acknowledgements, unsubscribe and chart-timeframe command.
- In-process broadcast event bus for provider/indexer workers to publish typed topic events.
- CI checks for format, Clippy, tests and release build.

Not yet implemented:
- Trade decoding, historical OHLCV history and holder indexing.
- Helius DAS metadata when the DAS method is unavailable or the asset has no returned metadata.
- Persistent historical indexing, retention, candle aggregation and backfill after service downtime. The current market publisher is active but uses five-second REST polling.
- Production deployment and TLS verification.

**Important:** The snapshot now returns verified on-chain mint decimals, raw total supply and token-program identity when RPC providers are available. Helius DAS metadata is optional. Market price, liquidity, 5-minute/24-hour change, 24-hour volume and transaction count can now be populated from DexScreener when a matching pair exists. The backend polls active subscribed tokens in batches and relays those quotes over its own WebSocket. This is periodic REST polling, not a provider-pushed market feed. Historical chart candles, decoded trades, holder counts and top-holder indexing still return unavailable/null values until their data sources and storage are implemented. Do not present this as a complete or production-ready market-data service.

## Market data source

The first market-data adapter uses the public DexScreener Solana token-pairs endpoint. The adapter batches up to 30 mint addresses per request and selects the highest-liquidity pair where the requested mint is the base token. The upstream API documents a 300-request/minute limit for this endpoint. The service polls active subscriptions every five seconds; this is suitable for an initial private/testing data layer, not a guaranteed sub-second feed. A dedicated paid streaming provider can be added later behind the same market-data interface if the required latency and budget justify it.

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
