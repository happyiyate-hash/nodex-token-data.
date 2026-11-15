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

- `token.market`: currently emits price, market cap, 5-minute/24-hour change, liquidity, 24-hour volume and transaction count when supplied by the selected DEX pair.
- `token.chart`: reserved for live candle updates; historical candles are loaded in the REST snapshot when GeckoTerminal has data for the selected pool and timeframe.
- `token.trades`: reserved for live confirmed buy/sell transaction events; recent pool trades are loaded in the REST snapshot when GeckoTerminal has data.
- `token.activity`: reserved for floating buy/sell activity; no publisher is connected yet.
- `token.holders`: reserved for holder count, largest holders and ownership concentration; no indexer is connected yet.
- `token.info` and `token.creator`: reserved for changes to verified metadata; no live publishers are connected yet.

Only `token.market` currently emits live events. The other topics are accepted by the subscription protocol but remain silent until their data pipelines are implemented.

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
- GeckoTerminal historical OHLCV and recent pool-trade snapshots, with 60-second caches and shared request pacing.
- Batched market quote polling for active WebSocket subscriptions, publishing `token.market` events through the existing broadcast bus.
- Optional Helius DAS `getAsset` metadata enrichment (name, symbol, image and explicitly identified creator fields when returned).
- Short-lived, single-use WebSocket tickets.
- Authenticated WebSocket handshake, topic subscription acknowledgements, unsubscribe and chart-timeframe command.
- In-process broadcast event bus for provider/indexer workers to publish typed topic events.
- CI checks for format, Clippy, tests and release build.

Not yet implemented:
- Live trade decoding/publishing and holder indexing.
- Second-level chart history (`1S`, `5S`, `10S`), which the current historical market-data source does not provide.
- Alternate metadata-provider fallback when Helius DAS is unavailable or returns no metadata.
- Persistent historical indexing, retention, candle aggregation and backfill after service downtime. The current market publisher is active but uses five-second REST polling.
- Production deployment and TLS verification.

**Important:** The snapshot now returns verified on-chain mint decimals, raw total supply and token-program identity when RPC providers are available. Helius DAS metadata is optional. Market price, liquidity, 5-minute/24-hour change, 24-hour volume and transaction count can be populated from DexScreener when a matching pair exists. The backend polls active subscribed tokens in batches and relays those quotes over its own WebSocket; this is periodic REST polling, not a provider-pushed market feed. The initial REST snapshot can also load historical candles and recent trades from GeckoTerminal for supported timeframes. GeckoTerminal's public API is cached for one minute and limited to approximately 10 requests per minute, so chart/trade availability is best-effort and not suitable as a high-volume production feed. The `1S`, `5S` and `10S` chart selections intentionally return no historical candles rather than inventing second-level data. Live trade events, holder counts and top-holder indexing remain unimplemented. Do not present this as a complete or production-ready market-data service.

## Market data source

The current market-data layer uses the public DexScreener Solana token-pairs endpoint for live market snapshots and the GeckoTerminal public API for historical candles and recent pool trades. The adapter batches up to 30 mint addresses per request and selects the highest-liquidity pair where the requested mint is the base token. The upstream API documents a 300-request/minute limit for this endpoint. The service polls active subscriptions every five seconds, batches up to 30 mints per request, and caps the number of unique active mints at 500. At that cap, one full poll cycle uses at most 17 requests (about 204 requests/minute before snapshot lookups), below the documented endpoint limit. This is suitable for an initial private/testing data layer, not a guaranteed sub-second feed. GeckoTerminal chart and trade requests are separately cached for 60 seconds in this service to reduce duplicate upstream calls, and a shared request gate spaces uncached GeckoTerminal calls at least seven seconds apart to stay below the public API's approximate rate limit. A dedicated paid streaming provider can be added later behind the same market-data interface if the required latency and budget justify it.

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
