# NodeX Token Data

Independent Rust/Axum service for Solana token identity, mint information, market quotes, chart history, confirmed trades, holder snapshots and live WebSocket updates. It is separate from the NodeX trading engine and does not create, sign, submit or manage orders. Paper positions and portfolio state remain owned by the trading engine.

## Implementation status

| Area | Implemented | Limits / current behavior |
|---|---|---|
| HTTP service | Axum/Tokio; health, provider status, token snapshot and missed-trade routes | Process health does not prove upstream provider health |
| Solana RPC | Helius and FluxRPC HTTP clients, provider key rotation, read-only getSlot checks, fallback | Requires configured provider credentials |
| Provider WebSockets | Helius-first / FluxRPC fallback supervision; mint-scoped logsSubscribe; confirmed transaction retrieval | Live ingestion depends on provider connectivity |
| Token identity/info | Mint/account data, decimals, raw supply, token program; optional Helius DAS metadata | Metadata fields may be absent |
| Market quotes | DexScreener Solana pairs; selects highest-liquidity matching base-token pair | Third-party reference data, not provider-pushed ticks |
| Live market | Confirmed decoded swaps publish immediate market updates; DexScreener refreshes reference metrics every 30 seconds | Swap decoder currently covers Pump.fun and PumpSwap |
| Historical chart | GeckoTerminal OHLCV for 1M, 5M, 15M, 30M, 1H and 24H; SQLite persistence | No fabricated historical 1S/5S/10S candles |
| Live chart | Confirmed decoded swaps update local 1S, 5S, 10S, 1M, 5M, 15M, 30M, 1H and 24H candles | Only supported decoded swaps contribute |
| Trades/activity | GeckoTerminal recent-trade snapshot; live confirmed swaps persisted and published to trade/activity topics | Not broad DEX coverage |
| Missed trades | SQLite cursor endpoint, signature/event-index deduplication | Only trades actually ingested and retained; no archival chain-wide backfill |
| Holders | Largest-account/ownership concentration reads and five-minute refresh | Not a complete indexed holder database |
| Creator | Explicit creator metadata when available | Update authority is not automatically creator |
| Positions | Not implemented here | Trading engine owns positions and portfolio |
| Authentication | Not implemented in current routes | No API-key middleware or WebSocket ticket validation; CORS currently allows any origin |
| Deployment | GitHub Actions build and EC2 deployment workflow | Must verify each run and live health; queued/committed is not proof of deployment |

## Architecture and data flow

- Runtime: Rust 2021, Tokio, Axum, SQLx and SQLite.
- Solana providers: Helius primary, FluxRPC fallback. HTTP and WebSocket endpoints are configured separately; configured keys rotate.
- Market reference: DexScreener public Solana token-pairs endpoint.
- Historical pool data: GeckoTerminal public API for OHLCV and recent pool trades.
- On-chain ingestion: provider logsSubscribe for monitored mints, confirmed transaction fetch, Pump.fun/PumpSwap event decoding.
- Persistence: SQLite for imported candles, confirmed swaps and per-mint trade cursors.
- Delivery: in-process Tokio broadcast bus multiplexed through the token WebSocket.
- Execution and portfolio: separate NodeX trading-engine service.

The service tracks at most 100 unique mints. Unsubscribed mints remain monitored during a five-minute grace period and cached rows are eligible for cleanup after ten minutes without watchers.

## HTTP API

Routes are registered in src/main.rs.

### GET /health

Returns service name, process status, provider configuration state and provider key counts. This is a process health endpoint, not a full upstream readiness test.

### GET /providers/status

Runs read-only getSlot checks against configured RPC providers and returns reachability, slot, latency and errors.

### GET /api/v1/tokens/{mint}/snapshot?timeframe=1M

Returns token identity, on-chain mint information, available market quote, holder snapshot, stored/imported candles, recent trades and confirmed buy/sell totals. Valid timeframes are 1S, 5S, 10S, 1M, 5M, 15M, 30M, 1H and 24H. The historical provider supplies only 1M and higher intervals listed above; second-level history is available only where locally recorded confirmed swaps have produced candles.

Missing data is not a verified zero. Consumers must respect nullable fields and data-status values.

### GET /api/v1/tokens/{mint}/trades/missed?after={cursor}&limit=500

Returns retained confirmed on-chain trades after a per-mint cursor. The limit is clamped to 1–500. The response includes head_cursor, next_cursor, has_more and cursor_reset. Clients should subscribe first, backfill from their last cursor, merge live and recovered trades through signature/event-index deduplication, and resnapshot if cursor_reset is true.

This is not an archival Solana backfill. It can only return swaps that this service successfully ingested and still retains.

## WebSocket API

Connect to ws://HOST:8081/ws/tokens (use wss:// when TLS is terminated by a deployment proxy).

Send a subscribe command:

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
      ],
      "timeframe": "1M"
    }

One connection tracks one mint and multiplexes selected topics. The server acknowledges the subscription and sends progressive snapshot sections; chart history is chunked oldest-first and ends with a completion event.

Supported commands:
- subscribe: track a mint and select topics.
- unsubscribe: stop tracking the selected mint.
- request_snapshot: send the progressive snapshot again.
- set_chart_timeframe: send a chart snapshot for another supported timeframe.

### Topic behavior

| Topic | Current behavior |
|---|---|
| token.market | Immediate confirmed-swap updates; slower DexScreener reference polling every 30 seconds |
| token.chart | Candle updates from confirmed decoded swaps; historical snapshot from SQLite/GeckoTerminal |
| token.trades | Confirmed decoded trade events with durable per-mint cursor |
| token.activity | Activity event for confirmed decoded swaps |
| token.holders | RPC holder refresh every five minutes for monitored mints |
| token.info | Included in snapshot; no separate live info-change publisher |
| token.creator | Included in identity/snapshot when available; no separate live creator-change publisher |

Events include event name, mint, topic, process-wide sequence, timestamp and data. Clients should reject duplicate/out-of-order sequence values and resynchronize after resync_required or reconnect.

## Data providers and limits

### DexScreener

Requests are batched up to 30 mint addresses. The service selects the matching pair where the requested mint is the base token and has the greatest reported USD liquidity. It reads price, reported market cap, liquidity, 5-minute and 24-hour changes, 24-hour volume, transaction counts and pair URL when present.

The background reference quote worker runs every 30 seconds. Confirmed on-chain swap prices take precedence when the stream event is assembled. DexScreener data can be delayed or unavailable.

### GeckoTerminal

Used for historical OHLCV and recent pool trades. Supported chart history is 1M, 5M, 15M, 30M, 1H and 24H. The 30-minute series is aggregated from 15-minute points. Historical requests are bounded to 48 hours and at most four pages of up to 1,000 rows.

Chart/trade responses are cached in process for 60 seconds. Uncached requests share a minimum seven-second request interval. This is best-effort public API access, not a guaranteed-latency or high-volume feed.

### Solana swap ingestion

The provider stream maintains mint-scoped logsSubscribe subscriptions for active monitored mints. Confirmed transactions are fetched with bounded concurrency and decoded for supported Pump.fun and PumpSwap events. New swaps are deduplicated by transaction signature and event index, persisted, and used to update candles and publish trade/activity/market topics.

Other Solana DEX programs and event layouts are not currently decoded. There is no automated archival RPC scan to recover every swap missed during provider outages. Live on-chain events depend on provider WebSocket availability and successful transaction retrieval.

### Holders and creator

Holder snapshots use available Solana RPC account data to estimate largest accounts and ownership concentration. This is not a complete indexed holder registry. Holder refresh runs every five minutes for monitored mints. Creator information is included only when explicitly returned by supported metadata/RPC sources.

## Persistence and data integrity

SQLite stores:
- Historical provider candles.
- Candles derived from confirmed decoded swaps.
- Confirmed on-chain trades.
- Per-mint monotonically increasing trade cursors.

Imported historical candles do not overwrite candles already created from local on-chain observations. Duplicate swaps are ignored using signature and event index. Cursor values advance only when a new trade is inserted. Cache cleanup removes a mint's candle/trade rows but retains its cursor.

The default database URL is sqlite://./data/nodex-token-data.db. Ensure the parent directory exists and the service account can write to it.

Rules:
- Unknown/unavailable data is not zero.
- Do not fabricate prices, candles, trades, holders or positions.
- Do not label a counter-asset amount as SOL unless the source identifies it as SOL.
- Do not substitute FDV for a missing market-cap field.
- The trading engine, not this service, is authoritative for positions and portfolio balances.

## Configuration

Copy .env.example to the runtime environment file.

| Variable | Purpose |
|---|---|
| NODEX_BIND_ADDR | HTTP bind address; default 0.0.0.0:8081 |
| NODEX_DATABASE_URL | SQLite URL/path; default sqlite://./data/nodex-token-data.db |
| HELIUS_API_KEYS | Comma-separated Helius keys |
| HELIUS_RPC_URL_TEMPLATE | Helius HTTP RPC URL containing {api_key} |
| HELIUS_WS_URL_TEMPLATE | Helius WebSocket URL containing {api_key} |
| FLUXRPC_API_KEYS | Comma-separated FluxRPC keys |
| FLUXRPC_RPC_URL_TEMPLATE | FluxRPC HTTP RPC URL containing {api_key} |
| FLUXRPC_WS_URL_TEMPLATE | FluxRPC WebSocket URL containing {api_key} |
| RUST_LOG | Tracing filter |

Important security note: .env.example currently lists NODEX_API_KEY, but the application does not read it and no API-key middleware is installed. The WebSocket route also has no ticket authentication. CORS currently allows any origin. Do not treat the service as an authenticated public API until those protections are implemented and tested. Never commit real provider credentials or runtime environment files.

## Run locally

Requirements: stable Rust toolchain and a writable SQLite data directory.

    mkdir -p data
    cp .env.example .env
    # Edit .env and provide at least one provider key set.
    cargo run

Health check:

    curl http://127.0.0.1:8081/health

Provider diagnostics:

    curl http://127.0.0.1:8081/providers/status

Quality checks and release build:

    cargo fmt --all -- --check
    cargo clippy --all-targets --all-features -- -D warnings
    cargo test --all-features
    cargo build --release

## Deployment

The GitHub Actions workflow at .github/workflows/ci.yml runs formatting, Clippy and tests for pushes and pull requests targeting main. A push to main builds the Linux release binary, uploads it as an artifact, and deploys it to the market-data EC2 instance.

The production GitHub environment requires:
- AWS_HOST: public IP or DNS name of the market-data EC2 instance.
- AWS_USER: SSH login user (currently ubuntu).
- AWS_SSH_PRIVATE_KEY: complete OpenSSH private key corresponding to the public key installed in that account's authorized_keys.

The currently inspected EC2 systemd configuration is:
- Service: nodex-token-data.service
- Working directory: /opt/nodex-token-data/app
- Executable: /opt/nodex-token-data/app/target/release/nodex-token-data
- Environment file: /etc/nodex-token-data.env
- Local health endpoint: http://127.0.0.1:8081/health

The workflow replaces the release executable, restarts the service, checks the local health endpoint and attempts to restore the previous executable if restart/health verification fails. It does not replace the environment file or SQLite database. Verify the GitHub Actions run and live service after every deployment; a commit or queued workflow is not proof of success.

## Repository layout

    src/api/          HTTP health, provider status, snapshot and missed-trade routes
    src/components/   Snapshot assembly for identity, market, chart, trades and holders
    src/providers/    RPC clients, provider rotation, WebSocket ingestion and swap decoding
    src/market.rs     DexScreener and GeckoTerminal integrations
    src/market_engine.rs In-memory market/candle calculations from confirmed trades
    src/state.rs      Active mint tracking, workers and event publishing
    src/storage.rs    SQLite schema, persistence, candles, trades and cursors
    src/stream.rs     WebSocket command and event contracts
    src/ws.rs         Multiplexed token WebSocket and progressive snapshots
