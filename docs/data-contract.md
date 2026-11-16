# NodeX Token Data — frontend data contract

This document separates one-time/page-load data from live values and records the actual provider coverage. Do not assume a topic is live merely because the WebSocket accepts its name.

## Snapshot groups

| Group | Current source and fields | Refresh policy |
|---|---|---|
| Identity | Solana mint from request; optional Helius DAS name, symbol, image and explicit creator fields | Fetch on token open; cache; refresh on explicit metadata invalidation |
| Token info | Solana RPC getTokenSupply and getAccountInfo: decimals, raw total supply and token program | Fetch on open; refresh only when on-chain account data changes |
| Market | DexScreener matching base-token pair: USD price, reported market cap, liquidity, 5m/24h change, 24h volume and transaction count | Initial snapshot, then five-second batched REST polling relayed over token.market WebSocket events |
| Chart | Historical seed from GeckoTerminal; SQLite stores imported candles and locally derived candles | Fetch history on open/timeframe change, seed without overwriting locally built candles, then load stored history. SQLite can aggregate confirmed on-chain swaps into 1s, 5s, 10s, 1m, 5m, 15m, 30m, 1h and 24h candles. Historical provider backfill is bounded to a 48-hour window and paged in batches of up to 1,000 candles. No on-chain swap source is connected yet. 1S, 5S and 10S remain unavailable. |
| Trades | GeckoTerminal recent pool swaps: signature, side, wallet, token amount, USD price, USD volume and timestamp | Initial snapshot; cached locally for 60 seconds. No live trade publisher yet. Counter-asset amount is not labeled as SOL. |
| Holders | No holder indexer connected | Unavailable until indexed account data is implemented |
| Creator | Optional explicit creator fields from Helius DAS; update authority is not treated as creator | Fetch on open; no live creator publisher yet |

Market price streaming is polling-based, not a provider-pushed stream. Historical candle persistence and swap-to-candle aggregation are implemented in SQLite, but the on-chain swap ingestion worker has not yet been connected. The public DexScreener endpoint supports batches of up to 30 addresses and documents a 300-request/minute limit. The backend caps unique active mint subscriptions at 500; at the cap, a five-second cycle uses at most 17 market requests.

GeckoTerminal's public API is cached upstream for one minute and limited to approximately 10 requests/minute. The backend caches chart and trade responses for 60 seconds and spaces uncached requests by at least seven seconds. This is an initial private/testing source, not a high-volume production feed.

## WebSocket topics

- token.market — currently emits DexScreener market quotes.
- token.chart — accepted but no live candle publisher yet.
- token.trades — accepted but no live trade publisher yet.
- token.activity — accepted but no publisher yet.
- token.holders — accepted but no indexer/publisher yet.
- token.info and token.creator — accepted but no live publishers yet.

One socket multiplexes all selected topics. Events include mint, topic, sequence and timestamp. Consumers should discard duplicate/out-of-order sequence values and resnapshot after a detected gap or reconnect.

## Separate trading-engine contract

Position and portfolio state are owned by the existing NodeX trading engine, not this service. The frontend combines the market snapshot with the trading engine's position snapshot and position stream. No position means the frontend renders its real empty state.

## Data integrity rules

- null means unknown/unavailable; zero means a verified zero.
- Empty arrays mean no records were returned, not necessarily that none exist unless the status confirms completeness.
- Every data group should eventually include source, last-updated time, completeness and freshness.
- Holder counts and top-holder rankings require indexed account data; a single largest-accounts RPC call is not a complete holder index.
- Market cap uses the provider's reported market-cap field; fully diluted valuation is not substituted when market cap is absent.
- Chart history comes from the historical pool-data source, not generated demo points. Imported history is stored in SQLite; locally observed confirmed swaps will update the same candle store once the DEX decoder is connected.
- Token amount and USD volume in the trade snapshot are kept distinct; the quote-asset amount is not assumed to be SOL.


## Confirmed-trade gap recovery

The read-only endpoint `GET /api/v1/tokens/{mint}/trades/missed?after={cursor}&limit=500`
returns confirmed on-chain trades after a per-mint, monotonically increasing SQLite cursor.
Each `token.trades` WebSocket event includes the same `data.cursor`. Clients should
subscribe first, backfill from their last cursor, and pass recovered trades through
the same signature/event-index deduplication path as live trades. Responses include
`head_cursor`, `next_cursor`, and `has_more` for pagination. The per-token cursor is retained when market cache rows expire, so the sequence
continues across normal cache eviction. `cursor_reset` is reserved for a database
reset or a cursor that is ahead of the current head; in that case the client restarts
from retained trades and refreshes its REST candle baseline.
