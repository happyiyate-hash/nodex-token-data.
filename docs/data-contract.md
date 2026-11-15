# NodeX Token Data — frontend data contract

This document separates one-time/page-load data from live values. It is the contract target; fields remain unavailable until the relevant provider/indexer is implemented.

## Snapshot groups

| Group | Fields | Refresh policy |
|---|---|---|
| Identity | name, symbol, logo, mint, creator address, creation time | Fetch on token open; cache; refresh on explicit metadata invalidation |
| Token info | decimals, total/circulating supply, token standard | Fetch on open; refresh only when on-chain metadata/account changes |
| Market | price USD/SOL, market cap, liquidity, percentage changes, 24h volume, transaction count, buy/sell volume and pressure | Initial snapshot, then WebSocket |
| Chart | historical OHLCV points and selected timeframe | Fetch history on open/timeframe change, then WebSocket updates |
| Trades | recent confirmed swaps and their side, wallet, SOL/token amount, price and timestamp | Initial recent-trades snapshot, then WebSocket |
| Holders | total count, top-10 concentration, largest holders and balances | Initial snapshot, then indexed refresh events; not necessarily every slot |
| Creator | creator wallet and available creator metadata | Fetch on open; update only if source data changes |

## WebSocket topics

- `token.market`
- `token.chart`
- `token.trades`
- `token.activity`
- `token.holders`
- `token.info`
- `token.creator`

One socket multiplexes all selected topics. Events include mint, topic, sequence and timestamp. Consumers should discard duplicate/out-of-order sequence values and resnapshot after a detected gap or reconnect.

## Separate trading-engine contract

Position and portfolio state are owned by the existing NodeX trading engine, not this service. The frontend combines the market snapshot with the trading engine's position snapshot and position stream. No position means the frontend renders its real empty state.

## Data integrity rules

- `null` means unknown/unavailable; zero means a verified zero.
- Empty arrays mean no records were returned, not necessarily that none exist unless the status confirms completeness.
- Every data group should eventually include source, last-updated time, completeness and freshness.
- Holder counts and top-holder rankings require indexed account data; a single largest-accounts RPC call is not a complete holder index.
- Market cap requires a verified price and a clearly defined supply basis.
- Chart history must be derived from indexed swaps or a trusted historical source, not generated demo points.
