use crate::{
    components::{
        chart_data, creator_data, holder_data, market_data, token_identity, token_info, trade_data,
    },
    models::{HoldersSnapshot, TokenSnapshot},
    state::AppState,
};
use axum::http::StatusCode;

pub async fn load_snapshot(
    mint: &str,
    timeframe: &str,
    state: &AppState,
) -> Result<TokenSnapshot, (StatusCode, String)> {
    let chain = state
        .provider_client
        .fetch_token_chain_data(mint)
        .await
        .map_err(|_| {
            (
                StatusCode::BAD_GATEWAY,
                "Token mint data could not be retrieved from configured Solana RPC providers"
                    .to_owned(),
            )
        })?;
    let (quote_result, holders_result, created_result) = tokio::join!(
        market_data::fetch(mint, &state.market_client),
        holder_data::fetch(
            mint,
            &chain.total_supply,
            chain.decimals,
            &state.provider_client
        ),
        state.provider_client.fetch_mint_created_at(mint),
    );
    let quote = quote_result.ok().flatten();
    let holders = holders_result.unwrap_or(HoldersSnapshot {
        total_count: None,
        top_10_ownership_pct: None,
        largest: Vec::new(),
    });
    let created_at = created_result.ok().flatten();
    let pair = quote
        .as_ref()
        .and_then(|value| value.pair_address.as_deref());
    let chart = chart_data::fetch(
        mint,
        timeframe,
        pair,
        &state.market_client,
        &state.market_store,
    )
    .await
    .map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Stored chart history could not be loaded".to_owned(),
        )
    })?;
    let trades = trade_data::fetch(mint, pair, &state.market_client, &state.market_store)
        .await
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Stored trade history could not be loaded".to_owned(),
            )
        })?;

    let mut snapshot = TokenSnapshot::unavailable(mint.to_owned(), timeframe.to_owned());
    snapshot.identity = token_identity::from_chain(mint, &chain, created_at);
    snapshot.info = token_info::from_chain(&chain);
    snapshot.info.creator_address = creator_data::creator_address(&chain);
    snapshot.holders = holders;
    snapshot.chart.points = chart;
    snapshot.trades.recent = trades;
    let (buy_volume, sell_volume, buy_count, sell_count) = state
        .market_store
        .load_trade_totals_24h(mint)
        .await
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Confirmed trade totals could not be loaded".to_owned(),
            )
        })?;
    snapshot.market.buy_volume_usd = buy_volume;
    snapshot.market.sell_volume_usd = sell_volume;
    snapshot.market.buy_pressure_pct = match (buy_volume, sell_volume) {
        (Some(buy), Some(sell)) if buy + sell > 0.0 => Some(buy / (buy + sell) * 100.0),
        (Some(buy), None) if buy > 0.0 => Some(100.0),
        (None, Some(sell)) if sell > 0.0 => Some(0.0),
        _ => None,
    };
    if snapshot.market.transactions_24h.is_none() {
        snapshot.market.transactions_24h = Some(buy_count + sell_count);
    }

    if let Some(quote) = quote {
        snapshot.market.price_usd = quote.price_usd;
        snapshot.market.market_cap_usd = quote.market_cap_usd;
        snapshot.market.liquidity_usd = quote.liquidity_usd;
        snapshot.market.change_5m_pct = quote.change_5m_pct;
        snapshot.market.change_24h_pct = quote.change_24h_pct;
        snapshot.market.volume_24h_usd = quote.volume_24h_usd;
        snapshot.market.transactions_24h = quote.transactions_24h;
        if snapshot.market.price_usd.is_some() {
            if let Ok(Some(sol)) = state
                .market_client
                .fetch_quote("So11111111111111111111111111111111111111112")
                .await
            {
                snapshot.market.price_sol = match (snapshot.market.price_usd, sol.price_usd) {
                    (Some(token), Some(sol_price)) if sol_price > 0.0 => Some(token / sol_price),
                    _ => None,
                };
            }
        }
        snapshot.data_status.market = if snapshot.market.price_usd.is_some() {
            "available"
        } else {
            "partial"
        };
    }
    snapshot.data_status.identity =
        if snapshot.identity.name.is_some() && snapshot.identity.symbol.is_some() {
            "available"
        } else {
            "partial"
        };
    snapshot.data_status.info =
        if snapshot.info.decimals.is_some() && snapshot.info.total_supply.is_some() {
            "available"
        } else {
            "partial"
        };
    snapshot.data_status.chart = if snapshot.chart.points.is_empty() {
        "unavailable"
    } else {
        "available"
    };
    snapshot.data_status.trades = if snapshot.trades.recent.is_empty() {
        "unavailable"
    } else {
        "available"
    };
    snapshot.data_status.holders = if snapshot.holders.largest.is_empty() {
        "unavailable"
    } else {
        "partial"
    };
    Ok(snapshot)
}
