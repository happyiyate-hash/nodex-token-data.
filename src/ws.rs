use std::{collections::HashSet, time::{SystemTime, UNIX_EPOCH}};

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
};
use futures_util::{Sink, SinkExt, StreamExt};
use serde_json::json;

use crate::{
    components::{chart_data, creator_data, holder_data, market_data, token_identity, token_info, trade_data},
    state::AppState,
    stream::{ClientCommand, TOPICS},
};

pub async fn connect(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();
    let mut events = state.stream_tx.subscribe();
    let mut selected_mint: Option<String> = None;
    let mut selected_topics: HashSet<String> = HashSet::new();

    loop {
        tokio::select! {
            incoming = receiver.next() => {
                let Some(Ok(message)) = incoming else {
                    break;
                };

                match message {
                    Message::Text(text) => {
                        let command = match serde_json::from_str::<ClientCommand>(&text) {
                            Ok(command) => command,
                            Err(_) => {
                                if send_json(&mut sender, json!({
                                    "event": "error",
                                    "code": "invalid_command"
                                })).await.is_err() {
                                    break;
                                }
                                continue;
                            }
                        };

                        match command.command.as_str() {
                            "subscribe" => {
                                let Some(mint) = command.mint.filter(|mint| {
                                    !mint.trim().is_empty() && mint.len() <= 64
                                }) else {
                                    if send_json(&mut sender, json!({
                                        "event": "error",
                                        "code": "invalid_mint"
                                    })).await.is_err() {
                                        break;
                                    }
                                    continue;
                                };

                                let invalid: Vec<&String> = command.topics.iter()
                                    .filter(|topic| !TOPICS.contains(&topic.as_str()))
                                    .collect();

                                if !invalid.is_empty() {
                                    if send_json(&mut sender, json!({
                                        "event": "error",
                                        "code": "unsupported_topics",
                                        "topics": invalid
                                    })).await.is_err() {
                                        break;
                                    }
                                    continue;
                                }

                                if command.topics.is_empty() {
                                    if send_json(&mut sender, json!({
                                        "event": "error",
                                        "code": "empty_topics"
                                    })).await.is_err() {
                                        break;
                                    }
                                    continue;
                                }

                                if selected_mint.as_deref() != Some(mint.as_str()) {
                                    if let Some(previous_mint) = selected_mint.take() {
                                        state.untrack_mint(&previous_mint).await;
                                    }
                                    if !state.track_mint(&mint).await {
                                        if send_json(&mut sender, json!({
                                            "event": "error",
                                            "code": "active_token_limit",
                                            "limit": 10
                                        })).await.is_err() {
                                            break;
                                        }
                                        continue;
                                    }
                                    selected_mint = Some(mint.clone());
                                }
                                selected_topics = command.topics.into_iter().collect();

                                if send_json(&mut sender, json!({
                                    "event": "subscribed",
                                    "mint": mint,
                                    "topics": selected_topics
                                })).await.is_err() {
                                    break;
                                }
                                // Send the initial token snapshot over this socket in
                                // independent sections. The chart is deliberately last and
                                // chunked oldest-to-newest so other UI sections can render first.
                                if stream_snapshot(&mut sender, &state, &mint, "1M").await.is_err() {
                                    break;
                                }
                            }
                            "request_snapshot" => {
                                if let Some(mint) = selected_mint.as_deref() {
                                    let timeframe = command.timeframe.as_deref().unwrap_or("1M");
                                    if !["1S", "5S", "10S", "1M", "5M", "15M", "30M", "1H", "24H"].contains(&timeframe) {
                                        if send_json(&mut sender, json!({"event":"error","code":"unsupported_timeframe"})).await.is_err() { break; }
                                    } else if stream_snapshot(&mut sender, &state, mint, timeframe).await.is_err() {
                                        break;
                                    }
                                } else if send_json(&mut sender, json!({"event":"error","code":"not_subscribed"})).await.is_err() {
                                    break;
                                }
                            }
                            "unsubscribe" => {
                                selected_topics.clear();
                                if let Some(previous_mint) = selected_mint.take() {
                                    state.untrack_mint(&previous_mint).await;
                                }
                                if send_json(&mut sender, json!({
                                    "event": "unsubscribed"
                                })).await.is_err() {
                                    break;
                                }
                            }
                            "set_chart_timeframe" => {
                                let valid = command.timeframe.as_deref().is_some_and(|timeframe| {
                                    ["1S", "5S", "10S", "1M", "5M", "15M", "30M", "1H", "24H"]
                                        .contains(&timeframe)
                                });

                                if !valid {
                                    if send_json(&mut sender, json!({
                                        "event": "error",
                                        "code": "unsupported_timeframe"
                                    })).await.is_err() {
                                        break;
                                    }
                                } else {
                                    if let (Some(mint), Some(timeframe)) = (selected_mint.as_deref(), command.timeframe.as_deref()) {
                                        if selected_topics.contains("token.chart") {
                                            if let Err(error) = state.publish_chart_snapshot(mint, timeframe).await {
                                                if send_json(&mut sender, json!({
                                                    "event": "error",
                                                    "code": "chart_snapshot_failed",
                                                    "message": error.to_string()
                                                })).await.is_err() { break; }
                                            }
                                        }
                                    }
                                    if send_json(&mut sender, json!({
                                        "event": "chart_timeframe_ack",
                                        "mint": selected_mint,
                                        "timeframe": command.timeframe
                                    })).await.is_err() { break; }
                                }
                            }
                            _ => {
                                if send_json(&mut sender, json!({
                                    "event": "error",
                                    "code": "unknown_command"
                                })).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    Message::Ping(payload) => {
                        if sender.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }
            event = events.recv() => {
                match event {
                    Ok(event)
                        if selected_mint.as_deref() == Some(event.mint.as_str())
                            && selected_topics.contains(&event.topic) =>
                    {
                        let value = serde_json::to_value(event)
                            .unwrap_or_else(|_| json!({"event": "serialization_error"}));
                        if send_json(&mut sender, value).await.is_err() {
                            break;
                        }
                    }
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        if send_json(&mut sender, json!({
                            "event": "resync_required",
                            "reason": "stream_lagged"
                        })).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }

    if let Some(mint) = selected_mint {
        state.untrack_mint(&mint).await;
    }
}


/// Delivers a token snapshot progressively on the requesting WebSocket.
/// Market/identity/holder/trade sections are independent messages; chart history
/// is sent in bounded oldest-first chunks and has an explicit completion marker.
async fn stream_snapshot<S>(
    sender: &mut S,
    state: &AppState,
    mint: &str,
    timeframe: &str,
) -> Result<(), ()>
where
    S: Sink<Message> + Unpin,
{
    let snapshot_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    send_json(sender, json!({
        "event": "snapshot_started",
        "snapshot_id": snapshot_id,
        "mint": mint,
        "timeframe": timeframe
    })).await?;

    let chain = match state.provider_client.fetch_token_chain_data(mint).await {
        Ok(chain) => chain,
        Err(_) => {
            send_json(sender, json!({
                "event": "snapshot_error",
                "snapshot_id": snapshot_id,
                "mint": mint,
                "message": "Token mint data could not be retrieved from configured Solana RPC providers"
            })).await?;
            return Ok(());
        }
    };

    let identity = token_identity::from_chain(mint, &chain, None);
    let info = token_info::from_chain(&chain);
    send_json(sender, json!({
        "event": "snapshot_section", "snapshot_id": snapshot_id,
        "mint": mint, "topic": "token.info", "data": info
    })).await?;
    send_json(sender, json!({
        "event": "snapshot_section", "snapshot_id": snapshot_id,
        "mint": mint, "topic": "token.identity", "data": identity
    })).await?;

    let (quote_result, holders_result, created_result) = tokio::join!(
        market_data::fetch(mint, &state.market_client),
        holder_data::fetch(mint, &chain.total_supply, chain.decimals, &state.provider_client),
        state.provider_client.fetch_mint_created_at(mint),
    );
    let quote = quote_result.ok().flatten();
    let holders = holders_result.ok();
    let created_at = created_result.ok().flatten();
    let pair = quote.as_ref().and_then(|value| value.pair_address.as_deref());

    let (buy_volume, sell_volume, buy_count, sell_count) =
        state.market_store.load_trade_totals_24h(mint).await.unwrap_or((None, None, 0, 0));
    let buy_pressure_pct = match (buy_volume, sell_volume) {
        (Some(buy), Some(sell)) if buy + sell > 0.0 => Some(buy / (buy + sell) * 100.0),
        (Some(buy), None) if sell_count == 0 && buy > 0.0 => Some(100.0),
        (None, Some(sell)) if buy_count == 0 && sell > 0.0 => Some(0.0),
        _ => None,
    };
    let market = if let Some(quote) = quote.as_ref() {
        json!({
            "price_usd": quote.price_usd,
            "market_cap_usd": quote.market_cap_usd,
            "liquidity_usd": quote.liquidity_usd,
            "change_5m_pct": quote.change_5m_pct,
            "change_24h_pct": quote.change_24h_pct,
            "volume_24h_usd": quote.volume_24h_usd,
            "transactions_24h": quote.transactions_24h.or(Some(buy_count + sell_count)),
            "buy_volume_usd": buy_volume,
            "sell_volume_usd": sell_volume,
            "buy_pressure_pct": buy_pressure_pct,
            "price_source": "snapshot_reference"
        })
    } else {
        json!({
            "buy_volume_usd": buy_volume,
            "sell_volume_usd": sell_volume,
            "buy_pressure_pct": buy_pressure_pct,
            "transactions_24h": buy_count + sell_count
        })
    };
    send_json(sender, json!({
        "event": "snapshot_section", "snapshot_id": snapshot_id,
        "mint": mint, "topic": "token.market", "data": market
    })).await?;

    if let Some(holders) = holders {
        send_json(sender, json!({
            "event": "snapshot_section", "snapshot_id": snapshot_id,
            "mint": mint, "topic": "token.holders", "data": holders
        })).await?;
    }
    let mut identity = identity;
    identity.created_at = created_at;
    identity.creator_address = creator_data::creator_address(&chain);
    send_json(sender, json!({
        "event": "snapshot_section", "snapshot_id": snapshot_id,
        "mint": mint, "topic": "token.identity", "data": identity
    })).await?;

    send_json(sender, json!({
        "event": "chart_history_started",
        "snapshot_id": snapshot_id,
        "mint": mint,
        "topic": "token.chart",
        "data": {"timeframe": timeframe}
    })).await?;

    match chart_data::fetch(mint, timeframe, pair, &state.market_client, &state.market_store).await {
        Ok(points) => {
            const CHUNK_SIZE: usize = 500;
            let total_chunks = points.len().div_ceil(CHUNK_SIZE);
            for (chunk_index, chunk) in points.chunks(CHUNK_SIZE).enumerate() {
                send_json(sender, json!({
                    "event": "chart_history_chunk",
                    "snapshot_id": snapshot_id,
                    "mint": mint,
                    "topic": "token.chart",
                    "data": {
                        "timeframe": timeframe,
                        "chunk_index": chunk_index,
                        "total_chunks": total_chunks,
                        "points": chunk
                    }
                })).await?;
            }
            send_json(sender, json!({
                "event": "chart_history_completed",
                "snapshot_id": snapshot_id,
                "mint": mint,
                "topic": "token.chart",
                "data": {"timeframe": timeframe, "point_count": points.len(), "status": if points.is_empty() {"unavailable"} else {"available"}}
            })).await?;
        }
        Err(error) => {
            tracing::warn!(mint, timeframe, error = %error, "Progressive chart history request failed");
            send_json(sender, json!({
                "event": "chart_history_completed",
                "snapshot_id": snapshot_id,
                "mint": mint,
                "topic": "token.chart",
                "data": {"timeframe": timeframe, "point_count": 0, "status": "unavailable"}
            })).await?;
        }
    }

    match trade_data::fetch(mint, pair, &state.market_client, &state.market_store).await {
        Ok(trades) => send_json(sender, json!({
            "event": "snapshot_section", "snapshot_id": snapshot_id,
            "mint": mint, "topic": "token.trades", "data": {"recent": trades}
        })).await?,
        Err(error) => tracing::warn!(mint, error = %error, "Progressive trade snapshot request failed"),
    }

    send_json(sender, json!({
        "event": "snapshot_completed",
        "snapshot_id": snapshot_id,
        "mint": mint,
        "timeframe": timeframe
    })).await
}

async fn send_json<S>(sender: &mut S, value: serde_json::Value) -> Result<(), ()>
where
    S: Sink<Message> + Unpin,
{
    let text = serde_json::to_string(&value).map_err(|_| ())?;
    sender
        .send(Message::Text(text.into()))
        .await
        .map_err(|_| ())
}
