use std::collections::HashSet;

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    response::IntoResponse,
};
use futures_util::{Sink, SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;

use crate::{
    state::AppState,
    stream::{ClientCommand, TOPICS},
};

#[derive(Deserialize)]
pub struct TicketQuery {
    ticket: String,
}

pub async fn connect(
    ws: WebSocketUpgrade,
    Query(query): Query<TicketQuery>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    if !state.consume_ws_ticket(&query.ticket) {
        return (
            axum::http::StatusCode::UNAUTHORIZED,
            "Invalid or expired WebSocket ticket",
        )
            .into_response();
    }

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

                                selected_mint = Some(mint.clone());
                                selected_topics = command.topics.into_iter().collect();

                                if send_json(&mut sender, json!({
                                    "event": "subscribed",
                                    "mint": mint,
                                    "topics": selected_topics
                                })).await.is_err() {
                                    break;
                                }
                            }
                            "unsubscribe" => {
                                selected_topics.clear();
                                selected_mint = None;
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
                                } else if send_json(&mut sender, json!({
                                    "event": "chart_timeframe_ack",
                                    "mint": selected_mint,
                                    "timeframe": command.timeframe
                                })).await.is_err() {
                                    break;
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
                    Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
}

async fn send_json<S>(sender: &mut S, value: serde_json::Value) -> Result<(), ()>
where
    S: Sink<Message> + Unpin,
{
    let text = serde_json::to_string(&value).map_err(|_| ())?;
    sender.send(Message::Text(text.into())).await.map_err(|_| ())
}
