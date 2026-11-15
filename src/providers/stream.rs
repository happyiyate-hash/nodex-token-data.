use crate::state::AppState;
use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

/// Maintains mint-scoped logsSubscribe subscriptions on one provider socket.
pub async fn run_connection(
    state: &AppState,
    endpoint: &str,
    provider: &'static str,
) -> Result<()> {
    let (mut socket, _) = connect_async(endpoint)
        .await
        .with_context(|| format!("{provider} WebSocket connection failed"))?;
    info!(provider, "Solana provider WebSocket connected");
    let mut request_id = 1_u64;
    let mut pending = HashMap::<u64, String>::new();
    let mut subscription_to_mint = HashMap::<u64, String>::new();
    let mut mint_to_subscription = HashMap::<String, u64>::new();
    let mut active = HashSet::<String>::new();
    let mut refresh = tokio::time::interval(Duration::from_secs(5));
    let mut keepalive = tokio::time::interval(Duration::from_secs(30));
    loop {
        tokio::select! {
            _ = keepalive.tick() => {
                socket.send(Message::Ping(Vec::new().into())).await?;
            }
            _ = refresh.tick() => {
                let current = state.subscribed_mints().into_iter().collect::<HashSet<_>>();
                for mint in current.difference(&active) {
                    let id = request_id; request_id += 1; pending.insert(id, mint.clone());
                    socket.send(Message::Text(json!({"jsonrpc":"2.0","id":id,"method":"logsSubscribe","params":[{"mentions":[mint]},{"commitment":"confirmed"}]}).to_string().into())).await.context("Provider mint subscription failed")?;
                }
                for mint in active.difference(&current) {
                    if let Some(subscription) = mint_to_subscription.remove(mint) {
                        let id = request_id; request_id += 1;
                        socket.send(Message::Text(json!({"jsonrpc":"2.0","id":id,"method":"logsUnsubscribe","params":[subscription]}).to_string().into())).await?;
                        subscription_to_mint.remove(&subscription);
                    }
                }
                active = current;
            }
            incoming = socket.next() => {
                let Some(incoming) = incoming else { return Err(anyhow!("{provider} WebSocket closed")); };
                match incoming.context("Provider WebSocket receive failed")? {
                    Message::Text(text) => {
                        let payload: Value = match serde_json::from_str(&text) { Ok(v) => v, Err(_) => continue };
                        if let Some(id) = payload.get("id").and_then(Value::as_u64) {
                            if let Some(mint) = pending.remove(&id) {
                                if let Some(sub) = payload.get("result").and_then(Value::as_u64) {
                                    subscription_to_mint.insert(sub, mint.clone()); mint_to_subscription.insert(mint, sub);
                                } else if let Some(error) = payload.get("error") { warn!(provider, mint, error=%error, "Provider rejected mint subscription"); }
                            }
                            continue;
                        }
                        if payload.get("method").and_then(Value::as_str) != Some("logsNotification") { continue; }
                        let Some(sub) = payload.pointer("/params/subscription").and_then(Value::as_u64) else { continue; };
                        let Some(mint) = subscription_to_mint.get(&sub).cloned() else { continue; };
                        let Some(value) = payload.pointer("/params/result/value") else { continue; };
                        if !value.get("err").is_some_and(Value::is_null) { continue; }
                        let Some(signature) = value.get("signature").and_then(Value::as_str) else { continue; };
                        let logs = value.get("logs").and_then(Value::as_array).cloned().unwrap_or_default();
                        match state.provider_client.fetch_transaction(signature).await {
                            Ok(transaction) => {
                                if let Some(trade) = super::swap_decoder::decode_trade(&mint, signature, &transaction, &logs, &state.market_client).await {
                                    if let Err(error) = state.publish_confirmed_trade(&mint, trade).await { warn!(provider, mint, error=%error, "Could not persist decoded swap"); }
                                }
                            }
                            Err(error) => warn!(provider, mint, signature, error=%error, "Could not fetch confirmed transaction"),
                        }
                    }
                    Message::Ping(payload) => socket.send(Message::Pong(payload)).await?,
                    Message::Close(_) => return Err(anyhow!("{provider} WebSocket closed by remote")),
                    _ => {}
                }
            }
        }
    }
}
