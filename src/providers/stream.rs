use crate::state::AppState;
use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

const MAX_CONCURRENT_TRANSACTION_FETCHES: usize = 32;
const SUBSCRIPTION_REFRESH: Duration = Duration::from_secs(2);
const SUBSCRIPTION_ACK_TIMEOUT: Duration = Duration::from_secs(15);

/// Maintains mint-scoped logsSubscribe subscriptions on one provider socket.
/// A mint is considered active only after the provider acknowledges it.
pub async fn run_connection(
    state: &AppState,
    endpoint: &str,
    provider: &'static str,
) -> Result<()> {
    let (mut socket, _) = connect_async(endpoint)
        .await
        .with_context(|| format!("{provider} WebSocket connection failed"))?;
    info!(provider, "Solana provider WebSocket connected");

    // Bound in-flight transaction RPC calls so a burst of logs cannot create
    // an unbounded number of detached tasks.
    let fetch_slots = Arc::new(Semaphore::new(MAX_CONCURRENT_TRANSACTION_FETCHES));
    let mut request_id = 1_u64;
    let mut pending = HashMap::<u64, (String, Instant)>::new();
    let mut subscription_to_mint = HashMap::<u64, String>::new();
    let mut mint_to_subscription = HashMap::<String, u64>::new();
    let mut refresh = tokio::time::interval(SUBSCRIPTION_REFRESH);
    let mut keepalive = tokio::time::interval(Duration::from_secs(30));

    loop {
        tokio::select! {
            _ = keepalive.tick() => {
                socket.send(Message::Ping(Vec::new().into())).await
                    .context("Provider WebSocket ping failed")?;
            }
            _ = refresh.tick() => {
                let now = Instant::now();
                let expired = expired_subscription_requests(&pending, now);
                if !expired.is_empty() {
                    let requests = expired
                        .iter()
                        .map(|(id, mint)| format!("{id}:{mint}"))
                        .collect::<Vec<_>>()
                        .join(",");
                    warn!(
                        provider,
                        requests,
                        "Subscription acknowledgement timed out; restarting socket so late acknowledgements cannot leak orphaned subscriptions"
                    );
                    return Err(anyhow!("{provider} subscription acknowledgement timed out"));
                }

                let desired = state.subscribed_mints().await.into_iter()
                    .collect::<std::collections::HashSet<_>>();
                let acknowledged = mint_to_subscription.keys()
                    .cloned()
                    .collect::<std::collections::HashSet<_>>();
                let awaiting = pending.values()
                    .map(|(mint, _)| mint.clone())
                    .collect::<std::collections::HashSet<_>>();

                for mint in desired.difference(&acknowledged).filter(|mint| !awaiting.contains(*mint)) {
                    let id = request_id;
                    request_id = request_id.wrapping_add(1).max(1);
                    pending.insert(id, (mint.clone(), Instant::now()));
                    let request = json!({
                        "jsonrpc":"2.0",
                        "id":id,
                        "method":"logsSubscribe",
                        "params":[{"mentions":[mint]},{"commitment":"confirmed"}]
                    });
                    socket.send(Message::Text(request.to_string().into())).await
                        .with_context(|| format!("{provider} mint subscription send failed"))?;
                }

                let removed = mint_to_subscription.keys()
                    .filter(|mint| !desired.contains(*mint))
                    .cloned()
                    .collect::<Vec<_>>();
                for mint in removed {
                    if let Some(subscription) = mint_to_subscription.remove(&mint) {
                        subscription_to_mint.remove(&subscription);
                        let id = request_id;
                        request_id = request_id.wrapping_add(1).max(1);
                        let request = json!({
                            "jsonrpc":"2.0",
                            "id":id,
                            "method":"logsUnsubscribe",
                            "params":[subscription]
                        });
                        socket.send(Message::Text(request.to_string().into())).await
                            .context("Provider unsubscribe send failed")?;
                    }
                }

                // Keep canceled requests correlated until their acknowledgement arrives.
                // If they time out, the socket is restarted above; this prevents a late
                // acknowledgement from creating an untracked provider-side subscription.
            }
            incoming = socket.next() => {
                let Some(incoming) = incoming else {
                    return Err(anyhow!("{provider} WebSocket closed"));
                };
                match incoming.context("Provider WebSocket receive failed")? {
                    Message::Text(text) => {
                        let payload: Value = match serde_json::from_str(&text) {
                            Ok(value) => value,
                            Err(error) => {
                                warn!(provider, error = %error, "Ignoring malformed provider WebSocket message");
                                continue;
                            }
                        };

                        if let Some(id) = payload.get("id").and_then(Value::as_u64) {
                            if let Some((mint, _)) = pending.remove(&id) {
                                if let Some(subscription) = payload.get("result").and_then(Value::as_u64) {
                                    if state.subscribed_mints().await.iter().any(|active| active == &mint) {
                                        subscription_to_mint.insert(subscription, mint.clone());
                                        mint_to_subscription.insert(mint.clone(), subscription);
                                        info!(provider, mint, subscription, "Provider mint subscription acknowledged");
                                    } else {
                                        let unsubscribe_id = request_id;
                                        request_id = request_id.wrapping_add(1).max(1);
                                        let request = json!({
                                            "jsonrpc":"2.0",
                                            "id":unsubscribe_id,
                                            "method":"logsUnsubscribe",
                                            "params":[subscription]
                                        });
                                        socket.send(Message::Text(request.to_string().into())).await?;
                                    }
                                } else if let Some(error) = payload.get("error") {
                                    warn!(provider, mint, error = %error, "Provider rejected mint subscription; it will be retried");
                                } else {
                                    warn!(provider, mint, "Provider returned an invalid subscription acknowledgement");
                                }
                            }
                            continue;
                        }

                        if payload.get("method").and_then(Value::as_str) != Some("logsNotification") {
                            continue;
                        }
                        let Some(subscription) = payload.pointer("/params/subscription").and_then(Value::as_u64) else {
                            continue;
                        };
                        let Some(mint) = subscription_to_mint.get(&subscription).cloned() else {
                            continue;
                        };
                        let Some(value) = payload.pointer("/params/result/value") else {
                            continue;
                        };
                        if !value.get("err").is_some_and(Value::is_null) {
                            continue;
                        }
                        let Some(signature) = value.get("signature").and_then(Value::as_str) else {
                            continue;
                        };
                        let logs = value.get("logs").and_then(Value::as_array).cloned().unwrap_or_default();
                        let permit = match acquire_fetch_permit(&fetch_slots) {
                            Ok(permit) => permit,
                            Err(error) => {
                                warn!(
                                    provider,
                                    mint,
                                    signature,
                                    limit = MAX_CONCURRENT_TRANSACTION_FETCHES,
                                    error = %error,
                                    "Transaction fetch capacity exhausted; closing provider stream to prevent unbounded task growth"
                                );
                                return Err(error);
                            }
                        };
                        let state = state.clone();
                        let mint = mint.clone();
                        let signature = signature.to_owned();
                        tokio::spawn(async move {
                            // The permit is released automatically when this task exits.
                            let _permit = permit;
                            match state.provider_client.fetch_transaction(&signature).await {
                                Ok(transaction) => {
                                    let trades = super::swap_decoder::decode_trades(
                                        &mint,
                                        &signature,
                                        &transaction,
                                        &logs,
                                        &state.market_client,
                                    ).await;
                                    for trade in trades {
                                        if let Err(error) = state.publish_confirmed_trade(&mint, trade).await {
                                            warn!(provider, mint, error = %error, "Could not persist decoded swap");
                                        }
                                    }
                                }
                                Err(error) => warn!(provider, mint, signature, error = %error, "Could not fetch confirmed transaction"),
                            }
                        });
                    }
                    Message::Ping(payload) => socket.send(Message::Pong(payload)).await?,
                    Message::Close(_) => return Err(anyhow!("{provider} WebSocket closed by remote")),
                    _ => {}
                }
            }
        }
    }
}

fn expired_subscription_requests(
    pending: &HashMap<u64, (String, Instant)>,
    now: Instant,
) -> Vec<(u64, String)> {
    pending
        .iter()
        .filter(|(_, (_, sent_at))| now.duration_since(*sent_at) >= SUBSCRIPTION_ACK_TIMEOUT)
        .map(|(id, (mint, _))| (*id, mint.clone()))
        .collect()
}

fn acquire_fetch_permit(semaphore: &Arc<Semaphore>) -> Result<OwnedSemaphorePermit> {
    semaphore
        .clone()
        .try_acquire_owned()
        .map_err(|_| anyhow!("maximum concurrent transaction fetches reached"))
}

#[cfg(test)]
mod tests {
    use super::{acquire_fetch_permit, MAX_CONCURRENT_TRANSACTION_FETCHES};
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    #[test]
    fn transaction_fetch_concurrency_is_bounded_and_permits_are_released() {
        let semaphore = Arc::new(Semaphore::new(2));
        let first = acquire_fetch_permit(&semaphore).unwrap();
        let second = acquire_fetch_permit(&semaphore).unwrap();

        assert!(acquire_fetch_permit(&semaphore).is_err());
        drop(first);

        assert!(acquire_fetch_permit(&semaphore).is_ok());
        drop(second);
    }

    #[test]
    fn expired_subscription_requests_are_detected_for_connection_restart() {
        use super::{expired_subscription_requests, SUBSCRIPTION_ACK_TIMEOUT};
        use std::{collections::HashMap, time::Instant};

        let now = Instant::now();
        let pending = HashMap::from([
            (
                1,
                ("mint-expired".to_owned(), now - SUBSCRIPTION_ACK_TIMEOUT),
            ),
            (2, ("mint-live".to_owned(), now)),
        ]);

        assert_eq!(
            expired_subscription_requests(&pending, now),
            vec![(1, "mint-expired".to_owned())]
        );
    }

    #[test]
    fn configured_fetch_limit_is_positive_and_bounded() {
        assert_eq!(MAX_CONCURRENT_TRANSACTION_FETCHES, 32);
    }
}
