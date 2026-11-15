use crate::{market::MarketDataClient, models::Trade};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::Value;

const PUMP: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const PUMP_SWAP: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const WSOL: &str = "So11111111111111111111111111111111111111112";
const USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const USDT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";

struct Swap {
    side: &'static str,
    trader: String,
    base: u64,
    quote: u64,
    quote_mint: Option<String>,
    sol: Option<f64>,
    time: i64,
}

pub async fn decode_trade(
    mint: &str,
    signature: &str,
    tx: &Value,
    logs: &[Value],
    market: &MarketDataClient,
) -> Option<Trade> {
    let lines = logs.iter().filter_map(Value::as_str).collect::<Vec<_>>();
    let event = decode_event(&lines, mint)?;
    let decimals = token_decimals(tx, mint)?;
    let token_amount = event.base as f64 / 10_f64.powi(decimals as i32);
    if token_amount <= 0.0 || !token_amount.is_finite() {
        return None;
    }
    let mut amount_sol = event.sol;
    let quote_usd = if let Some(sol) = event.sol.filter(|v| *v > 0.0) {
        market
            .fetch_quote(WSOL)
            .await
            .ok()
            .flatten()
            .and_then(|q| q.price_usd)
            .map(|p| sol * p)
    } else {
        let quote_mint = event
            .quote_mint
            .as_deref()
            .or_else(|| quote_mint_from_tx(tx, mint));
        match quote_mint {
            Some(WSOL) => {
                let decimals = token_decimals(tx, WSOL).unwrap_or(9);
                let sol = event.quote as f64 / 10_f64.powi(decimals as i32);
                amount_sol = Some(sol);
                market
                    .fetch_quote(WSOL)
                    .await
                    .ok()
                    .flatten()
                    .and_then(|q| q.price_usd)
                    .map(|p| sol * p)
            }
            Some(USDC) | Some(USDT) => {
                let decimals = token_decimals(tx, quote_mint.unwrap()).unwrap_or(6);
                Some(event.quote as f64 / 10_f64.powi(decimals as i32))
            }
            _ => None,
        }
    };
    let timestamp = if event.time > 0 {
        event.time
    } else {
        tx.get("blockTime")
            .and_then(Value::as_i64)
            .unwrap_or_default()
    };
    Some(Trade {
        signature: signature.into(),
        side: event.side.into(),
        trader: Some(event.trader),
        amount_sol,
        token_amount: Some(token_amount),
        price_usd: quote_usd.map(|v| v / token_amount),
        volume_usd: quote_usd,
        timestamp_ms: timestamp.saturating_mul(1000),
    })
}

fn decode_event(logs: &[&str], mint: &str) -> Option<Swap> {
    let pump = logs.iter().any(|line| line.contains(PUMP));
    let pump_swap = logs.iter().any(|line| line.contains(PUMP_SWAP));
    for line in logs {
        let Some(encoded) = line.strip_prefix("Program data: ") else {
            continue;
        };
        let Ok(bytes) = STANDARD.decode(encoded) else {
            continue;
        };
        if bytes.len() < 8 {
            continue;
        }
        if pump && bytes[..8] == [189, 219, 127, 211, 78, 230, 97, 238] {
            if bytes.len() < 97 || bs58::encode(&bytes[8..40]).into_string() != mint {
                continue;
            }
            let sol_raw = read_u64(&bytes, 40)?;
            let (quote_mint, quote_raw) = match decode_pump_quote(&bytes) {
                Some((mint, amount)) if mint != "11111111111111111111111111111111" && amount > 0 => (mint, amount),
                _ => (WSOL.to_owned(), sol_raw),
            };
            let sol = (quote_mint == WSOL).then_some(quote_raw as f64 / 1e9);
            return Some(Swap {
                side: if bytes[56] == 1 { "buy" } else { "sell" },
                trader: bs58::encode(&bytes[57..89]).into_string(),
                base: read_u64(&bytes, 48)?,
                quote: quote_raw,
                quote_mint: Some(quote_mint),
                sol,
                time: read_i64(&bytes, 89)?,
            });
        }
        if pump_swap {
            let d = &bytes[..8];
            let buy = d == [103, 244, 82, 31, 44, 245, 119, 119];
            let sell = d == [62, 47, 55, 10, 165, 3, 220, 42];
            if (!buy && !sell) || bytes.len() < 184 {
                continue;
            }
            return Some(Swap {
                side: if buy { "buy" } else { "sell" },
                trader: bs58::encode(&bytes[152..184]).into_string(),
                base: read_u64(&bytes, 16)?,
                quote: read_u64(&bytes, 64)?,
                quote_mint: None,
                sol: None,
                time: read_i64(&bytes, 8)?,
            });
        }
    }
    None
}
fn decode_pump_quote(bytes: &[u8]) -> Option<(String, u64)> {
    // TradeEvent's quote fields follow the Borsh ix_name string, mode/fee
    // fields and variable-length shareholders vector.
    let name_len = read_u32(bytes, 258)? as usize;
    let mut offset = 262_usize.checked_add(name_len)?;
    offset = offset.checked_add(1 + 8 * 4)?;
    let shareholder_count = read_u32(bytes, offset)? as usize;
    offset = offset.checked_add(4 + shareholder_count.checked_mul(34)?)?;
    let quote_mint = bs58::encode(bytes.get(offset..offset + 32)?).into_string();
    let quote_amount = read_u64(bytes, offset + 32)?;
    Some((quote_mint, quote_amount))
}

fn read_u32(b: &[u8], i: usize) -> Option<u32> {
    Some(u32::from_le_bytes(b.get(i..i + 4)?.try_into().ok()?))
}

fn token_decimals(tx: &Value, mint: &str) -> Option<u8> {
    for path in ["/meta/preTokenBalances", "/meta/postTokenBalances"] {
        if let Some(rows) = tx.pointer(path).and_then(Value::as_array) {
            for row in rows {
                if row.get("mint").and_then(Value::as_str) == Some(mint) {
                    return row
                        .pointer("/uiTokenAmount/decimals")
                        .and_then(Value::as_u64)
                        .and_then(|v| u8::try_from(v).ok());
                }
            }
        }
    }
    None
}
fn quote_mint_from_tx(tx: &Value, base: &str) -> Option<&str> {
    for path in ["/meta/preTokenBalances", "/meta/postTokenBalances"] {
        if let Some(rows) = tx.pointer(path).and_then(Value::as_array) {
            for row in rows {
                let Some(mint) = row.get("mint").and_then(Value::as_str) else {
                    continue;
                };
                if mint != base && [WSOL, USDC, USDT].contains(&mint) {
                    return Some(mint);
                }
            }
        }
    }
    None
}
fn read_u64(b: &[u8], i: usize) -> Option<u64> {
    Some(u64::from_le_bytes(b.get(i..i + 8)?.try_into().ok()?))
}
fn read_i64(b: &[u8], i: usize) -> Option<i64> {
    Some(i64::from_le_bytes(b.get(i..i + 8)?.try_into().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn decodes_pump_trade_event() {
        let mut b = vec![0_u8; 97];
        b[..8].copy_from_slice(&[189, 219, 127, 211, 78, 230, 97, 238]);
        b[8..40].copy_from_slice(&[1; 32]);
        b[40..48].copy_from_slice(&1_000_000_000_u64.to_le_bytes());
        b[48..56].copy_from_slice(&2_000_000_u64.to_le_bytes());
        b[56] = 1;
        b[57..89].copy_from_slice(&[2; 32]);
        b[89..97].copy_from_slice(&1_700_000_000_i64.to_le_bytes());
        let logs = vec![
            format!("Program {PUMP} invoke [1]"),
            format!("Program data: {}", STANDARD.encode(b)),
        ];
        let refs = logs.iter().map(String::as_str).collect::<Vec<_>>();
        let event = decode_event(&refs, &bs58::encode([1; 32]).into_string()).unwrap();
        assert_eq!(event.side, "buy");
        assert_eq!(event.base, 2_000_000);
    }
}
