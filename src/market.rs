use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{anyhow, Result};
use reqwest::Client;
use serde::Serialize;
use serde_json::Value;
use tracing::warn;

use crate::models::ChartPoint;

#[derive(Clone)]
pub struct MarketDataClient {
    http: Client,
    chart_cache: Arc<Mutex<HashMap<String, (Instant, Vec<ChartPoint>)>>>,
}

#[derive(Clone, Debug, Serialize)]
pub struct MarketQuote {
    pub mint: String,
    pub pair_address: Option<String>,
    pub dex_id: Option<String>,
    pub price_usd: Option<f64>,
    pub market_cap_usd: Option<f64>,
    pub liquidity_usd: Option<f64>,
    pub change_5m_pct: Option<f64>,
    pub change_24h_pct: Option<f64>,
    pub volume_24h_usd: Option<f64>,
    pub transactions_24h: Option<u64>,
    pub pair_url: Option<String>,
}

impl MarketDataClient {
    pub fn new() -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(8))
            .user_agent("NodeX-Token-Data/0.1")
            .build()?;
        Ok(Self {
            http,
            chart_cache: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Uses the public DexScreener token-pairs endpoint. Requests can contain
    /// up to 30 Solana mint addresses, allowing the streaming worker to batch
    /// active tokens instead of making one request per token.
    pub async fn fetch_quotes(&self, mints: &[String]) -> Result<HashMap<String, MarketQuote>> {
        if mints.is_empty() {
            return Ok(HashMap::new());
        }
        if mints.len() > 30 {
            return Err(anyhow!(
                "DexScreener batches are limited to 30 token addresses"
            ));
        }

        let addresses = mints.join(",");
        let url = format!("https://api.dexscreener.com/tokens/v1/solana/{addresses}");
        let response = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|_| anyhow!("Market-data request failed or timed out"))?;
        if !response.status().is_success() {
            return Err(anyhow!(
                "Market-data provider returned HTTP {}",
                response.status().as_u16()
            ));
        }

        let pairs: Vec<Value> = response
            .json()
            .await
            .map_err(|_| anyhow!("Market-data provider returned invalid JSON"))?;
        let mut quotes = HashMap::new();

        for pair in pairs {
            let Some(mint) = pair.pointer("/baseToken/address").and_then(Value::as_str) else {
                continue;
            };
            if !mints.iter().any(|candidate| candidate == mint) {
                continue;
            }

            let quote = MarketQuote {
                mint: mint.to_owned(),
                pair_address: pair
                    .get("pairAddress")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                dex_id: pair
                    .get("dexId")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                price_usd: parse_number(pair.get("priceUsd")),
                market_cap_usd: parse_number(pair.get("marketCap")),
                liquidity_usd: parse_number(pair.pointer("/liquidity/usd")),
                change_5m_pct: parse_number(pair.pointer("/priceChange/m5")),
                change_24h_pct: parse_number(pair.pointer("/priceChange/h24")),
                volume_24h_usd: parse_number(pair.pointer("/volume/h24")),
                transactions_24h: transaction_count(&pair),
                pair_url: pair
                    .get("url")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
            };

            match quotes.entry(mint.to_owned()) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(quote);
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    let current_liquidity = entry.get().liquidity_usd.unwrap_or(0.0);
                    if quote.liquidity_usd.unwrap_or(0.0) > current_liquidity {
                        entry.insert(quote);
                    }
                }
            }
        }

        Ok(quotes)
    }

    pub async fn fetch_quote(&self, mint: &str) -> Result<Option<MarketQuote>> {
        let mints = vec![mint.to_owned()];
        match self.fetch_quotes(&mints).await {
            Ok(quotes) => Ok(quotes.get(mint).cloned()),
            Err(error) => {
                warn!(error = %error, "Market-data snapshot lookup failed");
                Err(error)
            }
        }
    }

    /// Retrieves historical OHLCV from GeckoTerminal. Its public API is
    /// cached upstream for one minute and limited to approximately 10 calls
    /// per minute, so this client also caches each pool/timeframe for 60s.
    pub async fn fetch_chart(&self, pool: &str, timeframe: &str) -> Result<Vec<ChartPoint>> {
        let Some((period, aggregate, resample_30m)) = chart_period(timeframe) else {
            return Ok(Vec::new());
        };
        let cache_key = format!("{pool}:{timeframe}");
        if let Ok(cache) = self.chart_cache.lock() {
            if let Some((expires_at, points)) = cache.get(&cache_key) {
                if *expires_at > Instant::now() {
                    return Ok(points.clone());
                }
            }
        }

        let url = format!(
            "https://api.geckoterminal.com/api/v2/networks/solana/pools/{pool}/ohlcv/{period}?aggregate={aggregate}&limit=100&currency=usd&token=base"
        );
        let response = self
            .http
            .get(url)
            .header("accept", "application/json;version=20230203")
            .send()
            .await
            .map_err(|_| anyhow!("Chart-history request failed or timed out"))?;
        if !response.status().is_success() {
            return Err(anyhow!(
                "Chart-history provider returned HTTP {}",
                response.status().as_u16()
            ));
        }

        let payload: Value = response
            .json()
            .await
            .map_err(|_| anyhow!("Chart-history provider returned invalid JSON"))?;
        let rows = payload
            .pointer("/data/attributes/ohlcv_list")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("Chart-history response has no OHLCV list"))?;
        let mut points = rows
            .iter()
            .filter_map(parse_chart_point)
            .collect::<Vec<_>>();
        points.sort_by_key(|point| point.timestamp_ms);

        if resample_30m {
            points = aggregate_30m(points);
        }
        if let Ok(mut cache) = self.chart_cache.lock() {
            cache.insert(
                cache_key,
                (Instant::now() + Duration::from_secs(60), points.clone()),
            );
        }
        Ok(points)
    }
}

fn chart_period(timeframe: &str) -> Option<(&'static str, u8, bool)> {
    match timeframe {
        "1M" => Some(("minute", 1, false)),
        "5M" => Some(("minute", 5, false)),
        "15M" => Some(("minute", 15, false)),
        "30M" => Some(("minute", 15, true)),
        "1H" => Some(("hour", 1, false)),
        "24H" => Some(("day", 1, false)),
        _ => None,
    }
}

fn parse_chart_point(row: &Value) -> Option<ChartPoint> {
    let values = row.as_array()?;
    if values.len() < 6 {
        return None;
    }
    let timestamp_seconds = values[0]
        .as_i64()
        .or_else(|| values[0].as_u64().and_then(|value| i64::try_from(value).ok()))?;
    Some(ChartPoint {
        timestamp_ms: timestamp_seconds.saturating_mul(1000),
        open: parse_number(values.get(1))?,
        high: parse_number(values.get(2))?,
        low: parse_number(values.get(3))?,
        close: parse_number(values.get(4))?,
        volume: parse_number(values.get(5))?,
    })
}

fn aggregate_30m(points: Vec<ChartPoint>) -> Vec<ChartPoint> {
    const BUCKET_MS: i64 = 30 * 60 * 1000;
    let mut buckets: BTreeMap<i64, ChartPoint> = BTreeMap::new();

    for point in points {
        let bucket = point.timestamp_ms.div_euclid(BUCKET_MS) * BUCKET_MS;
        match buckets.get_mut(&bucket) {
            Some(current) => {
                current.high = current.high.max(point.high);
                current.low = current.low.min(point.low);
                current.close = point.close;
                current.volume += point.volume;
            }
            None => {
                buckets.insert(
                    bucket,
                    ChartPoint {
                        timestamp_ms: bucket,
                        ..point
                    },
                );
            }
        }
    }

    buckets.into_values().collect()
}

fn parse_number(value: Option<&Value>) -> Option<f64> {
    value.and_then(|value| {
        value
            .as_f64()
            .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
    })
}

fn transaction_count(pair: &Value) -> Option<u64> {
    let buys = pair.pointer("/txns/h24/buys").and_then(Value::as_u64)?;
    let sells = pair.pointer("/txns/h24/sells").and_then(Value::as_u64)?;
    Some(buys.saturating_add(sells))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_numeric_strings_and_json_numbers() {
        assert_eq!(
            parse_number(Some(&Value::String("12.5".to_owned()))),
            Some(12.5)
        );
        assert_eq!(parse_number(Some(&json!(7.25))), Some(7.25));
        assert_eq!(parse_number(Some(&Value::Null)), None);
    }

    #[test]
    fn sums_verified_buy_and_sell_counts() {
        let pair = json!({
            "txns": {
                "h24": {
                    "buys": 12,
                    "sells": 8
                }
            }
        });
        assert_eq!(transaction_count(&pair), Some(20));
    }

    #[test]
    fn missing_transaction_counts_stay_unavailable() {
        let pair = json!({ "txns": { "h24": { "buys": 12 } } });
        assert_eq!(transaction_count(&pair), None);
    }

    #[test]
    fn maps_supported_chart_timeframes_without_fabricating_second_candles() {
        assert_eq!(chart_period("1M"), Some(("minute", 1, false)));
        assert_eq!(chart_period("30M"), Some(("minute", 15, true)));
        assert_eq!(chart_period("1S"), None);
    }

    #[test]
    fn parses_geckoterminal_ohlcv_rows() {
        let row = json!([1_700_000_000, "1.0", "2.0", "0.5", "1.5", "1000"]);
        let point = parse_chart_point(&row).unwrap();
        assert_eq!(point.timestamp_ms, 1_700_000_000_000);
        assert_eq!(point.close, 1.5);
        assert_eq!(point.volume, 1000.0);
    }

    #[test]
    fn aggregates_fifteen_minute_points_into_thirty_minute_candles() {
        let points = vec![
            ChartPoint {
                timestamp_ms: 1_699_999_200_000,
                open: 1.0,
                high: 1.5,
                low: 0.8,
                close: 1.2,
                volume: 10.0,
            },
            ChartPoint {
                timestamp_ms: 1_700_000_100_000,
                open: 1.2,
                high: 1.8,
                low: 1.1,
                close: 1.7,
                volume: 20.0,
            },
        ];
        let aggregated = aggregate_30m(points);
        assert_eq!(aggregated.len(), 1);
        assert_eq!(aggregated[0].open, 1.0);
        assert_eq!(aggregated[0].high, 1.8);
        assert_eq!(aggregated[0].low, 0.8);
        assert_eq!(aggregated[0].close, 1.7);
        assert_eq!(aggregated[0].volume, 30.0);
    }

    #[tokio::test]
    async fn rejects_more_than_thirty_addresses_before_network_access() {
        let client = MarketDataClient::new().unwrap();
        let mints = vec!["mint".to_owned(); 31];
        assert!(client.fetch_quotes(&mints).await.is_err());
    }
}
