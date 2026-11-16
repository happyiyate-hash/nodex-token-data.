use std::collections::{BTreeMap, HashMap};

use crate::models::{ChartPoint, Trade};

const TIMEFRAMES: [(&str, i64); 9] = [
    ("1S", 1_000),
    ("5S", 5_000),
    ("10S", 10_000),
    ("1M", 60_000),
    ("5M", 300_000),
    ("15M", 900_000),
    ("30M", 1_800_000),
    ("1H", 3_600_000),
    ("24H", 86_400_000),
];

#[derive(Clone, Debug)]
struct CandleState {
    open_timestamp_ms: i64,
    close_timestamp_ms: i64,
    point: ChartPoint,
}

#[derive(Default)]
struct TokenCandles {
    last_price: Option<f64>,
    total_supply: Option<f64>,
    candles: BTreeMap<(String, i64), CandleState>,
}

#[derive(Default)]
pub struct MarketEngine {
    tokens: HashMap<String, TokenCandles>,
}

impl MarketEngine {
    /// Seed the current candle from the durable store before applying live events.
    pub fn seed_candle(&mut self, mint: &str, timeframe: &str, point: ChartPoint) {
        let Some((_, interval_ms)) = TIMEFRAMES.iter().find(|(name, _)| *name == timeframe) else {
            return;
        };
        let key_time = point.timestamp_ms.div_euclid(*interval_ms) * *interval_ms;
        let state = self.tokens.entry(mint.to_owned()).or_default();
        if timeframe == "1M" {
            state.last_price = Some(point.close);
        }
        state
            .candles
            .entry((timeframe.to_owned(), key_time))
            .or_insert(CandleState {
                open_timestamp_ms: key_time,
                close_timestamp_ms: key_time,
                point,
            });
    }

    /// Update every supported timeframe in memory from one confirmed swap.
    /// Returns the changed candle for each timeframe for immediate WS broadcast.
    pub fn apply_trade(&mut self, mint: &str, trade: &Trade) -> Vec<(String, ChartPoint)> {
        let Some(price) = trade
            .price_usd
            .filter(|value| value.is_finite() && *value > 0.0)
        else {
            return Vec::new();
        };
        let volume = trade
            .volume_usd
            .filter(|value| value.is_finite() && *value >= 0.0)
            .unwrap_or(0.0);
        let state = self.tokens.entry(mint.to_owned()).or_default();
        state.last_price = Some(price);
        let mut changed = Vec::with_capacity(TIMEFRAMES.len());

        for (timeframe, interval_ms) in TIMEFRAMES {
            let bucket = trade.timestamp_ms.div_euclid(interval_ms) * interval_ms;
            let key = (timeframe.to_owned(), bucket);
            let candle = state
                .candles
                .entry(key.clone())
                .or_insert_with(|| CandleState {
                    open_timestamp_ms: trade.timestamp_ms,
                    close_timestamp_ms: trade.timestamp_ms,
                    point: ChartPoint {
                        timestamp_ms: bucket,
                        open: price,
                        high: price,
                        low: price,
                        close: price,
                        volume: 0.0,
                    },
                });

            if trade.timestamp_ms < candle.open_timestamp_ms {
                candle.point.open = price;
                candle.open_timestamp_ms = trade.timestamp_ms;
            }
            candle.point.high = candle.point.high.max(price);
            candle.point.low = candle.point.low.min(price);
            if trade.timestamp_ms >= candle.close_timestamp_ms {
                candle.point.close = price;
                candle.close_timestamp_ms = trade.timestamp_ms;
            }
            candle.point.volume += volume;
            changed.push((timeframe.to_owned(), candle.point.clone()));
        }

        // Keep a bounded rolling window per token; older candles remain in SQLite.
        while state.candles.len() > TIMEFRAMES.len() * 120 {
            if let Some(first) = state.candles.keys().next().cloned() {
                state.candles.remove(&first);
            } else {
                break;
            }
        }
        changed
    }

    pub fn set_total_supply(&mut self, mint: &str, total_supply: f64) {
        if total_supply.is_finite() && total_supply > 0.0 {
            self.tokens.entry(mint.to_owned()).or_default().total_supply = Some(total_supply);
        }
    }

    pub fn market_cap_usd(&self, mint: &str) -> Option<f64> {
        let state = self.tokens.get(mint)?;
        let market_cap = state.last_price? * state.total_supply?;
        (market_cap.is_finite() && market_cap > 0.0).then_some(market_cap)
    }

    pub fn latest_price(&self, mint: &str) -> Option<f64> {
        self.tokens.get(mint).and_then(|state| state.last_price)
    }

    pub fn remove_token(&mut self, mint: &str) {
        self.tokens.remove(mint);
    }
}

#[cfg(test)]
mod tests {
    use super::MarketEngine;
    use crate::models::Trade;

    fn trade(signature: &str, timestamp_ms: i64, price: f64, volume: f64) -> Trade {
        Trade {
            signature: signature.to_owned(),
            event_index: 0,
            side: "buy".to_owned(),
            trader: None,
            amount_sol: None,
            token_amount: Some(10.0),
            price_usd: Some(price),
            volume_usd: Some(volume),
            timestamp_ms,
        }
    }

    #[test]
    fn aggregates_live_candles_for_every_timeframe() {
        let mut engine = MarketEngine::default();
        let first = engine.apply_trade("mint-a", &trade("a", 1_001, 2.0, 5.0));
        let second = engine.apply_trade("mint-a", &trade("b", 1_500, 3.0, 7.0));
        assert_eq!(first.len(), 9);
        assert_eq!(second.len(), 9);
        let one_second = second.iter().find(|(tf, _)| tf == "1S").unwrap();
        assert_eq!(one_second.1.open, 2.0);
        assert_eq!(one_second.1.high, 3.0);
        assert_eq!(one_second.1.low, 2.0);
        assert_eq!(one_second.1.close, 3.0);
        assert_eq!(one_second.1.volume, 12.0);
        assert_eq!(engine.latest_price("mint-a"), Some(3.0));
    }

    #[test]
    fn market_cap_uses_verified_token_supply_and_latest_onchain_price() {
        let mut engine = MarketEngine::default();
        engine.set_total_supply("mint-a", 1_000_000_000.0);
        engine.apply_trade("mint-a", &trade("a", 1_001, 0.000042, 10.0));
        assert_eq!(engine.market_cap_usd("mint-a"), Some(42_000.0));
        assert_eq!(engine.market_cap_usd("unknown"), None);
    }

    #[test]
    fn out_of_order_trades_keep_open_and_close_chronological() {
        let mut engine = MarketEngine::default();
        engine.apply_trade("mint-a", &trade("later", 1_800, 3.0, 2.0));
        let points = engine.apply_trade("mint-a", &trade("earlier", 1_200, 2.0, 1.0));
        let candle = &points.iter().find(|(tf, _)| tf == "1S").unwrap().1;
        assert_eq!(candle.open, 2.0);
        assert_eq!(candle.close, 3.0);
        assert_eq!(candle.high, 3.0);
        assert_eq!(candle.low, 2.0);
        assert_eq!(candle.volume, 3.0);
    }
}
