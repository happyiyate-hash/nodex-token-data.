use std::{path::Path, str::FromStr, time::Duration};

use anyhow::{Context, Result};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    SqlitePool,
};

use crate::models::{ChartPoint, Trade};

#[derive(Clone)]
pub struct MarketStore {
    pool: SqlitePool,
}

impl MarketStore {
    pub async fn connect(database_url: &str) -> Result<Self> {
        if let Some(path) = database_url.strip_prefix("sqlite://") {
            if path != ":memory:" {
                if let Some(parent) = Path::new(path).parent() {
                    if !parent.as_os_str().is_empty() {
                        tokio::fs::create_dir_all(parent)
                            .await
                            .with_context(|| format!("Could not create database directory {}", parent.display()))?;
                    }
                }
            }
        }

        let options = SqliteConnectOptions::from_str(database_url)?
            .create_if_missing(true)
            .busy_timeout(Duration::from_secs(5))
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .context("Could not open NodeX market-data database")?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS market_trades (
                mint TEXT NOT NULL,
                signature TEXT NOT NULL,
                side TEXT NOT NULL,
                trader TEXT,
                token_amount REAL,
                price_usd REAL,
                volume_usd REAL,
                timestamp_ms INTEGER NOT NULL,
                source TEXT NOT NULL,
                PRIMARY KEY (mint, signature)
            )",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_market_trades_mint_time
             ON market_trades(mint, timestamp_ms DESC)",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS market_candles (
                mint TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                timestamp_ms INTEGER NOT NULL,
                open_timestamp_ms INTEGER NOT NULL,
                close_timestamp_ms INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                source TEXT NOT NULL,
                PRIMARY KEY (mint, timeframe, timestamp_ms)
            )",
        )
        .execute(&pool)
        .await?;

        Ok(Self { pool })
    }

    /// Seed historical candles without replacing candles already built from
    /// locally observed swaps. This makes repeated history backfills safe.
    pub async fn seed_historical_candles(
        &self,
        mint: &str,
        timeframe: &str,
        points: &[ChartPoint],
    ) -> Result<()> {
        let mut transaction = self.pool.begin().await?;
        for point in points {
            sqlx::query(
                "INSERT INTO market_candles
                 (mint, timeframe, timestamp_ms, open_timestamp_ms, close_timestamp_ms, open, high, low, close, volume, source)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'historical_provider')
                 ON CONFLICT(mint, timeframe, timestamp_ms) DO NOTHING",
            )
            .bind(mint)
            .bind(timeframe)
            .bind(point.timestamp_ms)
            .bind(point.timestamp_ms)
            .bind(point.timestamp_ms)
            .bind(point.open)
            .bind(point.high)
            .bind(point.low)
            .bind(point.close)
            .bind(point.volume)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    /// Stores a confirmed swap once and updates locally derived candles.
    /// Duplicate signatures are ignored, preventing replay double-counting.
    pub async fn record_confirmed_trade(&self, mint: &str, trade: &Trade) -> Result<bool> {
        let mut transaction = self.pool.begin().await?;
        let inserted = sqlx::query(
            "INSERT INTO market_trades
             (mint, signature, side, trader, token_amount, price_usd, volume_usd, timestamp_ms, source)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'onchain')
             ON CONFLICT(mint, signature) DO NOTHING",
        )
        .bind(mint)
        .bind(&trade.signature)
        .bind(&trade.side)
        .bind(&trade.trader)
        .bind(trade.token_amount)
        .bind(trade.price_usd)
        .bind(trade.volume_usd)
        .bind(trade.timestamp_ms)
        .execute(&mut *transaction)
        .await?
        .rows_affected() == 1;

        if inserted {
            if let (Some(price), Some(volume)) = (trade.price_usd, trade.volume_usd) {
                if price.is_finite() && price > 0.0 && volume.is_finite() && volume >= 0.0 {
                    for (timeframe, interval_ms) in candle_intervals() {
                        let bucket = trade.timestamp_ms.div_euclid(interval_ms) * interval_ms;
                        sqlx::query(
                            "INSERT INTO market_candles
                             (mint, timeframe, timestamp_ms, open_timestamp_ms, close_timestamp_ms, open, high, low, close, volume, source)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'onchain')
                             ON CONFLICT(mint, timeframe, timestamp_ms) DO UPDATE SET
                               open = CASE WHEN excluded.open_timestamp_ms < market_candles.open_timestamp_ms THEN excluded.open ELSE market_candles.open END,
                               open_timestamp_ms = MIN(market_candles.open_timestamp_ms, excluded.open_timestamp_ms),
                               high = MAX(market_candles.high, excluded.high),
                               low = MIN(market_candles.low, excluded.low),
                               close = CASE WHEN excluded.close_timestamp_ms >= market_candles.close_timestamp_ms THEN excluded.close ELSE market_candles.close END,
                               close_timestamp_ms = MAX(market_candles.close_timestamp_ms, excluded.close_timestamp_ms),
                               volume = market_candles.volume + excluded.volume,
                               source = 'onchain'",
                        )
                        .bind(mint)
                        .bind(timeframe)
                        .bind(bucket)
                        .bind(trade.timestamp_ms)
                        .bind(trade.timestamp_ms)
                        .bind(price)
                        .bind(price)
                        .bind(price)
                        .bind(price)
                        .bind(volume)
                        .execute(&mut *transaction)
                        .await?;
                    }
                }
            }
        }

        transaction.commit().await?;
        Ok(inserted)
    }

    pub async fn load_candles(
        &self,
        mint: &str,
        timeframe: &str,
        limit: i64,
    ) -> Result<Vec<ChartPoint>> {
        let rows = sqlx::query_as::<_, (i64, f64, f64, f64, f64, f64)>(
            "SELECT timestamp_ms, open, high, low, close, volume
             FROM market_candles
             WHERE mint = ? AND timeframe = ?
             ORDER BY timestamp_ms DESC
             LIMIT ?",
        )
        .bind(mint)
        .bind(timeframe)
        .bind(limit.clamp(1, 5000))
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .rev()
            .map(|(timestamp_ms, open, high, low, close, volume)| ChartPoint {
                timestamp_ms,
                open,
                high,
                low,
                close,
                volume,
            })
            .collect())
    }
}

fn candle_intervals() -> [(&'static str, i64); 6] {
    [
        ("1M", 60_000),
        ("5M", 300_000),
        ("15M", 900_000),
        ("30M", 1_800_000),
        ("1H", 3_600_000),
        ("24H", 86_400_000),
    ]
}

#[cfg(test)]
mod tests {
    use super::{candle_intervals, MarketStore};
    use crate::models::Trade;

    #[tokio::test]
    async fn confirmed_trades_are_deduplicated_and_aggregate_candles() {
        let store = MarketStore::connect("sqlite::memory:").await.unwrap();
        let first = Trade {
            signature: "sig-1".to_owned(),
            side: "buy".to_owned(),
            trader: Some("wallet-a".to_owned()),
            amount_sol: None,
            token_amount: Some(10.0),
            price_usd: Some(2.0),
            volume_usd: Some(20.0),
            timestamp_ms: 120_000,
        };
        let second = Trade {
            signature: "sig-2".to_owned(),
            side: "sell".to_owned(),
            trader: Some("wallet-b".to_owned()),
            amount_sol: None,
            token_amount: Some(5.0),
            price_usd: Some(1.5),
            volume_usd: Some(7.5),
            timestamp_ms: 150_000,
        };

        assert!(store.record_confirmed_trade("mint-a", &second).await.unwrap());
        assert!(store.record_confirmed_trade("mint-a", &first).await.unwrap());
        assert!(!store.record_confirmed_trade("mint-a", &first).await.unwrap());

        let candles = store.load_candles("mint-a", "1M", 10).await.unwrap();
        assert_eq!(candles.len(), 1);
        assert_eq!(candles[0].timestamp_ms, 120_000);
        assert_eq!(candles[0].open, 2.0);
        assert_eq!(candles[0].high, 2.0);
        assert_eq!(candles[0].low, 1.5);
        assert_eq!(candles[0].close, 1.5);
        assert_eq!(candles[0].volume, 27.5);
    }

    #[test]
    fn candle_intervals_are_consistent() {
        let intervals = candle_intervals();
        assert_eq!(intervals[0], ("1M", 60_000));
        assert_eq!(intervals[5], ("24H", 86_400_000));
        for (_, interval) in intervals {
            assert!(interval > 0);
        }
    }
}
