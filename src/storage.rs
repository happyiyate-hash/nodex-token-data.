use std::{path::Path, str::FromStr, time::Duration};

use anyhow::{Context, Result};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Row, SqlitePool,
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
                        tokio::fs::create_dir_all(parent).await.with_context(|| {
                            format!("Could not create database directory {}", parent.display())
                        })?;
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
                event_index INTEGER NOT NULL DEFAULT 0,
                side TEXT NOT NULL,
                trader TEXT,
                token_amount REAL,
                amount_sol REAL,
                price_usd REAL,
                volume_usd REAL,
                timestamp_ms INTEGER NOT NULL,
                source TEXT NOT NULL,
                PRIMARY KEY (mint, signature, event_index)
            )",
        )
        .execute(&pool)
        .await?;
        let trade_columns = sqlx::query("PRAGMA table_info(market_trades)")
            .fetch_all(&pool)
            .await?;
        let has_amount_sol = trade_columns
            .iter()
            .any(|row| row.try_get::<String, _>("name").ok().as_deref() == Some("amount_sol"));
        if !has_amount_sol {
            sqlx::query("ALTER TABLE market_trades ADD COLUMN amount_sol REAL")
                .execute(&pool)
                .await?;
        }

        let has_event_index = trade_columns
            .iter()
            .any(|row| row.try_get::<String, _>("name").ok().as_deref() == Some("event_index"));
        if !has_event_index {
            sqlx::query(
                "CREATE TABLE market_trades_v2 (
                    mint TEXT NOT NULL,
                    signature TEXT NOT NULL,
                    event_index INTEGER NOT NULL DEFAULT 0,
                    side TEXT NOT NULL,
                    trader TEXT,
                    token_amount REAL,
                    amount_sol REAL,
                    price_usd REAL,
                    volume_usd REAL,
                    timestamp_ms INTEGER NOT NULL,
                    source TEXT NOT NULL,
                    PRIMARY KEY (mint, signature, event_index)
                )",
            )
            .execute(&pool)
            .await?;
            sqlx::query(
                "INSERT INTO market_trades_v2
                 (mint, signature, event_index, side, trader, token_amount, amount_sol, price_usd, volume_usd, timestamp_ms, source)
                 SELECT mint, signature, 0, side, trader, token_amount, amount_sol, price_usd, volume_usd, timestamp_ms, source
                 FROM market_trades",
            )
            .execute(&pool)
            .await?;
            sqlx::query("DROP TABLE market_trades")
                .execute(&pool)
                .await?;
            sqlx::query("ALTER TABLE market_trades_v2 RENAME TO market_trades")
                .execute(&pool)
                .await?;
        }

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

        let candle_columns = sqlx::query("PRAGMA table_info(market_candles)")
            .fetch_all(&pool)
            .await?;
        let has_open_time = candle_columns.iter().any(|row| {
            row.try_get::<String, _>("name").ok().as_deref() == Some("open_timestamp_ms")
        });
        if !has_open_time {
            sqlx::query("ALTER TABLE market_candles ADD COLUMN open_timestamp_ms INTEGER NOT NULL DEFAULT 0")
                .execute(&pool)
                .await?;
            sqlx::query("UPDATE market_candles SET open_timestamp_ms = timestamp_ms WHERE open_timestamp_ms = 0")
                .execute(&pool)
                .await?;
        }
        let has_close_time = candle_columns.iter().any(|row| {
            row.try_get::<String, _>("name").ok().as_deref() == Some("close_timestamp_ms")
        });
        if !has_close_time {
            sqlx::query("ALTER TABLE market_candles ADD COLUMN close_timestamp_ms INTEGER NOT NULL DEFAULT 0")
                .execute(&pool)
                .await?;
            sqlx::query("UPDATE market_candles SET close_timestamp_ms = timestamp_ms WHERE close_timestamp_ms = 0")
                .execute(&pool)
                .await?;
        }

        Ok(Self { pool })
    }

    /// Seed historical candles without replacing candles already built from
    /// locally observed swaps. This makes repeated history backfills safe.
    pub async fn delete_mint(&self, mint: &str) -> Result<()> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query("DELETE FROM market_trades WHERE mint = ?")
            .bind(mint)
            .execute(&mut *transaction)
            .await?;
        sqlx::query("DELETE FROM market_candles WHERE mint = ?")
            .bind(mint)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(())
    }

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
             (mint, signature, event_index, side, trader, token_amount, amount_sol, price_usd, volume_usd, timestamp_ms, source)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'onchain')
             ON CONFLICT(mint, signature, event_index) DO NOTHING",
        )
        .bind(mint)
        .bind(&trade.signature)
        .bind(i64::from(trade.event_index))
        .bind(&trade.side)
        .bind(&trade.trader)
        .bind(trade.token_amount)
        .bind(trade.amount_sol)
        .bind(trade.price_usd)
        .bind(trade.volume_usd)
        .bind(trade.timestamp_ms)
        .execute(&mut *transaction)
        .await?
        .rows_affected() == 1;

        if inserted {
            // Price is enough to form a real OHLC candle. USD volume is optional;
            // when unavailable keep the candle and mark its volume as zero rather
            // than dropping the entire price observation.
            if let Some(price) = trade.price_usd {
                let volume = trade.volume_usd.unwrap_or(0.0);
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

    /// Returns confirmed on-chain buy volume, sell volume, and trade counts
    /// for the trailing 24 hours. NULL volume values remain excluded rather than
    /// being silently treated as zero.
    pub async fn load_trade_totals_24h(
        &self,
        mint: &str,
    ) -> Result<(Option<f64>, Option<f64>, u64, u64)> {
        let cutoff_ms = chrono::Utc::now()
            .timestamp_millis()
            .saturating_sub(24 * 60 * 60 * 1000);
        let row = sqlx::query_as::<_, (Option<f64>, Option<f64>, i64, i64)>(
            "SELECT
                SUM(CASE WHEN lower(side) = 'buy' THEN volume_usd END),
                SUM(CASE WHEN lower(side) = 'sell' THEN volume_usd END),
                COALESCE(SUM(CASE WHEN lower(side) = 'buy' THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN lower(side) = 'sell' THEN 1 ELSE 0 END), 0)
             FROM market_trades WHERE mint = ? AND timestamp_ms >= ?",
        )
        .bind(mint)
        .bind(cutoff_ms)
        .fetch_one(&self.pool)
        .await?;
        Ok((row.0, row.1, row.2.max(0) as u64, row.3.max(0) as u64))
    }

    pub async fn load_recent_trades(&self, mint: &str, limit: i64) -> Result<Vec<Trade>> {
        let rows = sqlx::query_as::<_, (String, i64, String, Option<String>, Option<f64>, Option<f64>, Option<f64>, Option<f64>, i64)>(
            "SELECT signature, event_index, side, trader, token_amount, amount_sol, price_usd, volume_usd, timestamp_ms FROM market_trades WHERE mint = ? ORDER BY timestamp_ms DESC, signature DESC, event_index DESC LIMIT ?"
        ).bind(mint).bind(limit.clamp(1, 200)).fetch_all(&self.pool).await?;
        Ok(rows
            .into_iter()
            .map(
                |(
                    signature,
                    event_index,
                    side,
                    trader,
                    token_amount,
                    amount_sol,
                    price_usd,
                    volume_usd,
                    timestamp_ms,
                )| Trade {
                    signature,
                    event_index: u32::try_from(event_index).unwrap_or_default(),
                    side,
                    trader,
                    token_amount,
                    amount_sol,
                    price_usd,
                    volume_usd,
                    timestamp_ms,
                },
            )
            .collect())
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
            .map(
                |(timestamp_ms, open, high, low, close, volume)| ChartPoint {
                    timestamp_ms,
                    open,
                    high,
                    low,
                    close,
                    volume,
                },
            )
            .collect())
    }
}

fn candle_intervals() -> [(&'static str, i64); 9] {
    [
        ("1S", 1_000),
        ("5S", 5_000),
        ("10S", 10_000),
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
            event_index: 0,
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
            event_index: 0,
            side: "sell".to_owned(),
            trader: Some("wallet-b".to_owned()),
            amount_sol: None,
            token_amount: Some(5.0),
            price_usd: Some(1.5),
            volume_usd: Some(7.5),
            timestamp_ms: 150_000,
        };

        assert!(store
            .record_confirmed_trade("mint-a", &second)
            .await
            .unwrap());
        assert!(store
            .record_confirmed_trade("mint-a", &first)
            .await
            .unwrap());
        assert!(!store
            .record_confirmed_trade("mint-a", &first)
            .await
            .unwrap());

        let candles = store.load_candles("mint-a", "1M", 10).await.unwrap();
        assert_eq!(candles.len(), 1);
        assert_eq!(candles[0].timestamp_ms, 120_000);
        assert_eq!(candles[0].open, 2.0);
        assert_eq!(candles[0].high, 2.0);
        assert_eq!(candles[0].low, 1.5);
        assert_eq!(candles[0].close, 1.5);
        assert_eq!(candles[0].volume, 27.5);
    }

    #[tokio::test]
    async fn creates_price_candles_when_usd_volume_is_unavailable() {
        let store = MarketStore::connect("sqlite::memory:").await.unwrap();
        let trade = Trade {
            signature: "price-without-volume".to_owned(),
            event_index: 0,
            side: "buy".to_owned(),
            trader: None,
            amount_sol: None,
            token_amount: Some(25.0),
            price_usd: Some(0.0025),
            volume_usd: None,
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
        };
        assert!(store.record_confirmed_trade("mint-a", &trade).await.unwrap());
        let candles = store.load_candles("mint-a", "1S", 10).await.unwrap();
        assert_eq!(candles.len(), 1);
        assert_eq!(candles[0].close, 0.0025);
        assert_eq!(candles[0].volume, 0.0);
    }

    #[tokio::test]
    async fn stores_multiple_swaps_from_one_transaction_by_event_index() {
        let store = MarketStore::connect("sqlite::memory:").await.unwrap();
        let first = Trade {
            signature: "same-signature".to_owned(),
            event_index: 12,
            side: "buy".to_owned(),
            trader: Some("wallet-a".to_owned()),
            amount_sol: Some(1.0),
            token_amount: Some(10.0),
            price_usd: Some(2.0),
            volume_usd: Some(20.0),
            timestamp_ms: 120_000,
        };
        let second = Trade {
            signature: "same-signature".to_owned(),
            event_index: 28,
            side: "sell".to_owned(),
            trader: Some("wallet-a".to_owned()),
            amount_sol: Some(0.5),
            token_amount: Some(5.0),
            price_usd: Some(1.5),
            volume_usd: Some(7.5),
            timestamp_ms: 121_000,
        };

        assert!(store
            .record_confirmed_trade("mint-a", &first)
            .await
            .unwrap());
        assert!(store
            .record_confirmed_trade("mint-a", &second)
            .await
            .unwrap());
        assert!(!store
            .record_confirmed_trade("mint-a", &first)
            .await
            .unwrap());

        let trades = store.load_recent_trades("mint-a", 10).await.unwrap();
        assert_eq!(trades.len(), 2);
        assert!(trades.iter().any(|trade| trade.event_index == 12));
        assert!(trades.iter().any(|trade| trade.event_index == 28));
    }

    #[test]
    fn candle_intervals_are_consistent() {
        let intervals = candle_intervals();
        assert_eq!(intervals[0], ("1S", 1_000));
        assert_eq!(intervals[8], ("24H", 86_400_000));
        for (_, interval) in intervals {
            assert!(interval > 0);
        }
    }
}
