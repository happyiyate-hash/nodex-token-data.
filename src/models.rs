use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub struct TokenSnapshot {
    pub schema_version: u32,
    pub mint: String,
    pub as_of: String,
    pub identity: TokenIdentity,
    pub market: MarketSnapshot,
    pub chart: ChartSnapshot,
    pub trades: TradesSnapshot,
    pub holders: HoldersSnapshot,
    pub info: TokenInfo,
    pub data_status: DataStatus,
}

#[derive(Clone, Debug, Serialize)]
pub struct TokenIdentity {
    pub name: Option<String>,
    pub symbol: Option<String>,
    pub logo_url: Option<String>,
    pub mint_address: String,
    pub creator_address: Option<String>,
    pub created_at: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct MarketSnapshot {
    pub price_usd: Option<f64>,
    pub price_sol: Option<f64>,
    pub market_cap_usd: Option<f64>,
    pub liquidity_usd: Option<f64>,
    pub change_1m_pct: Option<f64>,
    pub change_5m_pct: Option<f64>,
    pub change_24h_pct: Option<f64>,
    pub volume_24h_usd: Option<f64>,
    pub transactions_24h: Option<u64>,
    pub buy_volume_usd: Option<f64>,
    pub sell_volume_usd: Option<f64>,
    pub buy_pressure_pct: Option<f64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ChartSnapshot {
    pub timeframe: String,
    pub points: Vec<ChartPoint>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ChartPoint {
    pub timestamp_ms: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct TradesSnapshot {
    pub recent: Vec<Trade>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Trade {
    pub signature: String,
    pub event_index: u32,
    pub side: String,
    pub trader: Option<String>,
    pub amount_sol: Option<f64>,
    pub token_amount: Option<f64>,
    pub price_usd: Option<f64>,
    pub volume_usd: Option<f64>,
    pub timestamp_ms: i64,
}

#[derive(Clone, Debug, Serialize)]
pub struct HoldersSnapshot {
    pub total_count: Option<u64>,
    pub top_10_ownership_pct: Option<f64>,
    pub largest: Vec<Holder>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Holder {
    pub address: String,
    pub token_account: Option<String>,
    pub token_amount: f64,
    pub supply_pct: Option<f64>,
    pub rank: u32,
}

#[derive(Clone, Debug, Serialize)]
pub struct TokenInfo {
    pub decimals: Option<u8>,
    pub total_supply: Option<String>,
    pub circulating_supply: Option<String>,
    pub token_standard: Option<String>,
    pub creator_address: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct DataStatus {
    pub identity: &'static str,
    pub market: &'static str,
    pub chart: &'static str,
    pub trades: &'static str,
    pub holders: &'static str,
    pub info: &'static str,
}

impl TokenSnapshot {
    /// Missing provider data stays null/empty; no simulated market values are generated.
    pub fn unavailable(mint: String, timeframe: String) -> Self {
        Self {
            schema_version: 1,
            identity: TokenIdentity {
                name: None,
                symbol: None,
                logo_url: None,
                mint_address: mint.clone(),
                creator_address: None,
                created_at: None,
            },
            market: MarketSnapshot {
                price_usd: None,
                price_sol: None,
                market_cap_usd: None,
                liquidity_usd: None,
                change_1m_pct: None,
                change_5m_pct: None,
                change_24h_pct: None,
                volume_24h_usd: None,
                transactions_24h: None,
                buy_volume_usd: None,
                sell_volume_usd: None,
                buy_pressure_pct: None,
            },
            chart: ChartSnapshot {
                timeframe,
                points: Vec::new(),
            },
            trades: TradesSnapshot { recent: Vec::new() },
            holders: HoldersSnapshot {
                total_count: None,
                top_10_ownership_pct: None,
                largest: Vec::new(),
            },
            info: TokenInfo {
                decimals: None,
                total_supply: None,
                circulating_supply: None,
                token_standard: None,
                creator_address: None,
            },
            data_status: DataStatus {
                identity: "unavailable",
                market: "unavailable",
                chart: "unavailable",
                trades: "unavailable",
                holders: "unavailable",
                info: "unavailable",
            },
            as_of: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            mint,
        }
    }
}
