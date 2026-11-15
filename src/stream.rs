use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize)]
pub struct StreamEvent {
    pub event: String,
    pub mint: String,
    pub topic: String,
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub data: Value,
}

#[derive(Debug, Deserialize)]
pub struct ClientCommand {
    pub command: String,
    pub mint: Option<String>,
    #[serde(default)]
    pub topics: Vec<String>,
    pub timeframe: Option<String>,
}

pub const TOPICS: &[&str] = &[
    "token.market",
    "token.chart",
    "token.trades",
    "token.activity",
    "token.holders",
    "token.info",
    "token.creator",
];
