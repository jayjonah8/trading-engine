#![allow(dead_code)]

use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub struct AlpacaBarsResponse {
    pub bars: HashMap<String, Vec<AlpacaBar>>,
    pub next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct AlpacaTradesResponse {
    pub trades: HashMap<String, Vec<AlpacaTrade>>,
    pub next_page_token: Option<String>,
}

#[derive(Debug, Clone)]
pub struct HistoricalBar {
    pub symbol: String,
    pub timestamp: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: u64,
}

#[derive(Debug, Clone)]
pub struct HistoricalTrade {
    pub symbol: String,
    pub timestamp: DateTime<Utc>,
    pub price: f64,
    pub size: u64,
}

#[derive(Debug, Deserialize)]
pub struct AlpacaBar {
    #[serde(rename = "t")]
    pub timestamp: DateTime<Utc>,

    #[serde(rename = "o")]
    pub open: f64,

    #[serde(rename = "h")]
    pub high: f64,

    #[serde(rename = "l")]
    pub low: f64,

    #[serde(rename = "c")]
    pub close: f64,

    #[serde(rename = "v")]
    pub volume: u64,
}

#[derive(Debug, Deserialize)]
pub struct AlpacaTrade {
    #[serde(rename = "t")]
    pub timestamp: DateTime<Utc>,

    #[serde(rename = "p")]
    pub price: f64,

    #[serde(rename = "s")]
    pub size: u64,
}