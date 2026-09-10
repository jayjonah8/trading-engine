#![allow(dead_code)]

use std::collections::HashMap;

use chrono::{
    DateTime,
    Utc,
};

use serde::Deserialize;

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
    pub volume: f64,
}

#[derive(Debug, Deserialize)]
pub struct AlpacaTrade {
    #[serde(rename = "t")]
    pub timestamp: DateTime<Utc>,

    #[serde(rename = "p")]
    pub price: f64,

    #[serde(rename = "s")]
    pub size: f64,
}