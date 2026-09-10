#![allow(dead_code)]

use chrono::{
    DateTime,
    Utc,
};

#[derive(Debug, Clone)]
pub struct HistoricalBar {
    pub symbol: String,
    pub timestamp: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Debug, Clone)]
pub struct HistoricalTrade {
    pub symbol: String,
    pub timestamp: DateTime<Utc>,
    pub price: f64,
    pub size: f64,
}