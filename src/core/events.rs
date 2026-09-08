use serde::Deserialize;

use chrono::{
    DateTime,
    Utc,
};

use crate::market::timeframe::Timeframe;

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct AppEventEnvelope {
    pub symbol: String,
    pub event: AppEvent,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub enum AppEvent {
    // Emitted when a market trade is received.
    Trade(Trade),

    // Emitted when a bid/ask quote update is received.
    // Represents the current bid and ask state of the market.
    // A quote contains the highest price buyers are willing to pay (bid)
    // and the lowest price sellers are willing to accept (ask), along with
    // the available size at each price. Quotes are useful for measuring
    // spread, liquidity, and estimating the current executable market price.
    Quote(Quote),
    
    // Emitted only after a candle has fully completed.
    BarClosed(Bar),
}

#[allow(dead_code)]
#[derive(Clone, Debug, Deserialize)]
pub struct Trade {
    #[serde(rename = "S")]
    pub symbol: String,

    #[serde(rename = "p")]
    pub price: f64,

    #[serde(rename = "s")]
    pub size: u64,

    #[serde(rename = "t")]
    pub timestamp: String,
}

#[allow(dead_code)]
#[derive(Clone, Debug, Deserialize)]
pub struct Quote {
    #[serde(rename = "S")]
    pub symbol: String,

    #[serde(rename = "bp")]
    pub bid_price: f64,

    #[serde(rename = "ap")]
    pub ask_price: f64,

    #[serde(rename = "bs")]
    pub bid_size: u64,

    #[serde(rename = "as")]
    pub ask_size: u64,

    #[serde(rename = "t")]
    pub timestamp: String,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct Bar {
    pub symbol: String,

    // The candle timeframe that produced this bar.
    pub timeframe: Timeframe,

    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,

    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,

    pub volume: u64,
    pub trade_count: u64,

    // Exact sum of price * trade size for every raw trade
    // inside this candle. Used to calculate exact VWAP.
    pub price_volume_sum: f64,
}