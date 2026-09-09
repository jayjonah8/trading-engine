#![allow(dead_code)]

use chrono::{
    DateTime,
    Utc,
};

use crate::market::timeframe::Timeframe;

/// Venue-independent market-data envelope.
///
/// Every market-data provider converts its native messages into these
/// canonical events before sending them into the application.
///
/// This keeps strategies and the engine independent from Alpaca,
/// Hyperliquid, IBKR, or any future market-data provider.
#[derive(Clone, Debug)]
pub struct AppEventEnvelope {
    pub symbol: String,
    pub event: AppEvent,
}

/// Canonical market-data events understood by the application.
///
/// These represent market information only. Execution events for our own
/// orders belong to the separate `ExecEvent` system.
#[derive(Clone, Debug)]
pub enum AppEvent {
    /// A public trade printed on the market tape.
    TapeTrade(TapeTrade),

    /// Current best bid and ask.
    Quote(Quote),

    /// A fully completed candle/bar.
    BarClosed(Bar),
}

/// A public trade printed on the market tape.
///
/// `TapeTrade` is intentionally separate from the engine's `Trade`.
///
/// - `TapeTrade` = public market-data execution
/// - `Trade`     = strategy-level trade lifecycle
/// - `Fill`      = execution of one of our own orders
///
/// Venue-specific messages should be converted into this canonical format
/// before entering the application's market-data event bus.
///
/// TODO:
/// Replace primitive price/quantity values with canonical `Price` and
/// `Quantity` types once the strong-type cleanup is completed.
#[derive(Clone, Debug)]
pub struct TapeTrade {
    pub symbol: String,

    /// Price at which the public trade executed.
    pub price: f64,

    /// Quantity executed in the public trade.
    ///
    /// `f64` is temporarily used because quantities may be fractional,
    /// especially for crypto and fractional equities.
    pub size: f64,

    /// Venue-provided event timestamp normalized to Unix milliseconds.
    pub timestamp_ms: i64,
}

/// Canonical best bid/ask market quote.
///
/// This contains the top of the order book rather than an execution.
///
/// Strategies may derive midpoint, spread, liquidity, or other values from
/// this structure without depending on venue-specific market-data formats.
///
/// TODO:
/// Replace primitive price/quantity values with canonical `Price` and
/// `Quantity` types once the strong-type cleanup is completed.
#[derive(Clone, Debug)]
pub struct Quote {
    pub symbol: String,

    /// Highest currently available bid price.
    pub bid_price: f64,

    /// Lowest currently available ask price.
    pub ask_price: f64,

    /// Quantity available at the best bid.
    pub bid_size: f64,

    /// Quantity available at the best ask.
    pub ask_size: f64,

    /// Venue-provided event timestamp normalized to Unix milliseconds.
    pub timestamp_ms: i64,
}

impl Quote {
    /// Midpoint between the current best bid and ask.
    pub fn mid_price(&self) -> f64 {
        (self.bid_price + self.ask_price) / 2.0
    }

    /// Absolute bid/ask spread.
    pub fn spread(&self) -> f64 {
        self.ask_price - self.bid_price
    }

    /// Bid/ask spread as a percentage of the midpoint.
    pub fn spread_percent(&self) -> f64 {
        let mid = self.mid_price();

        if mid <= 0.0 {
            return 0.0;
        }

        (self.spread() / mid) * 100.0
    }
}

/// Canonical completed OHLCV candle/bar.
///
/// Bars are provider-independent and may be produced from live tape data,
/// historical data, or another normalized market-data source.
#[derive(Clone, Debug)]
pub struct Bar {
    pub symbol: String,

    /// Candle timeframe that produced this bar.
    pub timeframe: Timeframe,

    /// Inclusive beginning of the candle interval.
    pub start: DateTime<Utc>,

    /// End of the candle interval.
    pub end: DateTime<Utc>,

    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,

    /// Total quantity traded during the candle.
    pub volume: f64,

    /// Number of public trades included in the candle.
    pub trade_count: u64,

    /// Exact sum of `trade_price * trade_size` for every tape trade
    /// included in the candle.
    ///
    /// VWAP can therefore be calculated as:
    ///
    /// `price_volume_sum / volume`
    pub price_volume_sum: f64,
}