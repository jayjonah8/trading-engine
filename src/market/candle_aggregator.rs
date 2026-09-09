// This component should not store historical candle history.
// It only owns candles that are currently being constructed.

use std::{
    collections::HashMap,
    time::Duration,
};

use chrono::{
    DateTime,
    Utc,
};

use crate::{
    core::events::Bar,
    market::timeframe::Timeframe,
};

/// Internal candle currently being constructed.
///
/// Volume uses `f64` because trade quantities may be fractional across
/// venues such as Hyperliquid and fractional-equity brokers.
#[derive(Debug)]
struct ActiveCandle {
    symbol: String,

    start: DateTime<Utc>,
    end: DateTime<Utc>,

    open: f64,
    high: f64,
    low: f64,
    close: f64,

    volume: f64,
    trade_count: u64,

    /// Running sum of:
    ///
    ///     trade_price * trade_size
    ///
    /// Used to calculate exact VWAP from the raw trades.
    price_volume_sum: f64,
}

/// Builds candles for one timeframe from raw tape trades.
///
/// Each symbol has its own independently active candle, allowing one
/// aggregator to process many symbols simultaneously.
pub struct CandleAggregator {
    timeframe: Timeframe,
    active: HashMap<String, ActiveCandle>,
}

impl CandleAggregator {
    pub fn new(timeframe: Timeframe) -> Self {
        assert!(
            !timeframe.duration().is_zero(),
            "candle timeframe cannot be zero"
        );

        Self {
            timeframe,
            active: HashMap::new(),
        }
    }

    pub fn timeframe(&self) -> Timeframe {
        self.timeframe
    }

    /// Adds one raw tape trade to the appropriate active candle.
    ///
    /// `size` is `f64` because market quantities may be fractional.
    pub fn on_trade(
        &mut self,
        symbol: &str,
        timestamp: DateTime<Utc>,
        price: f64,
        size: f64,
    ) {
        // Reject malformed market data before allowing it into candle state.
        if !price.is_finite() || price <= 0.0 || !size.is_finite() || size <= 0.0 {
            return;
        }

        let candle_start = bucket_start(timestamp, self.timeframe.duration());
        let candle_end = candle_start + chrono_duration(self.timeframe.duration());

        if let Some(candle) = self.active.get_mut(symbol) {
            // Ignore stale/out-of-order trades for now.
            // This can later become a configurable market-data policy.
            if timestamp < candle.start {
                return;
            }

            if candle_start == candle.start {
                candle.high = candle.high.max(price);
                candle.low = candle.low.min(price);
                candle.close = price;

                candle.volume += size;
                candle.trade_count = candle.trade_count.saturating_add(1);
                candle.price_volume_sum += price * size;

                return;
            }
        }

        // No active candle exists for this symbol/time bucket, or the
        // previous candle belongs to an older bucket.
        self.active.insert(
            symbol.to_string(),
            ActiveCandle {
                symbol: symbol.to_string(),
                start: candle_start,
                end: candle_end,

                open: price,
                high: price,
                low: price,
                close: price,

                volume: size,
                trade_count: 1,

                // First raw-trade contribution to exact VWAP.
                price_volume_sum: price * size,
            },
        );
    }

    /// Closes every active candle whose end time has passed.
    ///
    /// Candle closing is intentionally independent of another trade arriving
    /// for the same symbol.
    pub fn close_through(&mut self, now: DateTime<Utc>) -> Vec<Bar> {
        let symbols_to_close: Vec<String> = self.active
            .iter()
            .filter_map(|(symbol, candle)| {
                if candle.end <= now {
                    Some(symbol.clone())
                } else {
                    None
                }
            })
            .collect();

        let mut closed = Vec::with_capacity(symbols_to_close.len());

        for symbol in symbols_to_close {
            if let Some(candle) = self.active.remove(&symbol) {
                closed.push(
                    Bar {
                        symbol: candle.symbol,
                        timeframe: self.timeframe,

                        start: candle.start,
                        end: candle.end,

                        open: candle.open,
                        high: candle.high,
                        low: candle.low,
                        close: candle.close,

                        volume: candle.volume,
                        trade_count: candle.trade_count,

                        // Exact accumulated price * quantity from raw trades.
                        price_volume_sum: candle.price_volume_sum,
                    }
                );
            }
        }

        // Keep output deterministic when multiple symbols close together.
        closed.sort_by_key(|bar| bar.end);

        closed
    }
}

/// Converts a standard Rust Duration into chrono's Duration type.
fn chrono_duration(duration: Duration) -> chrono::Duration {
    chrono::Duration::nanoseconds(duration.as_nanos() as i64)
}

/// Returns the exact candle bucket containing `timestamp`.
///
/// Example with a 5-minute timeframe:
///
///     10:07:23 -> 10:05:00
///     10:09:59 -> 10:05:00
///     10:10:00 -> 10:10:00
fn bucket_start(
    timestamp: DateTime<Utc>,
    timeframe: Duration,
) -> DateTime<Utc> {
    let timestamp_ns =
        (timestamp.timestamp() as i128 * 1_000_000_000)
        + timestamp.timestamp_subsec_nanos() as i128;

    let timeframe_ns = timeframe.as_nanos() as i128;
    let bucket_ns = timestamp_ns.div_euclid(timeframe_ns) * timeframe_ns;

    let seconds = bucket_ns.div_euclid(1_000_000_000);
    let nanos = bucket_ns.rem_euclid(1_000_000_000);

    DateTime::<Utc>::from_timestamp(seconds as i64, nanos as u32)
        .expect("valid candle bucket timestamp")
}