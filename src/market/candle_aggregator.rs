// This component should not store your 24 hours of historical candles.
// It only knows about candles currently being constructed.

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

#[derive(Debug)]
struct ActiveCandle {
    symbol: String,

    start: DateTime<Utc>,
    end: DateTime<Utc>,

    open: f64,
    high: f64,
    low: f64,
    close: f64,

    volume: u64,
    trade_count: u64,

    // Running sum of price * trade size.
    price_volume_sum: f64,
}

pub struct CandleAggregator {
    timeframe: Timeframe,

    active:
        HashMap<String, ActiveCandle>,
}

impl CandleAggregator {
    pub fn new(
        timeframe: Timeframe,
    ) -> Self {
        assert!(
            !timeframe.duration().is_zero(),
            "candle timeframe cannot be zero"
        );

        Self {
            timeframe,
            active: HashMap::new(),
        }
    }

    pub fn timeframe(
        &self,
    ) -> Timeframe {
        self.timeframe
    }

    pub fn on_trade(
        &mut self,
        symbol: &str,
        timestamp: DateTime<Utc>,
        price: f64,
        size: u64,
    ) {
        if !price.is_finite()
            || price <= 0.0
        {
            return;
        }

        let candle_start =
            bucket_start(
                timestamp,
                self.timeframe.duration(),
            );

        let candle_end =
            candle_start
                + chrono_duration(
                    self.timeframe.duration()
                );

        match self.active.get_mut(symbol) {
            Some(candle) => {
                // Ignore stale/out-of-order trades for now.
                //
                // Later this can become a configurable policy.
                if timestamp < candle.start {
                    return;
                }

                if candle_start == candle.start {
                    candle.high =
                        candle.high.max(price);

                    candle.low =
                        candle.low.min(price);

                    candle.close = price;

                    candle.volume =
                        candle.volume
                            .saturating_add(size);

                    candle.trade_count =
                        candle.trade_count
                            .saturating_add(1);

                    candle.price_volume_sum +=
                        price * size as f64;

                    return;
                }
            }

            None => {}
        }

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

                // First trade contribution to exact VWAP.
                price_volume_sum:
                    price * size as f64,
            },
        );
    }

    // Closes every candle whose end time has passed.
    //
    // This is important because we don't want candle closing
    // to depend on another trade arriving for THAT symbol.
    pub fn close_through(
        &mut self,
        now: DateTime<Utc>,
    ) -> Vec<Bar> {
        let symbols_to_close:
            Vec<String> =
            self.active
                .iter()
                .filter_map(
                    |(symbol, candle)| {
                        if candle.end <= now {
                            Some(symbol.clone())
                        } else {
                            None
                        }
                    }
                )
                .collect();

        let mut closed =
            Vec::with_capacity(
                symbols_to_close.len()
            );

        for symbol in symbols_to_close {
            if let Some(candle) =
                self.active.remove(&symbol)
            {
                closed.push(
                    Bar {
                        symbol: candle.symbol,

                        timeframe:
                            self.timeframe,

                        start: candle.start,
                        end: candle.end,

                        open: candle.open,
                        high: candle.high,
                        low: candle.low,
                        close: candle.close,

                        volume: candle.volume,

                        trade_count:
                            candle.trade_count,

                        // Exact accumulated price * volume from raw trades.
                        price_volume_sum:
                            candle.price_volume_sum,
                    }
                );
            }
        }

        closed.sort_by_key(
            |bar| bar.end
        );

        closed
    }
}

fn chrono_duration(
    duration: Duration,
) -> chrono::Duration {
    chrono::Duration::nanoseconds(
        duration.as_nanos() as i64
    )
}

fn bucket_start(
    timestamp: DateTime<Utc>,
    timeframe: Duration,
) -> DateTime<Utc> {
    let timestamp_ns =
        (timestamp.timestamp() as i128
            * 1_000_000_000)
        + timestamp
            .timestamp_subsec_nanos()
            as i128;

    let timeframe_ns =
        timeframe.as_nanos() as i128;

    let bucket_ns =
        timestamp_ns.div_euclid(
            timeframe_ns
        ) * timeframe_ns;

    let seconds =
        bucket_ns.div_euclid(
            1_000_000_000
        );

    let nanos =
        bucket_ns.rem_euclid(
            1_000_000_000
        );

    DateTime::<Utc>::from_timestamp(
        seconds as i64,
        nanos as u32,
    )
    .expect(
        "valid candle bucket timestamp"
    )
}