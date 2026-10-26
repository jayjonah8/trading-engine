#![allow(dead_code)]

use std::collections::{
    HashMap,
    VecDeque,
};

use crate::shared::print_log;


// ============================================================
// PRICE WINDOW
// ============================================================

pub struct PriceWindowTs {
    pub debug_mode: bool,

    // Rolling price history for each symbol.
    window_prices:
        HashMap<String, VecDeque<(i64, f64)>>,

    // Rolling window duration in seconds.
    entry_window_secs: i64,
}


// ============================================================
// CONSTRUCTOR
// ============================================================

impl PriceWindowTs {
    pub fn new(
        entry_window_secs: i64,
        debug_mode: bool,
    ) -> Self {
        Self {
            debug_mode,
            window_prices: HashMap::new(),
            entry_window_secs,
        }
    }


    // ========================================================
    // UPDATE TIMESTAMP-BASED PRICE WINDOW
    // ========================================================

    /// Maintains a rolling price window using Unix timestamps in milliseconds.
    ///
    /// Uses market event timestamps instead of Instant or SystemTime for
    /// time-based calculations, ensuring consistent behavior across live
    /// trading, paper trading, simulation, and historical backtesting.
    ///
    /// Calculates the percentage difference between the current price
    /// and the minimum and maximum prices within the configured window.
    ///
    /// Maintains independent rolling price histories for each symbol.
    ///
    /// Returns:
    /// (pct_from_min, pct_from_max, elapsed_secs)
    pub fn update_price_window_extreme_diffs_ts(
        &mut self,
        symbol: &str,
        timestamp_ms: i64,
        price_now: f64,
    ) -> Option<(f64, f64, u64)> {
        const EPS: f64 = 1e-8;

        // Reject invalid prices.
        if !price_now.is_finite() || price_now <= 0.0 {
            return None;
        }

        let buf = self
            .window_prices
            .entry(symbol.to_string())
            .or_insert_with(VecDeque::new);

        // ====================================================
        // VALIDATE TIMESTAMP ORDER
        // ====================================================

        // Rolling windows require chronological market events.
        if let Some((last_ts, _)) = buf.back() {
            if timestamp_ms < *last_ts {
                return None;
            }
        }

        // ====================================================
        // UPDATE PRICE HISTORY
        // ====================================================

        // Avoid storing identical consecutive prices.
        //
        // Update the timestamp instead of adding another entry.
        if let Some((last_ts, last_price)) = buf.back_mut() {
            if (*last_price - price_now).abs() < EPS {
                *last_ts = timestamp_ms;
            } else {
                buf.push_back((timestamp_ms, price_now));
            }
        } else {
            buf.push_back((timestamp_ms, price_now));
        }

        // ====================================================
        // PRUNE EXPIRED PRICES
        // ====================================================

        // Calculate the oldest timestamp allowed in the window.
        let cutoff_ms = timestamp_ms.saturating_sub(
            self.entry_window_secs.saturating_mul(1000)
        );

        // Remove expired prices.
        while let Some((oldest_ts, _)) = buf.front() {
            if *oldest_ts < cutoff_ms {
                buf.pop_front();
            } else {
                break;
            }
        }

        if buf.len() < 2 {
            return None;
        }

        // ====================================================
        // FIND WINDOW EXTREMES
        // ====================================================

        let mut min_price = f64::INFINITY;
        let mut max_price = f64::NEG_INFINITY;

        let mut min_ts = timestamp_ms;
        let mut max_ts = timestamp_ms;

        let mut oldest_ts = timestamp_ms;

        for (ts, price) in buf.iter() {
            if *price < min_price {
                min_price = *price;
                min_ts = *ts;
            }

            if *price > max_price {
                max_price = *price;
                max_ts = *ts;
            }

            if *ts < oldest_ts {
                oldest_ts = *ts;
            }
        }

        if !min_price.is_finite()
            || !max_price.is_finite()
            || min_price <= 0.0
        {
            return None;
        }

        // ====================================================
        // CALCULATE PERCENTAGE DIFFERENCES
        // ====================================================

        let pct_from_min =
            ((price_now - min_price) / min_price) * 100.0;

        let pct_from_max =
            ((max_price - price_now) / max_price) * 100.0;

        let elapsed_secs =
            timestamp_ms.saturating_sub(oldest_ts) as u64 / 1000;

        // ====================================================
        // DEBUG
        // ====================================================

        if self.debug_mode {
            print_log(
                self.debug_mode,
                false,
                format!(
                    "[PRICE WINDOW][{}] \
                    ts_ms={} | \
                    price=${:.4} | \
                    min=${:.4} @ {} | \
                    max=${:.4} @ {} | \
                    above_min={:.3}% | \
                    below_max={:.3}% | \
                    elapsed={}s | \
                    points={}",
                    symbol,
                    timestamp_ms,
                    price_now,
                    min_price,
                    min_ts,
                    max_price,
                    max_ts,
                    pct_from_min,
                    pct_from_max,
                    elapsed_secs,
                    buf.len(),
                ),
            );
        }

        Some((
            pct_from_min,
            pct_from_max,
            elapsed_secs,
        ))
    }

    
    // ========================================================
    // CLEAR SYMBOL PRICE WINDOW
    // ========================================================

    /// Removes the entire price history for a specific symbol.
    ///
    /// Returns true if the symbol had an existing price window.
    /// Returns false if the symbol was already absent.
    pub fn clear_symbol(
        &mut self,
        symbol: &str,
    ) -> bool {
        self.window_prices.remove(symbol).is_some()
    }


    // ========================================================
    // CLEAR ALL PRICE WINDOWS
    // ========================================================

    /// Removes all stored symbol price histories.
    pub fn clear_all(
        &mut self,
    ) {
        self.window_prices.clear();
    }
}