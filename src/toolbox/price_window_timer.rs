#![allow(dead_code)]

use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};

use crate::shared::print_log;


// ============================================================
// PRICE WINDOW TIMER
// ============================================================

pub struct PriceWindowTimer {
    pub debug_mode: bool,

    // Independent rolling price history for each symbol.
    window_prices: HashMap<String, VecDeque<(Instant, f64)>>,

    // Rolling window duration in seconds.
    entry_window_secs: i64,
}


// ============================================================
// IMPLEMENTATION
// ============================================================

impl PriceWindowTimer {

    // ========================================================
    // CONSTRUCTOR
    // ========================================================

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
    // UPDATE TIMER-BASED PRICE WINDOW
    // ========================================================

    /// Maintains a rolling price window using Instant-based timers.
    ///
    /// Uses monotonic elapsed time instead of market event timestamps,
    /// making this version suitable for real-time trading.
    ///
    /// Calculates the percentage difference between the current price
    /// and the minimum and maximum prices within the configured window.
    ///
    /// Maintains independent rolling price histories for each symbol.
    ///
    /// Returns:
    /// (pct_from_min, pct_from_max, elapsed_secs)
    pub fn update_price_window_extreme_diffs_timer(
        &mut self,
        symbol: &str,
        now: Instant,
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
        // VALIDATE TIMER ORDER
        // ====================================================

        // Rolling windows require chronological observations.
        if let Some((last_t, _)) = buf.back() {
            if now < *last_t {
                return None;
            }
        }

        // ====================================================
        // UPDATE PRICE HISTORY
        // ====================================================

        // Avoid storing identical consecutive prices.
        // Update the timestamp instead of adding another entry.
        if let Some((last_t, last_price)) = buf.back_mut() {
            if (*last_price - price_now).abs() < EPS {
                *last_t = now;
            } else {
                buf.push_back((now, price_now));
            }
        } else {
            buf.push_back((now, price_now));
        }

        // ====================================================
        // PRUNE EXPIRED PRICES
        // ====================================================

        let cutoff = now
            .checked_sub(Duration::from_secs(
                self.entry_window_secs as u64,
            ))
            .unwrap_or(now);

        while let Some((oldest_t, _)) = buf.front() {
            if *oldest_t < cutoff {
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

        let mut min_time = now;
        let mut max_time = now;
        let mut oldest_time = now;

        for (t, price) in buf.iter() {
            if *price < min_price {
                min_price = *price;
                min_time = *t;
            }

            if *price > max_price {
                max_price = *price;
                max_time = *t;
            }

            if *t < oldest_time {
                oldest_time = *t;
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

        let elapsed_secs = now
            .duration_since(oldest_time)
            .as_secs();

        // ====================================================
        // DEBUG
        // ====================================================

        if self.debug_mode {
            print_log(
                self.debug_mode,
                false,
                format!(
                    "[PRICE WINDOW TIMER][{}] \
                     now={:?} | \
                     price=${:.4} | \
                     min=${:.4} @ {:?} | \
                     max=${:.4} @ {:?} | \
                     above_min={:.3}% | \
                     below_max={:.3}% | \
                     elapsed={}s | \
                     points={}",
                    symbol,
                    now,
                    price_now,
                    min_price,
                    min_time,
                    max_price,
                    max_time,
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
}