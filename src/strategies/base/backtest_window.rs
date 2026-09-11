use std::collections::VecDeque;

use chrono::{
    DateTime,
    Duration,
    Utc,
};

use super::backtest_state::BaseBacktestStrategy;

use crate::shared::print_log;

impl BaseBacktestStrategy {
    /// Maintains a rolling historical price window and calculates how far
    /// the current price is above the minimum and below the maximum.
    ///
    /// Historical timestamps are used instead of Instant::now(), allowing
    /// the backtest to run at full speed while still respecting market time.
    pub(crate) fn update_and_get_backtest_window_extreme_diffs(
        &mut self,
        symbol: &str,
        now: DateTime<Utc>,
        price_now: f64,
    ) -> Option<(f64, f64, u64)> {
        let buf = self
            .window_prices
            .entry(symbol.to_string())
            .or_insert_with(VecDeque::new);

        const EPS: f64 = 1e-8;

        // Avoid filling the window with identical consecutive prices.
        // If the price is unchanged, update its timestamp instead.
        if let Some((last_t, last_p)) = buf.back_mut() {
            if (*last_p - price_now).abs() < EPS {
                *last_t = now;
            } else {
                buf.push_back((now, price_now));
            }
        } else {
            buf.push_back((now, price_now));
        }

        // Calculate the oldest historical timestamp that is still allowed
        // to remain inside the configured rolling window.
        let cutoff =
            now - Duration::seconds(self.entry_window_secs);

        while let Some((t_old, _)) = buf.front() {
            if *t_old < cutoff {
                buf.pop_front();
            } else {
                break;
            }
        }

        if buf.len() < 2 {
            return None;
        }

       let mut min_p = f64::INFINITY;
        let mut max_p = f64::NEG_INFINITY;

        // Store the timestamps where the rolling minimum
        // and maximum prices occurred for debugging.
        let mut min_time = now;
        let mut max_time = now;

        let mut oldest_t = now;

        for (timestamp, price) in buf.iter() {
            if price.is_finite() {
                if *price < min_p {
                    min_p = *price;
                    min_time = *timestamp;
                }

                if *price > max_p {
                    max_p = *price;
                    max_time = *timestamp;
                }
            }

            if *timestamp < oldest_t {
                oldest_t = *timestamp;
            }
        }

        if !price_now.is_finite()
            || !min_p.is_finite()
            || !max_p.is_finite()
            || min_p <= 0.0
        {
            return None;
        }

        let pct_from_min =
            ((price_now - min_p) / min_p) * 100.0;

        let pct_from_max =
            if max_p > 0.0 {
                ((max_p - price_now) / max_p) * 100.0
            } else {
                0.0
            };

        print_log(
            self.debug_mode,
            false,
            format!(
                "[BASE BACKTEST WINDOW DEBUG][{}] \
                now={} | \
                price=${:.4} | \
                min=${:.4} @ {} | \
                max=${:.4} @ {} | \
                above_min={:.3}% | \
                below_max={:.3}% | \
                points={}",
                symbol,
                now,
                price_now,
                min_p,
                min_time,
                max_p,
                max_time,
                pct_from_min,
                pct_from_max,
                buf.len(),
            ),
        );

        let elapsed_secs =
            (now - oldest_t)
                .num_seconds()
                .max(0) as u64;

        Some((
            pct_from_min,
            pct_from_max,
            elapsed_secs,
        ))
    }
}