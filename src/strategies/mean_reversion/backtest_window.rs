use std::collections::VecDeque;

use chrono::{
    DateTime,
    Duration,
    Utc,
};

use super::backtest_strategy::
    MeanReversionBacktestStrategy;


impl MeanReversionBacktestStrategy {
    pub(crate) fn update_and_get_backtest_window_extreme_diffs(
        &mut self,
        symbol: &str,
        now: DateTime<Utc>,
        price_now: f64,
    ) -> Option<(f64, f64, u64)> {
        let buf =
            self.window_prices
                .entry(
                    symbol.to_string()
                )
                .or_insert_with(
                    VecDeque::new
                );

        const EPS: f64 = 1e-8;


        if let Some((
            last_t,
            last_p,
        )) = buf.back_mut()
        {
            if (*last_p - price_now)
                .abs() < EPS
            {
                *last_t = now;
            } else {
                buf.push_back(
                    (now, price_now)
                );
            }
        } else {
            buf.push_back(
                (now, price_now)
            );
        }


        let cutoff =
            now
                - Duration::seconds(
                    self.entry_window_secs
                );


        while let Some(
            (t_old, _)
        ) = buf.front()
        {
            if *t_old < cutoff {
                buf.pop_front();
            } else {
                break;
            }
        }


        if buf.len() < 2 {
            return None;
        }


        let mut min_p =
            f64::INFINITY;

        let mut max_p =
            f64::NEG_INFINITY;

        let mut oldest_t =
            now;


        for (
            timestamp,
            price,
        ) in buf.iter()
        {
            if price.is_finite() {
                if *price < min_p {
                    min_p = *price;
                }

                if *price > max_p {
                    max_p = *price;
                }
            }

            if *timestamp < oldest_t {
                oldest_t =
                    *timestamp;
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
            ((price_now - min_p)
                / min_p)
                * 100.0;


        let pct_from_max =
            if max_p > 0.0 {
                ((max_p - price_now)
                    / max_p)
                    * 100.0
            } else {
                0.0
            };


        let elapsed_secs =
            (now - oldest_t)
                .num_seconds()
                .max(0)
                as u64;


        Some((
            pct_from_min,
            pct_from_max,
            elapsed_secs,
        ))
    }
}