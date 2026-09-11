use std::{
    collections::VecDeque,
    time::{
        Duration,
        Instant,
    },
};

use super::state::MeanReversionStrategy;


impl MeanReversionStrategy {
    pub(crate) fn update_and_get_window_extreme_diffs(
        &mut self,
        symbol: &str,
        now: Instant,
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
            now.checked_sub(
                Duration::from_secs(
                    self.entry_window_secs
                )
            )
            .unwrap_or(now);


        while let Some(
            (t_old, _)
        ) = buf.front().copied()
        {
            if t_old < cutoff {
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


        for (t, p) in
            buf.iter().copied()
        {
            if p.is_finite() {
                if p < min_p {
                    min_p = p;
                }

                if p > max_p {
                    max_p = p;
                }
            }

            if t < oldest_t {
                oldest_t = t;
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
            now.duration_since(
                oldest_t
            )
            .as_secs();


        Some((
            pct_from_min,
            pct_from_max,
            elapsed_secs,
        ))
    }
}