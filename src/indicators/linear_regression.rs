use crate::core::events::Bar;

/// LINEAR REGRESSION TREND METRICS
///
/// Fits a straight line through candle closing prices over time.
///
/// Returns:
/// - slope_pct_per_hour:
///   Trend slope normalized into percent price change per hour.
///   Positive = uptrend.
///   Negative = downtrend.
///
/// - r2:
///   R-squared goodness-of-fit between 0.0 and 1.0.
///   Near 1.0 = smooth, consistent trend.
///   Near 0.0 = noisy / poorly fitted trend.
///
/// Useful for:
/// - trend direction
/// - trend strength
/// - filtering noisy price movement
/// - distinguishing smooth trends from random chop
#[derive(Debug, Clone, Copy)]
pub struct LinearRegressionTrend {
    pub slope_pct_per_hour: f64,
    pub r2: f64,
}

pub fn linear_regression_trend(
    bars: &[Bar],
    period: usize,
) -> Option<LinearRegressionTrend> {
    if period < 2 || bars.len() < period {
        return None;
    }

    let start =
        bars.len() - period;

    let recent =
        &bars[start..];

    let first_time =
        recent.first()?.end;

    let mean_price =
        recent.iter()
            .map(|bar| bar.close)
            .sum::<f64>()
            / period as f64;

    if !mean_price.is_finite()
        || mean_price <= 0.0
    {
        return None;
    }

    let x_values: Vec<f64> =
        recent.iter()
            .map(|bar| {
                (bar.end - first_time)
                    .num_milliseconds()
                    as f64
                    / 3_600_000.0
            })
            .collect();

    let mean_x =
        x_values.iter()
            .sum::<f64>()
            / period as f64;

    let mut covariance =
        0.0;

    let mut variance_x =
        0.0;

    let mut variance_y =
        0.0;

    for (x, bar) in
        x_values.iter()
            .zip(recent.iter())
    {
        let dx =
            *x - mean_x;

        let dy =
            bar.close - mean_price;

        covariance +=
            dx * dy;

        variance_x +=
            dx * dx;

        variance_y +=
            dy * dy;
    }

    if variance_x <= 0.0 {
        return None;
    }

    let slope_price_per_hour =
        covariance / variance_x;

    let slope_pct_per_hour =
        (
            slope_price_per_hour
                / mean_price
        )
        * 100.0;

    let r2 =
        if variance_y <= 0.0 {
            1.0
        } else {
            let denominator =
                variance_x
                    * variance_y;

            if denominator <= 0.0 {
                0.0
            } else {
                (
                    covariance
                        * covariance
                )
                    / denominator
            }
        };

    Some(
        LinearRegressionTrend {
            slope_pct_per_hour,
            r2:
                r2.clamp(
                    0.0,
                    1.0,
                ),
        }
    )
}