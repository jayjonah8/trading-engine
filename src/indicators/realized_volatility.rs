use crate::core::events::Bar;

/// REALIZED VOLATILITY
///
/// Measures variability of candle-to-candle returns using
/// the standard deviation of log returns.
///
/// This version is NOT annualized.
///
/// The returned value is expressed as a percent.
///
/// Example:
///     0.50 = approximately 0.50% return volatility
///     over the selected candle-return sample.
///
/// Useful for:
/// - measuring choppiness
/// - identifying changing volatility regimes
/// - comparing volatility between symbols
/// - filtering unstable market conditions
///
/// Unlike ATR, realized volatility measures variation in returns,
/// not the average high-low candle range.
pub fn realized_volatility_pct(
    bars: &[Bar],
    period: usize,
) -> Option<f64> {
    if period == 0
        || bars.len() < period + 1
    {
        return None;
    }

    let start =
        bars.len()
            - period
            - 1;

    let recent =
        &bars[start..];

    let mut returns =
        Vec::with_capacity(period);

    for pair in recent.windows(2) {
        let previous =
            pair[0].close;

        let current =
            pair[1].close;

        if previous <= 0.0
            || current <= 0.0
        {
            return None;
        }

        returns.push(
            (current / previous).ln()
        );
    }

    let mean =
        returns.iter()
            .sum::<f64>()
            / returns.len() as f64;

    let variance =
        returns.iter()
            .map(|value| {
                let diff =
                    value - mean;

                diff * diff
            })
            .sum::<f64>()
            / returns.len() as f64;

    Some(
        variance.sqrt()
            * 100.0
    )
}