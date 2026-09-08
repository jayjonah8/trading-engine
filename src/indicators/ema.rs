use crate::core::events::Bar;

/// EXPONENTIAL MOVING AVERAGE (EMA)
///
/// EMA measures the average price over time while giving more weight
/// to recent candles than older candles.
///
/// Smoothing multiplier:
///     alpha = 2 / (period + 1)
///
/// Each new EMA is approximately:
///     EMA = current_close * alpha + previous_EMA * (1 - alpha)
///
/// Example:
///     EMA20 on 5-minute candles tracks the recent 20-candle price
///     trend while responding more strongly to newer price movements.
///
/// Common uses:
/// - Detect short-term and long-term trends.
/// - Measure changing price direction.
/// - Compare fast and slow EMAs.
/// - Build indicators such as MACD.
///
/// Interpretation:
/// - Price > EMA can indicate strength relative to the recent trend.
/// - Price < EMA can indicate weakness relative to the recent trend.
/// - Rising EMA suggests upward price momentum.
/// - Falling EMA suggests downward price momentum.
///
/// EMA responds faster to recent price changes than SMA.
pub fn ema(
    bars: &[Bar],
    period: usize,
) -> Option<f64> {
    if period == 0 || bars.len() < period {
        return None;
    }

    let seed_sum: f64 =
        bars[..period]
            .iter()
            .map(|bar| bar.close)
            .sum();

    let mut ema =
        seed_sum / period as f64;

    let multiplier =
        2.0 / (period as f64 + 1.0);

    for bar in &bars[period..] {
        ema =
            (bar.close - ema)
                * multiplier
                + ema;
    }

    Some(ema)
}