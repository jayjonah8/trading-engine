use crate::core::events::Bar;

/// SIMPLE MOVING AVERAGE (SMA)
///
/// SMA measures the average closing price over the most recent N candles.
///
/// Formula:
///     SMA = sum of the last N closing prices / N
///
/// Example:
///     SMA20 on 5-minute candles = average close of the last
///     20 completed 5-minute candles.
///
/// Common uses:
/// - Identify the general price trend.
/// - Compare current price against its recent average.
/// - Compare fast and slow SMAs for trend changes.
/// - Use as the center line for indicators such as Bollinger Bands.
///
/// Interpretation:
/// - Price > SMA can indicate price is above its recent average.
/// - Price < SMA can indicate price is below its recent average.
/// - Rising SMA suggests the recent average price is increasing.
/// - Falling SMA suggests the recent average price is decreasing.
///
/// SMA reacts more slowly to recent price changes than EMA because
/// every candle inside the selected period receives equal weight.
pub fn sma(
    bars: &[Bar],
    period: usize,
) -> Option<f64> {
    if period == 0 || bars.len() < period {
        return None;
    }

    let start =
        bars.len() - period;

    let sum: f64 =
        bars[start..]
            .iter()
            .map(|bar| bar.close)
            .sum();

    Some(
        sum / period as f64
    )
}