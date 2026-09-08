use crate::core::events::Bar;

/// AVERAGE TRUE RANGE (ATR)
///
/// ATR measures price volatility: how much the market has been moving,
/// regardless of whether the movement is upward or downward.
///
/// True Range considers:
/// - current high - current low
/// - absolute(current high - previous close)
/// - absolute(current low - previous close)
///
/// ATR averages True Range over the selected number of candles.
///
/// Example:
///     ATR14 = average True Range over the most recent 14 candles.
///
/// Common uses:
/// - Measure current volatility.
/// - Size stop losses according to volatility.
/// - Detect volatility expansion or contraction.
/// - Compare current movement against normal market movement.
///
/// Interpretation:
/// - Higher ATR = larger recent price movements / higher volatility.
/// - Lower ATR = smaller recent price movements / lower volatility.
///
/// ATR measures movement magnitude, NOT direction.
/// A high ATR does not mean bullish or bearish by itself.
pub fn atr(
    bars: &[Bar],
    period: usize,
) -> Option<f64> {
    if period == 0
        || bars.len() < period + 1
    {
        return None;
    }

    let start =
        bars.len() - period;

    let mut total_true_range =
        0.0;

    for i in start..bars.len() {
        let current =
            &bars[i];

        let previous_close =
            bars[i - 1].close;

        let high_low =
            current.high
                - current.low;

        let high_prev =
            (current.high
                - previous_close)
                .abs();

        let low_prev =
            (current.low
                - previous_close)
                .abs();

        let true_range =
            high_low
                .max(high_prev)
                .max(low_prev);

        total_true_range +=
            true_range;
    }

    Some(
        total_true_range
            / period as f64
    )
}