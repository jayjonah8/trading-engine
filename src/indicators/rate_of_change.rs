use crate::core::events::Bar;

/// NET MOVE PERCENT NOTE
///
/// rate_of_change() is also effectively a net_move_pct calculation
/// when the requested lookback period matches the analysis window.
///
/// Example:
///     rate_of_change(bars, 20)
///
/// gives the net percentage move from the close 20 candles ago
/// to the latest candle close.

/// RATE OF CHANGE (ROC)
///
/// ROC measures the percentage change in price between the current
/// candle and a candle N periods ago.
///
/// Formula:
///     ROC = ((current_close - old_close) / old_close) * 100
///
/// Example:
///     ROC20 = +2.0%
///     Current price is approximately 2% above the comparison price.
///
///     ROC20 = -2.0%
///     Current price is approximately 2% below the comparison price.
///
/// Common uses:
/// - Measure price momentum.
/// - Detect accelerating or weakening moves.
/// - Compare momentum between symbols.
/// - Create momentum entry filters.
///
/// Interpretation:
/// - Positive ROC = price increased over the selected period.
/// - Negative ROC = price decreased over the selected period.
/// - Larger absolute ROC = larger price movement.
/// - ROC near zero = little net movement over the selected period.
///
/// ROC measures the net price change and does not describe the path
/// price took between the starting and ending candles.
pub fn rate_of_change(
    bars: &[Bar],
    period: usize,
) -> Option<f64> {
    if period == 0
        || bars.len() < period + 1
    {
        return None;
    }

    let current =
        bars.last()?.close;

    let old =
        bars[bars.len() - 1 - period]
            .close;

    if old <= 0.0 {
        return None;
    }

    Some(
        ((current - old) / old)
            * 100.0
    )
}