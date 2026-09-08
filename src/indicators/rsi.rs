use crate::core::events::Bar;

/// Relative Strength Index (RSI).
///
/// RSI measures recent price momentum by comparing average gains
/// against average losses over a lookback period.
///
/// Typical interpretation:
/// - RSI > 70  -> relatively overbought / strong upward momentum
/// - RSI < 30  -> relatively oversold / strong downward momentum
/// - RSI ~ 50  -> neutral momentum
///
/// RSI should usually be treated as context rather than a standalone
/// buy/sell signal.
///
/// Returns None when there are not enough candles.
pub fn rsi(
    bars: &[Bar],
    period: usize,
) -> Option<f64> {
    if period == 0
        || bars.len() < period + 1
    {
        return None;
    }

    let start =
        bars.len() - period - 1;

    let relevant =
        &bars[start..];

    let mut gains =
        0.0;

    let mut losses =
        0.0;

    for pair in relevant.windows(2) {
        let change =
            pair[1].close
                - pair[0].close;

        if change > 0.0 {
            gains += change;
        } else if change < 0.0 {
            losses += -change;
        }
    }

    let average_gain =
        gains / period as f64;

    let average_loss =
        losses / period as f64;

    if average_loss == 0.0 {
        return Some(100.0);
    }

    if average_gain == 0.0 {
        return Some(0.0);
    }

    let rs =
        average_gain
            / average_loss;

    Some(
        100.0
            - (
                100.0
                    / (1.0 + rs)
            )
    )
}