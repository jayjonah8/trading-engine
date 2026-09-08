use crate::core::events::Bar;

/// Moving Average Convergence Divergence (MACD).
///
/// MACD compares a fast EMA against a slower EMA to measure
/// trend direction and momentum.
///
/// Standard settings:
/// - Fast EMA:   12
/// - Slow EMA:   26
/// - Signal EMA: 9
///
/// Returns:
/// - macd:      fast EMA - slow EMA
/// - signal:    EMA of recent MACD values
/// - histogram: macd - signal
///
/// Positive MACD generally suggests bullish momentum.
/// Negative MACD generally suggests bearish momentum.
///
/// Returns None when there are not enough candles.
#[derive(Debug, Clone, Copy)]
pub struct Macd {
    pub macd: f64,
    pub signal: f64,
    pub histogram: f64,
}

pub fn macd(
    bars: &[Bar],
    fast_period: usize,
    slow_period: usize,
    signal_period: usize,
) -> Option<Macd> {
    if fast_period == 0
        || slow_period == 0
        || signal_period == 0
        || fast_period >= slow_period
        || bars.len()
            < slow_period + signal_period - 1
    {
        return None;
    }

    let closes: Vec<f64> =
        bars.iter()
            .map(|bar| bar.close)
            .collect();

    let fast_series =
        ema_series(
            &closes,
            fast_period,
        )?;

    let slow_series =
        ema_series(
            &closes,
            slow_period,
        )?;

    let offset =
        slow_period - fast_period;

    let mut macd_values =
        Vec::new();

    for i in 0..slow_series.len() {
        let fast_index =
            i + offset;

        if fast_index
            >= fast_series.len()
        {
            break;
        }

        macd_values.push(
            fast_series[fast_index]
                - slow_series[i]
        );
    }

    if macd_values.len()
        < signal_period
    {
        return None;
    }

    let signal_series =
        ema_series(
            &macd_values,
            signal_period,
        )?;

    let current_macd =
        *macd_values.last()?;

    let current_signal =
        *signal_series.last()?;

    Some(
        Macd {
            macd:
                current_macd,

            signal:
                current_signal,

            histogram:
                current_macd
                    - current_signal,
        }
    )
}


// Internal EMA series helper used by MACD.
fn ema_series(
    values: &[f64],
    period: usize,
) -> Option<Vec<f64>> {
    if period == 0
        || values.len() < period
    {
        return None;
    }

    let seed =
        values[..period]
            .iter()
            .sum::<f64>()
            / period as f64;

    let multiplier =
        2.0
            / (
                period as f64
                    + 1.0
            );

    let mut current =
        seed;

    let mut output =
        vec![current];

    for value in
        &values[period..]
    {
        current =
            (*value - current)
                * multiplier
                + current;

        output.push(
            current
        );
    }

    Some(output)
}