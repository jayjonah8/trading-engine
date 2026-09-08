use crate::core::events::Bar;

/// RELATIVE VOLUME (RVOL)
///
/// Relative Volume compares the current candle's volume against the
/// average volume of recent candles.
///
/// Basic formula:
///     RVOL = current_volume / average_previous_volume
///
/// Example:
///     RVOL = 1.0x -> approximately normal volume.
///     RVOL = 2.0x -> approximately twice normal volume.
///     RVOL = 5.0x -> approximately five times normal volume.
///
/// Common uses:
/// - Detect unusually high market participation.
/// - Confirm breakouts or momentum moves.
/// - Identify unusually active stocks.
/// - Filter weak price movements occurring on low volume.
///
/// Interpretation:
/// - RVOL > 1.0 = volume is above its recent average.
/// - RVOL < 1.0 = volume is below its recent average.
/// - Very high RVOL indicates unusually strong participation.
///
/// Important:
/// A simple rolling RVOL does not account for normal intraday volume
/// patterns. Market-open and market-close candles naturally tend to
/// have much higher volume than midday candles.
pub fn relative_volume(
    bars: &[Bar],
    period: usize,
) -> Option<f64> {
    if period == 0
        || bars.len() < period + 1
    {
        return None;
    }

    let current =
        bars.last()?;

    let history_end =
        bars.len() - 1;

    let history_start =
        history_end - period;

    let average_volume =
        bars[history_start..history_end]
            .iter()
            .map(|bar| {
                bar.volume as f64
            })
            .sum::<f64>()
            / period as f64;

    if average_volume <= 0.0 {
        return None;
    }

    Some(
        current.volume as f64
            / average_volume
    )
}