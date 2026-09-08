use crate::core::events::Bar;

/// Volume moving averages.
///
/// These indicators measure the average trading volume across recent
/// candles instead of averaging price.
///
/// Volume SMA:
/// - equal weight for every candle
///
/// Volume EMA:
/// - gives more weight to recent candles
///
/// Useful for:
/// - identifying increasing participation
/// - confirming breakouts
/// - comparing current volume against normal volume
/// - determining whether momentum has meaningful volume behind it
///
/// Returns None when there are not enough candles.
pub fn volume_sma(
    bars: &[Bar],
    period: usize,
) -> Option<f64> {
    if period == 0
        || bars.len() < period
    {
        return None;
    }

    let start =
        bars.len() - period;

    let total =
        bars[start..]
            .iter()
            .map(|bar| {
                bar.volume as f64
            })
            .sum::<f64>();

    Some(
        total / period as f64
    )
}


pub fn volume_ema(
    bars: &[Bar],
    period: usize,
) -> Option<f64> {
    if period == 0
        || bars.len() < period
    {
        return None;
    }

    let seed =
        bars[..period]
            .iter()
            .map(|bar| {
                bar.volume as f64
            })
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

    for bar in
        &bars[period..]
    {
        let volume =
            bar.volume as f64;

        current =
            (volume - current)
                * multiplier
                + current;
    }

    Some(current)
}