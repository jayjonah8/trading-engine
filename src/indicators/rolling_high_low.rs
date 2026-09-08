use crate::core::events::Bar;

/// Rolling High / Low.
///
/// Finds the highest high and lowest low over the most recent
/// N completed candles.
///
/// Useful for:
/// - breakout detection
/// - support / resistance
/// - pullback measurement
/// - recent market range
/// - distance from local extremes
///
/// Returns None when there are not enough candles.
#[derive(Debug, Clone, Copy)]
pub struct RollingHighLow {
    pub high: f64,
    pub low: f64,
    pub range: f64,
}

pub fn rolling_high_low(
    bars: &[Bar],
    period: usize,
) -> Option<RollingHighLow> {
    if period == 0
        || bars.len() < period
    {
        return None;
    }

    let start =
        bars.len() - period;

    let mut high =
        f64::NEG_INFINITY;

    let mut low =
        f64::INFINITY;

    for bar in
        &bars[start..]
    {
        high =
            high.max(
                bar.high
            );

        low =
            low.min(
                bar.low
            );
    }

    if !high.is_finite()
        || !low.is_finite()
    {
        return None;
    }

    Some(
        RollingHighLow {
            high,
            low,

            range:
                high - low,
        }
    )
}