use crate::core::events::Bar;

/// Bollinger Bands.
///
/// Bollinger Bands measure how far price is from its recent average
/// relative to recent volatility.
///
/// Standard configuration:
/// - period = 20
/// - standard deviation multiplier = 2.0
///
/// Returns:
/// - middle: SMA
/// - upper:  SMA + standard deviation band
/// - lower:  SMA - standard deviation band
/// - width:  distance between upper and lower bands
///
/// Useful for:
/// - volatility expansion
/// - volatility compression
/// - mean-reversion context
/// - identifying unusually stretched prices
///
/// Returns None when there are not enough candles.
#[derive(Debug, Clone, Copy)]
pub struct BollingerBands {
    pub upper: f64,
    pub middle: f64,
    pub lower: f64,
    pub width: f64,
}

pub fn bollinger_bands(
    bars: &[Bar],
    period: usize,
    std_dev_multiplier: f64,
) -> Option<BollingerBands> {
    if period == 0
        || bars.len() < period
        || !std_dev_multiplier
            .is_finite()
    {
        return None;
    }

    let start =
        bars.len() - period;

    let closes: Vec<f64> =
        bars[start..]
            .iter()
            .map(|bar| bar.close)
            .collect();

    let mean =
        closes.iter()
            .sum::<f64>()
            / period as f64;

    let variance =
        closes.iter()
            .map(|close| {
                let diff =
                    close - mean;

                diff * diff
            })
            .sum::<f64>()
            / period as f64;

    let standard_deviation =
        variance.sqrt();

    let upper =
        mean
            + standard_deviation
                * std_dev_multiplier;

    let lower =
        mean
            - standard_deviation
                * std_dev_multiplier;

    Some(
        BollingerBands {
            upper,
            middle: mean,
            lower,

            width:
                upper - lower,
        }
    )
}