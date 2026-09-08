use crate::core::events::Bar;

/// Candle strength metrics.
///
/// Measures the shape and movement of completed candles.
///
/// Useful metrics:
/// - average_range: average high-to-low price range
/// - average_body: average absolute open-to-close body size
/// - average_body_percent: average candle body as a % of open
/// - latest_body_to_range: how much of the latest candle's total
///   range is occupied by its real body
/// - latest_close_position: where the latest close sits inside the
///   candle range (0.0 = low, 1.0 = high)
///
/// Useful for:
/// - momentum strength
/// - identifying large directional candles
/// - detecting weak/wicky candles
/// - custom breakout / continuation logic
///
/// Returns None when there are not enough candles.
#[derive(Debug, Clone, Copy)]
pub struct CandleStrength {
    pub average_range: f64,
    pub average_body: f64,
    pub average_body_percent: f64,

    pub latest_body_to_range: f64,
    pub latest_close_position: f64,
}

pub fn candle_strength(
    bars: &[Bar],
    period: usize,
) -> Option<CandleStrength> {
    if period == 0
        || bars.len() < period
    {
        return None;
    }

    let start =
        bars.len() - period;

    let recent =
        &bars[start..];

    let mut total_range =
        0.0;

    let mut total_body =
        0.0;

    let mut total_body_percent =
        0.0;

    for bar in recent {
        let range =
            bar.high - bar.low;

        let body =
            (
                bar.close
                    - bar.open
            )
            .abs();

        total_range +=
            range;

        total_body +=
            body;

        if bar.open > 0.0 {
            total_body_percent +=
                (
                    body
                        / bar.open
                )
                * 100.0;
        }
    }

    let latest =
        bars.last()?;

    let latest_range =
        latest.high
            - latest.low;

    let latest_body =
        (
            latest.close
                - latest.open
        )
        .abs();

    let latest_body_to_range =
        if latest_range > 0.0 {
            latest_body
                / latest_range
        } else {
            0.0
        };

    let latest_close_position =
        if latest_range > 0.0 {
            (
                latest.close
                    - latest.low
            )
                / latest_range
        } else {
            0.5
        };

    Some(
        CandleStrength {
            average_range:
                total_range
                    / period as f64,

            average_body:
                total_body
                    / period as f64,

            average_body_percent:
                total_body_percent
                    / period as f64,

            latest_body_to_range,

            latest_close_position,
        }
    )
}