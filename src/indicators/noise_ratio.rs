use crate::{
    core::events::Bar,
    indicators::{
        rate_of_change,
        realized_volatility_pct,
    },
};

/// NOISE RATIO
///
/// Compares realized volatility against the absolute net price move.
///
/// Formula:
///     noise_ratio = realized_volatility_pct / abs(net_move_pct)
///
/// Interpretation:
/// - Lower value = cleaner directional move.
/// - Higher value = more volatility/chop relative to net progress.
///
/// Example:
///     net move = +3%
///     realized volatility = 3.5%
///     noise ratio ≈ 1.17
///
///     net move = +3%
///     realized volatility = 12%
///     noise ratio = 4.0
///
/// Useful for:
/// - filtering choppy trends
/// - detecting clean directional movement
/// - separating noisy momentum from efficient momentum
///
/// Returns None when the net move is effectively zero because
/// the ratio would become unstable or meaningless.
pub fn noise_ratio(
    bars: &[Bar],
    period: usize,
) -> Option<f64> {
    let realized_vol =
        realized_volatility_pct(
            bars,
            period,
        )?;

    let net_move =
        rate_of_change(
            bars,
            period,
        )?;

    let abs_net_move =
        net_move.abs();

    const EPS: f64 =
        1e-12;

    if abs_net_move < EPS {
        return None;
    }

    Some(
        realized_vol
            / abs_net_move
    )
}