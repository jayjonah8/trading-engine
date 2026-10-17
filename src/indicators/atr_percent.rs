use crate::{
    core::events::Bar,
    indicators::atr,
};

// WARNING: ATR_PERCENT has special historical candle requirements (typically period + 1).
// The shared valid_candle_window() function must account for these requirements
// before integration. Leave ATR and ATR% validation unchanged until then.


/// ATR PERCENT
///
/// Converts Average True Range from raw price units into
/// a percentage of the latest closing price.
///
/// Formula:
///     ATR% = ATR / latest_close * 100
///
/// Example:
///     ATR = $1.50
///     price = $100
///     ATR% = 1.5%
///
/// Useful for:
/// - comparing volatility between differently priced stocks
/// - volatility-normalized stops
/// - volatility filters
/// - determining how large candle movement is relative to price
///
/// Unlike raw ATR, ATR% is directly comparable across symbols.
pub fn atr_percent(
    bars: &[Bar],
    period: usize,
) -> Option<f64> {
    let atr_value =
        atr(
            bars,
            period,
        )?;

    let latest_close =
        bars.last()?.close;

    if latest_close <= 0.0 {
        return None;
    }

    Some(
        (
            atr_value
                / latest_close
        )
            * 100.0
    )
}