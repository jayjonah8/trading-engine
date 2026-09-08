use crate::{
    core::events::Bar,
    indicators::atr,
};

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