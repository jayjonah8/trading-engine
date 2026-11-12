use anyhow::{bail, Context, Result};
use serde_json::json;
use std::{
    fs,
    path::Path,
};

use crate::{
    core::events::Bar,
    market::timeframe::Timeframe,
    toolbox::price_levels::range::types::PriceRanges,
};


// ====================================================
// PRICE RANGE HTML VISUALIZATION
// ====================================================
//
// Generates an interactive HTML candlestick chart
// containing:
//
// - Historical OHLC candles.
// - Detected trading ranges.
// - Upper and lower range boundaries.
// - Range midpoint.
// - Actual observed price extremes.
// - Range strength and validation statistics.
//
// No changes to the existing detectors are required.


// ====================================================
// GENERATE PRICE RANGE CHART
// ====================================================

pub fn generate_price_ranges_chart(
    output_path: impl AsRef<Path>,
    symbol: &str,
    timeframe: Timeframe,
    bars: &[Bar],
    ranges: &PriceRanges,
) -> Result<()> {

    // ====================================================
    // VALIDATE INPUT
    // ====================================================

    if bars.is_empty() {
        bail!("Cannot generate chart without candles");
    }

    // ====================================================
    // SERIALIZE CANDLES
    // ====================================================

    let candles: Vec<_> = bars
        .iter()
        .map(|bar| {
            json!({
                "time": bar.start.timestamp(),
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
            })
        })
        .collect();

    // ====================================================
    // SERIALIZE TRADING RANGES
    // ====================================================

    let price_ranges: Vec<_> = ranges
        .ranges
        .iter()
        .map(|range| {
            json!({
                "lower": range.lower_boundary,
                "upper": range.upper_boundary,

                "lowest": range.lowest_price,
                "highest": range.highest_price,

                "midpoint": range.midpoint,
                "width": range.width_pct,

                "supportStrength": range.support_strength,
                "resistanceStrength": range.resistance_strength,

                "supportTouches": range.support_touches,
                "resistanceTouches": range.resistance_touches,

                "oscillations": range.completed_oscillations,
                "containment": range.containment_pct,

                "strength": range.strength,

                "startTs": range.start_ts,
                "confirmedTs": range.confirmed_ts,
                "lastTouchTs": range.last_touch_ts,
            })
        })
        .collect();

    // ====================================================
    // SERIALIZE CHART DATA
    // ====================================================

    let chart_data = json!({
        "symbol": symbol,
        "timeframe": timeframe.as_secs(),
        "calculatedTs": ranges.calculated_ts,
        "candles": candles,
        "ranges": price_ranges,
    });

    let chart_data_json = serde_json::to_string(&chart_data)?
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026");

    // ====================================================
    // HTML TEMPLATE
    // ====================================================

    let html = include_str!("range_template.html")
        .replace("__PRICE_RANGE_DATA__", &chart_data_json);

    // ====================================================
    // WRITE HTML FILE
    // ====================================================

    let path = output_path.as_ref();

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .context("Failed to create chart directory")?;
        }
    }

    fs::write(path, html)
        .with_context(|| {
            format!(
                "Failed to write range chart: {}",
                path.display()
            )
        })?;

    Ok(())
}