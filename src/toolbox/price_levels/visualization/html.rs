use anyhow::{bail, Context, Result};
use serde_json::json;
use std::{
    fs,
    path::Path,
};

use crate::{
    core::events::Bar,
    market::timeframe::Timeframe,
    toolbox::price_levels::{
        PriceLevelType,
        PriceLevels,
    },
};

// ====================================================
// PRICE LEVEL HTML VISUALIZATION
// ====================================================
//
// Generates an interactive HTML candlestick chart
// containing:
//
// - Historical OHLC candles.
// - Support and resistance zones.
// - Representative price levels.
// - Strength scores and reaction counts.
// - Average reversal and relative volume.
//
// No changes to the existing detector are required.

// ====================================================
// GENERATE PRICE LEVEL CHART
// ====================================================

pub fn generate_price_levels_chart(
    output_path: impl AsRef<Path>,
    symbol: &str,
    timeframe: Timeframe,
    bars: &[Bar],
    levels: &PriceLevels,
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
    // SERIALIZE PRICE LEVELS
    // ====================================================

    let price_levels: Vec<_> = levels
        .levels
        .iter()
        .map(|level| {

            let level_type = match level.level_type {
                PriceLevelType::Support => "Support",
                PriceLevelType::Resistance => "Resistance",
                PriceLevelType::SupportResistance => {
                    "SupportResistance"
                }
            };

            json!({
                "price": level.price,
                "lower": level.lower_bound,
                "upper": level.upper_bound,
                "type": level_type,
                "strength": level.strength,
                "touches": level.touches,
                "reversal": level.average_reversal_pct,
                "volume": level.relative_volume,
                "lastTouch": level.last_touch_ts,
            })
        })
        .collect();

    // ====================================================
    // SERIALIZE CHART DATA
    // ====================================================

    let chart_data = json!({
        "symbol": symbol,
        "timeframe": timeframe.as_secs(),
        "calculatedTs": levels.calculated_ts,
        "candles": candles,
        "levels": price_levels,
    });

    // Prevent serialized data from prematurely closing
    // the HTML script element.

    let chart_data_json = serde_json::to_string(&chart_data)?
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026");

    // ====================================================
    // HTML TEMPLATE
    // ====================================================

    let html = include_str!("template.html")
        .replace("__PRICE_LEVEL_DATA__", &chart_data_json);

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
                "Failed to write chart: {}",
                path.display()
            )
        })?;

    Ok(())
}