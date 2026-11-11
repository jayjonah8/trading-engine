use anyhow::{bail, Context, Result};
use chrono::{DateTime, NaiveDateTime, Utc};

use crate::{
    market::{
        candle_history::CandleHistory,
        timeframe::Timeframe,
    },
    toolbox::price_levels::{
        PriceLevelConfig,
        PriceLevelDetector,
        PriceLevelType,
        generate_price_levels_chart,
    },
};

use super::hyperliquid::{
    fetch_test_candles,
    fetch_test_candles_range,
};

// ====================================================
// PRICE LEVEL DETECTION VALIDATION
// ====================================================
//
// Downloads real historical candles from Hyperliquid,
// populates CandleHistory, and runs the existing
// PriceLevelDetector.
//
// Supports two testing modes:
//
// 1. Latest completed candles using a lookback duration.
// 2. Historical candles using explicit UTC start/end dates.
//
// Prints a single final snapshot of detected support
// and resistance zones for manual comparison against
// actual historical market charts.
//
// This is a network-dependent integration test.

// ====================================================
// TEST CONFIGURATION
// ====================================================

const TEST_SYMBOL: &str = "ZEC";

const TEST_TIMEFRAME: Timeframe = Timeframe::FIVE_MINUTES;

// false = Latest completed candles.
// true  = Historical start/end date range.

const USE_HISTORICAL_RANGE: bool = false;

// 1 day.
const TEST_LOOKBACK_SECS: u64 = 24 * 60 * 60;

// 2 days.
// const TEST_LOOKBACK_SECS: u64 = 2 * 24 * 60 * 60;

// 7 days.
// const TEST_LOOKBACK_SECS: u64 = 7 * 24 * 60 * 60;

// Dates are interpreted as UTC.
//
// The start date is inclusive.
// The end date is exclusive.
//
// Example:
// 2026-11-01 00:00:00 -> 2026-11-03 00:00:00
// represents exactly 48 hours.

// 4 days ago (UTC).
const TEST_START_DATE: &str = "2026-11-07 00:00:00";
// 2 days ago (UTC).
const TEST_END_DATE: &str = "2026-11-09 00:00:00";

// // 6 days ago (UTC).
// const TEST_START_DATE: &str = "2026-11-05 00:00:00";
// // 4 days ago (UTC).
// const TEST_END_DATE: &str = "2026-11-07 00:00:00";

// ====================================================
// PARSE HISTORICAL DATE
// ====================================================

fn parse_test_date(date: &str) -> Result<DateTime<Utc>> {

    let naive = NaiveDateTime::parse_from_str(
        date,
        "%Y-%m-%d %H:%M:%S",
    )
    .with_context(|| {
        format!("Invalid historical test date: {}", date)
    })?;

    Ok(DateTime::<Utc>::from_naive_utc_and_offset(
        naive,
        Utc,
    ))
}

// ====================================================
// TEST PRICE LEVEL DETECTION
// ====================================================

// Run the test:
// cargo test test_hyperliquid_price_levels -- --nocapture

// Open the generated HTML chart (macOS):
// open price_levels_chart.html

// Direct browser address:
// file:///Users/jared/web3/quant-finance/trading-bots/trading-engine/price_levels_chart.html

#[tokio::test]
async fn test_hyperliquid_price_levels() -> Result<()> {

    // ====================================================
    // RESOLVE TEST CONFIGURATION
    // ====================================================

    let historical_range = if USE_HISTORICAL_RANGE {

        let start = parse_test_date(TEST_START_DATE)?;
        let end = parse_test_date(TEST_END_DATE)?;

        if start >= end {
            bail!("Historical start date must precede end date");
        }

        let duration_secs = u64::try_from(
            end.signed_duration_since(start).num_seconds(),
        )
        .context("Invalid historical range duration")?;

        if duration_secs == 0 {
            bail!("Historical range must be at least one second");
        }

        Some((start, end, duration_secs))

    } else {
        None
    };

    let lookback_secs = historical_range
        .as_ref()
        .map(|(_, _, duration_secs)| *duration_secs)
        .unwrap_or(TEST_LOOKBACK_SECS);

    // ====================================================
    // CONFIGURE DETECTOR
    // ====================================================

    let config = PriceLevelConfig {
        timeframe: TEST_TIMEFRAME,
        lookback_secs,
        ..PriceLevelConfig::default()
    };

    let required_candles = config.required_candles();

    // Additional candles allow swing confirmation
    // and provide a small historical buffer.

    let candle_limit = required_candles
        .checked_add(config.swing_strength * 2)
        .context("Candle limit overflow")?;

    // ====================================================
    // FETCH HISTORICAL CANDLES
    // ====================================================

    let bars = if let Some((start, end, _)) = historical_range {

        // Fetch exactly the selected historical period.
        //
        // No candles beyond the specified end timestamp
        // are included in the detector's history.

        fetch_test_candles_range(
            TEST_SYMBOL,
            TEST_TIMEFRAME,
            start.timestamp_millis(),
            end.timestamp_millis(),
        )
        .await?

    } else {

        // Preserve existing latest-candle behavior.

        fetch_test_candles(
            TEST_SYMBOL,
            TEST_TIMEFRAME,
            candle_limit,
        )
        .await?
    };

    if bars.is_empty() {
        bail!(
            "No historical candles returned for {}",
            TEST_SYMBOL
        );
    }

    // ====================================================
    // VALIDATE HISTORICAL DATA
    // ====================================================

    // Only completed candles are permitted.
    //
    // In historical mode, the selected end timestamp
    // is the analysis cutoff.
    //
    // In latest mode, the final completed candle
    // determines the analysis timestamp.

    let current_ts = if USE_HISTORICAL_RANGE {

        parse_test_date(TEST_END_DATE)?
            .timestamp_millis()

    } else {

        bars.last()
            .context("Missing latest candle")?
            .end
            .timestamp_millis()
    };

    // Defensive validation against future candles.

    for bar in &bars {
        if bar.end.timestamp_millis() > current_ts {
            bail!(
                "Candle extends beyond analysis timestamp: {}",
                bar.end
            );
        }
    }

    let current_price = bars
        .last()
        .context("Missing latest candle")?
        .close;

    // ====================================================
    // BUILD CANDLE HISTORY
    // ====================================================

    let retention_secs = lookback_secs
        .checked_add(TEST_TIMEFRAME.as_secs() * 10)
        .context("History retention overflow")?;

    let mut history = CandleHistory::new(
        std::time::Duration::from_secs(retention_secs),
    );

    for bar in bars.iter().cloned() {
        history.push(bar);
    }

    // ====================================================
    // RUN PRICE LEVEL DETECTOR
    // ====================================================

    let mut detector = PriceLevelDetector::new(config);

    detector.update(
        TEST_SYMBOL,
        TEST_TIMEFRAME,
        current_ts,
        &history,
    );

    let levels = detector
        .get_levels(TEST_SYMBOL, TEST_TIMEFRAME)
        .context("Detector did not return price levels")?;

    // ====================================================
    // GENERATE PRICE LEVEL VISUALIZATION
    // ====================================================

    let chart_path = "price_levels_chart.html";

    generate_price_levels_chart(
        chart_path,
        TEST_SYMBOL,
        TEST_TIMEFRAME,
        &bars,
        levels,
    )?;

    println!();
    println!("====================================================");
    println!("PRICE LEVEL VISUALIZATION");
    println!("====================================================");
    println!("Chart generated: {}", chart_path);

    // ====================================================
    // PRINT TEST SUMMARY
    // ====================================================

    println!();
    println!("====================================================");
    println!("PRICE LEVEL DETECTION TEST");
    println!("====================================================");

    println!("Symbol: {}", TEST_SYMBOL);

    println!(
        "Timeframe: {} seconds",
        TEST_TIMEFRAME.as_secs()
    );

    println!(
        "Test mode: {}",
        if USE_HISTORICAL_RANGE {
            "Historical Range"
        } else {
            "Latest Completed Candles"
        }
    );

    if USE_HISTORICAL_RANGE {
        println!("Start date (UTC): {}", TEST_START_DATE);
        println!("End date (UTC): {}", TEST_END_DATE);
    }

    println!("Lookback: {} hours", lookback_secs / 3600);
    println!("Candles fetched: {}", bars.len());

    println!(
        "Current price: ${:.8}",
        current_price
    );

    println!(
        "Detected levels: {}",
        levels.levels.len()
    );

    println!("Analysis timestamp: {}", current_ts);

    // ====================================================
    // PRINT DETECTED LEVELS
    // ====================================================

    for level_type in [
        PriceLevelType::Support,
        PriceLevelType::Resistance,
        PriceLevelType::SupportResistance,
    ] {

        println!();
        println!("====================================================");
        println!("{:?} LEVELS", level_type);
        println!("====================================================");

        for level in levels.levels.iter().filter(|level| {
            level.level_type == level_type
        }) {

            println!(
                "Level=${:.8} | Zone=${:.8}-${:.8} | \
                 Strength={:.2} | Touches={} | \
                 Reversal={:.3}% | Relative Volume={:.2}",
                level.price,
                level.lower_bound,
                level.upper_bound,
                level.strength,
                level.touches,
                level.average_reversal_pct,
                level.relative_volume,
            );
        }
    }

    // ====================================================
    // PRINT NEAREST SUPPORT / RESISTANCE
    // ====================================================

    println!();
    println!("====================================================");
    println!("NEAREST SUPPORT / RESISTANCE");
    println!("====================================================");

    if let Some(support) = levels.nearest_support(current_price) {
        println!(
            "Nearest Support: ${:.8} | Strength={:.2}",
            support.price,
            support.strength,
        );
    }

    if let Some(resistance) = levels.nearest_resistance(current_price) {
        println!(
            "Nearest Resistance: ${:.8} | Strength={:.2}",
            resistance.price,
            resistance.strength,
        );
    }

    println!();

    Ok(())
}






// use anyhow::{Context, Result};

// use crate::{
//     market::{
//         candle_history::CandleHistory,
//         timeframe::Timeframe,
//     },
//     toolbox::price_levels::{
//         PriceLevelConfig,
//         PriceLevelDetector,
//         PriceLevelType,
//     },
// };

// use super::hyperliquid::fetch_test_candles;

// // ====================================================
// // PRICE LEVEL DETECTION VALIDATION
// // ====================================================
// //
// // Downloads real historical candles from Hyperliquid,
// // populates CandleHistory, and runs the existing
// // PriceLevelDetector.
// //
// // Prints a single final snapshot of detected support
// // and resistance zones for manual comparison against
// // actual historical market charts.
// //
// // This is a network-dependent integration test.

// // ====================================================
// // TEST CONFIGURATION
// // ====================================================

// const TEST_SYMBOL: &str = "ZEC";

// const TEST_TIMEFRAME: Timeframe = Timeframe::FIVE_MINUTES;

// // 1 day
// // const TEST_LOOKBACK_SECS: u64 = 24 * 60 * 60; 

// // 2 days of historical candles.
// const TEST_LOOKBACK_SECS: u64 = 2 * 24 * 60 * 60;

// // 7 days of historical candles.
// // const TEST_LOOKBACK_SECS: u64 = 7 * 24 * 60 * 60;

// // ====================================================
// // TEST PRICE LEVEL DETECTION
// // ====================================================

// // cargo test test_hyperliquid_price_levels -- --nocapture

// #[tokio::test]
// async fn test_hyperliquid_price_levels() -> Result<()> {

//     // ====================================================
//     // CONFIGURE DETECTOR
//     // ====================================================

//     let config = PriceLevelConfig {
//         timeframe: TEST_TIMEFRAME,
//         lookback_secs: TEST_LOOKBACK_SECS,
//         ..PriceLevelConfig::default()
//     };

//     let required_candles = config.required_candles();

//     // Additional candles allow swing confirmation
//     // and provide a small historical buffer.

//     let candle_limit = required_candles
//         .checked_add(config.swing_strength * 2)
//         .context("Candle limit overflow")?;

//     // ====================================================
//     // FETCH HISTORICAL CANDLES
//     // ====================================================

//     let bars = fetch_test_candles(
//         TEST_SYMBOL,
//         TEST_TIMEFRAME,
//         candle_limit,
//     )
//     .await?;

//     if bars.is_empty() {
//         anyhow::bail!(
//             "No historical candles returned for {}",
//             TEST_SYMBOL
//         );
//     }

//     let current_price = bars
//         .last()
//         .context("Missing latest candle")?
//         .close;

//     let current_ts = bars
//         .last()
//         .context("Missing latest candle")?
//         .end
//         .timestamp_millis();

//     // ====================================================
//     // BUILD CANDLE HISTORY
//     // ====================================================

//     let retention_secs = TEST_LOOKBACK_SECS
//         .checked_add(TEST_TIMEFRAME.as_secs() * 10)
//         .context("History retention overflow")?;

//     let mut history = CandleHistory::new(
//         std::time::Duration::from_secs(retention_secs),
//     );

//     for bar in bars.iter().cloned() {
//         history.push(bar);
//     }

//     // ====================================================
//     // RUN PRICE LEVEL DETECTOR
//     // ====================================================

//     let mut detector = PriceLevelDetector::new(config);

//     detector.update(
//         TEST_SYMBOL,
//         TEST_TIMEFRAME,
//         current_ts,
//         &history,
//     );

//     let levels = detector
//         .get_levels(TEST_SYMBOL, TEST_TIMEFRAME)
//         .context("Detector did not return price levels")?;

//     // ====================================================
//     // PRINT TEST SUMMARY
//     // ====================================================

//     println!();
//     println!("====================================================");
//     println!("PRICE LEVEL DETECTION TEST");
//     println!("====================================================");
//     println!("Symbol: {}", TEST_SYMBOL);
//     println!("Timeframe: {} seconds", TEST_TIMEFRAME.as_secs());
//     println!("Lookback: {} hours", TEST_LOOKBACK_SECS / 3600);
//     println!("Candles fetched: {}", bars.len());
//     println!("Current price: ${:.8}", current_price);
//     println!("Detected levels: {}", levels.levels.len());
//     println!("Analysis timestamp: {}", current_ts);

//     // ====================================================
//     // PRINT DETECTED LEVELS
//     // ====================================================

//     for level_type in [
//         PriceLevelType::Support,
//         PriceLevelType::Resistance,
//         PriceLevelType::SupportResistance,
//     ] {

//         println!();
//         println!("====================================================");
//         println!("{:?} LEVELS", level_type);
//         println!("====================================================");

//         for level in levels.levels.iter().filter(|level| {
//             level.level_type == level_type
//         }) {

//             println!(
//                 "Level=${:.8} | Zone=${:.8}-${:.8} | \
//                  Strength={:.2} | Touches={} | \
//                  Reversal={:.3}% | Relative Volume={:.2}",
//                 level.price,
//                 level.lower_bound,
//                 level.upper_bound,
//                 level.strength,
//                 level.touches,
//                 level.average_reversal_pct,
//                 level.relative_volume,
//             );
//         }
//     }

//     // ====================================================
//     // PRINT NEAREST SUPPORT / RESISTANCE
//     // ====================================================

//     println!();
//     println!("====================================================");
//     println!("NEAREST SUPPORT / RESISTANCE");
//     println!("====================================================");

//     if let Some(support) = levels.nearest_support(current_price) {
//         println!(
//             "Nearest Support: ${:.8} | Strength={:.2}",
//             support.price,
//             support.strength,
//         );
//     }

//     if let Some(resistance) = levels.nearest_resistance(current_price) {
//         println!(
//             "Nearest Resistance: ${:.8} | Strength={:.2}",
//             resistance.price,
//             resistance.strength,
//         );
//     }

//     println!();

//     Ok(())
// }