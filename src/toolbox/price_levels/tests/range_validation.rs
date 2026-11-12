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
        range::{
            RangeConfig,
            RangeDetector,
        },
    },
};

use super::hyperliquid::{
    fetch_test_candles,
    fetch_test_candles_range,
};

use crate::toolbox::price_levels::visualization::generate_price_ranges_chart;

// ====================================================
// HYPERLIQUID RANGE DETECTION VALIDATION
// ====================================================
//
// Downloads real historical candles from Hyperliquid,
// populates CandleHistory, and runs:
//
// 1. PriceLevelDetector
// 2. RangeDetector
//
// Supports:
// - Latest completed candles.
// - Historical UTC start/end date ranges.
//
// Prints detected trading ranges for manual validation.
//
// This is a network-dependent integration test.


// ====================================================
// TEST CONFIGURATION
// ====================================================

const TEST_SYMBOL: &str = "BTC";

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
// Start date is inclusive.
// End date is exclusive.

const TEST_START_DATE: &str = "2026-11-07 00:00:00";
const TEST_END_DATE: &str = "2026-11-09 00:00:00";


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
// TEST HYPERLIQUID RANGE DETECTION
// ====================================================

// Run:
// cargo test test_hyperliquid_ranges -- --nocapture

// html: file:///Users/jared/web3/quant-finance/trading-bots/trading-engine/price_ranges_chart.html

#[tokio::test]
async fn test_hyperliquid_ranges() -> Result<()> {

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
    // CONFIGURE PRICE LEVEL DETECTOR
    // ====================================================

    let price_level_config = PriceLevelConfig {
        timeframe: TEST_TIMEFRAME,
        lookback_secs,
        ..PriceLevelConfig::default()
    };

    let required_candles =
        price_level_config.required_candles();

    let candle_limit = required_candles
        .checked_add(price_level_config.swing_strength * 2)
        .context("Candle limit overflow")?;

    // ====================================================
    // CONFIGURE RANGE DETECTOR
    // ====================================================

    let range_config = RangeConfig {
        timeframe: TEST_TIMEFRAME,
        lookback_secs,
        ..RangeConfig::default()
    };

    // ====================================================
    // FETCH HISTORICAL CANDLES
    // ====================================================

    let bars = if let Some((start, end, _)) = historical_range {

        fetch_test_candles_range(
            TEST_SYMBOL,
            TEST_TIMEFRAME,
            start.timestamp_millis(),
            end.timestamp_millis(),
        )
        .await?

    } else {

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

    let current_ts = if USE_HISTORICAL_RANGE {

        parse_test_date(TEST_END_DATE)?
            .timestamp_millis()

    } else {

        bars.last()
            .context("Missing latest candle")?
            .end
            .timestamp_millis()
    };

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

    let mut price_level_detector =
        PriceLevelDetector::new(price_level_config);

    price_level_detector.update(
        TEST_SYMBOL,
        TEST_TIMEFRAME,
        current_ts,
        &history,
    );

    let levels = price_level_detector
        .get_levels(TEST_SYMBOL, TEST_TIMEFRAME)
        .context("PriceLevelDetector returned no results")?;

    // ====================================================
    // RUN RANGE DETECTOR
    // ====================================================

    let mut range_detector =
        RangeDetector::new(range_config);

    let ranges = range_detector
        .update(
            TEST_SYMBOL,
            TEST_TIMEFRAME,
            current_ts,
            &history,
            levels,
        )
        .context("RangeDetector returned no results")?;

    // ====================================================
    // GENERATE RANGE VISUALIZATION
    // ====================================================

    generate_price_ranges_chart(
        "price_ranges_chart.html",
        TEST_SYMBOL,
        TEST_TIMEFRAME,
        &bars,
        ranges,
    )?;

    println!("\n====================================================");
    println!("RANGE VISUALIZATION");
    println!("====================================================");
    println!("Chart generated: price_ranges_chart.html");

    // ====================================================
    // PRINT TEST SUMMARY
    // ====================================================

    println!();
    println!("====================================================");
    println!("RANGE DETECTION TEST");
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
        "Detected price levels: {}",
        levels.levels.len()
    );

    println!(
        "Detected trading ranges: {}",
        ranges.ranges.len()
    );

    println!("Analysis timestamp: {}", current_ts);

    // ====================================================
    // PRINT DETECTED RANGES
    // ====================================================

    println!();
    println!("====================================================");
    println!("DETECTED TRADING RANGES");
    println!("====================================================");

    if ranges.ranges.is_empty() {
        println!("No validated trading ranges detected.");
    }

    for (index, range) in ranges.ranges.iter().enumerate() {

        println!();
        println!("----------------------------------------------------");
        println!("Range #{}", index + 1);
        println!("----------------------------------------------------");

        println!(
            "Lower Boundary: ${:.8}",
            range.lower_boundary
        );

        println!(
            "Upper Boundary: ${:.8}",
            range.upper_boundary
        );

        println!(
            "Lowest Observed Price: ${:.8}",
            range.lowest_price
        );

        println!(
            "Highest Observed Price: ${:.8}",
            range.highest_price
        );

        println!(
            "Midpoint: ${:.8}",
            range.midpoint
        );

        println!(
            "Range Width: {:.3}%",
            range.width_pct
        );

        println!(
            "Support Strength: {:.2}",
            range.support_strength
        );

        println!(
            "Resistance Strength: {:.2}",
            range.resistance_strength
        );

        println!(
            "Support Touches: {}",
            range.support_touches
        );

        println!(
            "Resistance Touches: {}",
            range.resistance_touches
        );

        println!(
            "Completed Oscillations: {}",
            range.completed_oscillations
        );

        println!(
            "Price Containment: {:.2}%",
            range.containment_pct
        );

        println!(
            "Range Strength: {:.2}",
            range.strength
        );

        // ====================================================
        // RANGE STATUS
        // ====================================================

        println!(
            "Range Status: {:?}",
            range.status
        );

        // ====================================================
        // RANGE TIMESTAMPS
        // ====================================================

        println!(
            "Start Timestamp: {}",
            range.start_ts
        );

        println!(
            "End Timestamp: {}",
            range.end_ts
        );

        println!(
            "Confirmed Timestamp: {}",
            range.confirmed_ts
        );

        println!(
            "Last Boundary Touch: {}",
            range.last_touch_ts
        );

        // ====================================================
        // CURRENT PRICE POSITION
        // ====================================================

        let width =
            range.upper_boundary - range.lower_boundary;

        if width > 0.0 {

            let position_pct =
                ((current_price - range.lower_boundary)
                    / width)
                    * 100.0;

            println!(
                "Current Position: {:.2}%",
                position_pct
            );

            println!(
                "Current Price Inside Range: {}",
                current_price >= range.lower_boundary
                    && current_price <= range.upper_boundary
            );
        }
    }

    println!();

    Ok(())
}





