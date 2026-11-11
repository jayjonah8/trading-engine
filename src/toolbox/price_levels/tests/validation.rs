use anyhow::{Context, Result};

use crate::{
    market::{
        candle_history::CandleHistory,
        timeframe::Timeframe,
    },
    toolbox::price_levels::{
        PriceLevelConfig,
        PriceLevelDetector,
        PriceLevelType,
    },
};

use super::hyperliquid::fetch_test_candles;

// ====================================================
// PRICE LEVEL DETECTION VALIDATION
// ====================================================
//
// Downloads real historical candles from Hyperliquid,
// populates CandleHistory, and runs the existing
// PriceLevelDetector.
//
// Prints a single final snapshot of detected support
// and resistance zones for manual comparison against
// actual historical market charts.
//
// This is a network-dependent integration test.

// ====================================================
// TEST CONFIGURATION
// ====================================================

const TEST_SYMBOL: &str = "BTC";

const TEST_TIMEFRAME: Timeframe = Timeframe::FIVE_MINUTES;

const TEST_LOOKBACK_SECS: u64 = 24 * 60 * 60;

// ====================================================
// TEST PRICE LEVEL DETECTION
// ====================================================

// cargo test test_hyperliquid_price_levels -- --nocapture

#[tokio::test]
async fn test_hyperliquid_price_levels() -> Result<()> {

    // ====================================================
    // CONFIGURE DETECTOR
    // ====================================================

    let config = PriceLevelConfig {
        timeframe: TEST_TIMEFRAME,
        lookback_secs: TEST_LOOKBACK_SECS,
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

    let bars = fetch_test_candles(
        TEST_SYMBOL,
        TEST_TIMEFRAME,
        candle_limit,
    )
    .await?;

    if bars.is_empty() {
        anyhow::bail!(
            "No historical candles returned for {}",
            TEST_SYMBOL
        );
    }

    let current_price = bars
        .last()
        .context("Missing latest candle")?
        .close;

    let current_ts = bars
        .last()
        .context("Missing latest candle")?
        .end
        .timestamp_millis();

    // ====================================================
    // BUILD CANDLE HISTORY
    // ====================================================

    let retention_secs = TEST_LOOKBACK_SECS
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
    // PRINT TEST SUMMARY
    // ====================================================

    println!();
    println!("====================================================");
    println!("PRICE LEVEL DETECTION TEST");
    println!("====================================================");
    println!("Symbol: {}", TEST_SYMBOL);
    println!("Timeframe: {} seconds", TEST_TIMEFRAME.as_secs());
    println!("Lookback: {} hours", TEST_LOOKBACK_SECS / 3600);
    println!("Candles fetched: {}", bars.len());
    println!("Current price: ${:.8}", current_price);
    println!("Detected levels: {}", levels.levels.len());
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