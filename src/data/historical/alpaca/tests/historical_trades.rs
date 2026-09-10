use std::time::Duration;

use crate::data::historical::alpaca::fetch::{
    fetch_historical_trades,
    HistoricalFetchConfig,
};


// Tests Alpaca's historical trades API by fetching trade-level data
// for multiple symbols over a multi-day period.
//
// Uses a custom pagination configuration and prints the first 100
// trades so timestamps, symbols, prices, and sizes can be inspected.
//
// cargo test test_get_alpaca_historical_trades -- --nocapture
#[tokio::test]
async fn test_get_alpaca_historical_trades() {
    dotenvy::dotenv().ok();

    // used the default setting
    // let fetch_config = HistoricalFetchConfig::default();

    // Uses custom settings for the test
    // - up to 10,000 trades per API request
    // - waits 750ms between paginated requests
    let fetch_config = HistoricalFetchConfig {
        page_limit: 10_000,
        delay_between_requests: Duration::from_millis(500),
        min_trade_interval: Duration::from_millis(300),
    };

    let trades = fetch_historical_trades(
        &["AAPL", "MSFT", "NVDA"],
        "2026-08-03T13:30:00Z",
        "2026-08-04T20:00:00Z",
        &fetch_config,
    )
    .await
    .expect("Failed to get historical trades");

    println!("Received {} total trades", trades.len());

    for trade in trades.iter().take(100) {
        println!(
            "{} | {} | P: {:.4} | S: {}",
            trade.timestamp,
            trade.symbol,
            trade.price,
            trade.size
        );
    }

    assert!(!trades.is_empty());
}