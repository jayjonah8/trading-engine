use crate::data::historical::alpaca::fetch::fetch_historical_bars;


// Tests Alpaca's historical bars API by fetching 1-minute OHLCV data
// for multiple symbols and verifying that historical bars are returned.
//
// Prints the first 30 bars so timestamps, symbols, OHLC prices,
// and volume can be manually inspected.
//
// cargo test test_get_alpaca_historical_data -- --nocapture
#[tokio::test]
async fn test_get_alpaca_historical_data() {
    dotenvy::dotenv().ok();

    let bars = fetch_historical_bars(
        &["AAPL", "TSLA", "NVDA"],
        "2026-08-03T13:30:00Z",
        "2026-08-03T14:00:00Z",
    )
    .await
    .expect("Failed to get historical bars");

    println!("Received {} bars", bars.len());

    for bar in bars.iter().take(30) {
        println!(
            "{} | {} | O: {:.2} H: {:.2} L: {:.2} C: {:.2} V: {}",
            bar.timestamp,
            bar.symbol,
            bar.open,
            bar.high,
            bar.low,
            bar.close,
            bar.volume
        );
    }

    assert!(!bars.is_empty());
}