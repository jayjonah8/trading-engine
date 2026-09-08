#![allow(dead_code)]

use crate::backtest::types::{
    AlpacaBarsResponse,
    AlpacaTradesResponse,
    HistoricalBar,
    HistoricalTrade,
};

use std::time::Duration;
use tokio::time::sleep;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct HistoricalFetchConfig {
    // Maximum number of trades Alpaca should return in a single API request/page.
    //
    // Keeping this high means fewer total API calls are required when downloading
    // large historical datasets.
    //
    // Example:
    // 50,000 trades at 10,000/page = roughly 5 API requests.
    pub page_limit: usize,

    // How long to wait before requesting the next page from Alpaca.
    //
    // This throttles our requests so a large backtest doesn't hammer the API
    // and run into Alpaca's request-rate limits.
    //
    // 750ms = at most ~80 requests/minute from this fetch loop.
    pub delay_between_requests: Duration,

    // Minimum amount of historical time that must pass before we keep
    // another trade for the SAME symbol.
    //
    // Example:
    // Duration::from_millis(100)
    //
    // AAPL @ 13:30:00.000 -> keep
    // AAPL @ 13:30:00.020 -> remove
    // AAPL @ 13:30:00.080 -> remove
    // AAPL @ 13:30:00.101 -> keep
    //
    // Trades from OTHER symbols are handled independently.
    pub min_trade_interval: Duration,

}

impl Default for HistoricalFetchConfig {
    fn default() -> Self {
        Self {
            page_limit: 10_000,
            delay_between_requests: Duration::from_millis(250),
            min_trade_interval: Duration::from_millis(300),
        }
    }
}

// Fetches historical trade-level price data from Alpaca for multiple assets.
// Automatically handles pagination and delays between API requests, then
// sorts and downsamples the trades into one chronological backtest dataset.
pub async fn fetch_historical_trades(
    symbols: &[&str],
    start: &str,
    end: &str,
    fetch_config: &HistoricalFetchConfig,
) -> Result<Vec<HistoricalTrade>, Box<dyn std::error::Error>> {
    let api_key: String = std::env::var("ALPACA_API_KEY_ID")?;
    let api_secret = std::env::var("ALPACA_API_SECRET_KEY")?;

    let symbols = symbols.join(",");
    let client = reqwest::Client::new();

    let mut all_trades = Vec::new();
    let mut next_page_token: Option<String> = None;
    let mut page_number = 0usize;

    loop {
        page_number += 1;

        let limit = fetch_config.page_limit.to_string();

        let mut request = client
            .get("https://data.alpaca.markets/v2/stocks/trades")
            .header("APCA-API-KEY-ID", &api_key)
            .header("APCA-API-SECRET-KEY", &api_secret)
            .query(&[
                ("symbols", symbols.as_str()),
                ("start", start),
                ("end", end),
                ("feed", "iex"),
                ("limit", limit.as_str()),
            ]);

        if let Some(token) = &next_page_token {
            request = request.query(&[
                ("page_token", token.as_str()),
            ]);
        }

        let response = request
            .send()
            .await?
            .error_for_status()?
            .json::<AlpacaTradesResponse>()
            .await?;

        let mut page_trade_count = 0usize;

        for (symbol, symbol_trades) in response.trades {
            page_trade_count += symbol_trades.len();

            for trade in symbol_trades {
                all_trades.push(HistoricalTrade {
                    symbol: symbol.clone(),
                    timestamp: trade.timestamp,
                    price: trade.price,
                    size: trade.size,
                });
            }
        }

        println!(
            "Fetched page {} | {} trades | total: {}",
            page_number,
            page_trade_count,
            all_trades.len()
        );

        next_page_token = response.next_page_token;

        if next_page_token.is_none() {
            break;
        }

        sleep(fetch_config.delay_between_requests).await;
    }

    // Alpaca's multi-symbol response is not guaranteed to already be
    // globally ordered across every symbol, so sort all trades first.
    all_trades.sort_by_key(|trade| trade.timestamp);

    Ok(all_trades)
}

// Reduces dense raw trade data using fixed time windows for each symbol.
// Only the last trade from each window is kept, preserving the latest price
// while greatly reducing the number of events the backtest must process.
pub(crate) fn downsample_trades(
    trades: Vec<HistoricalTrade>,
    interval: Duration,
) -> Vec<HistoricalTrade> {
    // A zero-sized interval means no downsampling.
    if interval.is_zero() {
        return trades;
    }

    let interval_ns = interval.as_nanos();

    // Stores the currently-active bucket for each symbol.
    //
    // Example:
    //
    // "AAPL" -> (bucket 12345, last AAPL trade)
    // "TSLA" -> (bucket 12345, last TSLA trade)
    //
    // As newer trades arrive inside the same bucket, we simply replace
    // the stored trade so the LAST trade in that bucket survives.
    let mut active_buckets:
        HashMap<String, (i128, HistoricalTrade)> = HashMap::new();

    let mut filtered = Vec::with_capacity(trades.len());

    for trade in trades {
        // Convert the historical timestamp into nanoseconds since Unix epoch.
        //
        // We use this only to determine which fixed time bucket
        // the trade belongs to. The actual HistoricalTrade keeps
        // its original precise timestamp.
        let timestamp_ns =
            (trade.timestamp.timestamp() as i128 * 1_000_000_000)
                + trade.timestamp.timestamp_subsec_nanos() as i128;

        let bucket_id = timestamp_ns / interval_ns as i128;

        match active_buckets.get_mut(&trade.symbol) {
            Some((current_bucket_id, current_trade)) => {
                if *current_bucket_id == bucket_id {
                    // Same symbol + same time bucket.
                    //
                    // Replace the previous trade so we keep the LAST
                    // trade that occurred during this window.
                    *current_trade = trade;
                } else {
                    // This symbol has entered a new time bucket.
                    //
                    // The previously stored trade is therefore the LAST
                    // trade from the previous bucket, so keep it.
                    let previous_trade =
                        std::mem::replace(current_trade, trade);

                    filtered.push(previous_trade);

                    *current_bucket_id = bucket_id;
                }
            }

            None => {
                // First trade we've seen for this symbol.
                active_buckets.insert(
                    trade.symbol.clone(),
                    (bucket_id, trade),
                );
            }
        }
    }

    // Every symbol will still have its final active bucket stored in
    // the HashMap, so add those final trades to the output.
    for (_, (_, trade)) in active_buckets {
        filtered.push(trade);
    }

    // Because each symbol is downsampled independently, pushing completed
    // buckets can temporarily disturb global ordering.
    //
    // Sort again so the BacktestEngine receives one globally chronological
    // market-event stream across every asset.
    filtered.sort_by_key(|trade| trade.timestamp);

    filtered
}

// Fetches historical 1-minute OHLCV candle data from Alpaca for
// multiple symbols. The returned bars are converted into our internal
// HistoricalBar type and sorted into chronological order across all assets.
pub async fn get_historical_bars(
    symbols: &[&str],
    start: &str,
    end: &str,
) -> Result<Vec<HistoricalBar>, Box<dyn std::error::Error>> {
    let api_key = std::env::var("ALPACA_API_KEY_ID")?;
    let api_secret = std::env::var("ALPACA_API_SECRET_KEY")?;

    let symbols = symbols.join(",");

    let url = format!(
        "https://data.alpaca.markets/v2/stocks/bars\
        ?symbols={}\
        &timeframe=1Min\
        &start={}\
        &end={}\
        &feed=iex\
        &limit=10000",
        symbols,
        start,
        end
    );

    let client = reqwest::Client::new();

    let response = client
        .get(&url)
        .header("APCA-API-KEY-ID", api_key)
        .header("APCA-API-SECRET-KEY", api_secret)
        .send()
        .await?
        .error_for_status()?
        .json::<AlpacaBarsResponse>()
        .await?;

    let mut bars = Vec::new();

    for (symbol, symbol_bars) in response.bars {
        for bar in symbol_bars {
            bars.push(HistoricalBar {
                symbol: symbol.clone(),
                timestamp: bar.timestamp,
                open: bar.open,
                high: bar.high,
                low: bar.low,
                close: bar.close,
                volume: bar.volume,
            });
        }
    }

    bars.sort_by_key(|bar| bar.timestamp);

    Ok(bars)
}