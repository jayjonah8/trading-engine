#![allow(dead_code)]

use std::time::Duration;

use tokio::time::sleep;

use crate::data::historical::{
    alpaca::types::{
        AlpacaBarsResponse,
        AlpacaTradesResponse,
    },

    types::{
        HistoricalBar,
        HistoricalTrade,
    },
};


// ============================================================
// HISTORICAL FETCH CONFIG
// ============================================================

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
    // This throttles requests so large historical downloads do not hammer
    // Alpaca's API or run into request-rate limits.
    pub delay_between_requests: Duration,
}

impl Default for HistoricalFetchConfig {
    fn default() -> Self {
        Self {
            page_limit: 10_000,
            delay_between_requests: Duration::from_millis(250),
        }
    }
}


// ============================================================
// FETCH HISTORICAL TRADES
// ============================================================

/// Fetches historical trade-level market data from Alpaca for multiple symbols.
///
/// Pagination is handled automatically.
///
/// All returned trades are normalized into the shared `HistoricalTrade` type
/// and globally sorted by exchange timestamp across every requested symbol.
///
/// Downsampling is intentionally NOT performed here. That belongs to the
/// provider-independent backtest layer.
pub async fn fetch_historical_trades(
    symbols: &[&str],
    start: &str,
    end: &str,
    fetch_config: &HistoricalFetchConfig,
) -> Result<Vec<HistoricalTrade>, Box<dyn std::error::Error>> {
    let api_key = std::env::var("ALPACA_API_KEY_ID")?;
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
                all_trades.push(
                    HistoricalTrade {
                        symbol: symbol.clone(),
                        timestamp: trade.timestamp,
                        price: trade.price,
                        size: trade.size,
                    }
                );
            }
        }

        println!(
            "[ALPACA HISTORICAL] fetched page={} | trades={} | total={}",
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
    // globally ordered across every symbol.
    all_trades.sort_by_key(
        |trade| trade.timestamp
    );

    Ok(all_trades)
}


// ============================================================
// FETCH HISTORICAL BARS
// ============================================================

/// Fetches historical 1-minute OHLCV bars from Alpaca.
///
/// Returned bars are normalized into the shared `HistoricalBar` type
/// and globally sorted by timestamp across every requested symbol.
pub async fn fetch_historical_bars(
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
            bars.push(
                HistoricalBar {
                    symbol: symbol.clone(),
                    timestamp: bar.timestamp,
                    open: bar.open,
                    high: bar.high,
                    low: bar.low,
                    close: bar.close,
                    volume: bar.volume,
                }
            );
        }
    }

    bars.sort_by_key(
        |bar| bar.timestamp
    );

    Ok(bars)
}