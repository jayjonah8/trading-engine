#![allow(dead_code)]

use std::env;

use chrono::DateTime;

use futures_util::{
    SinkExt,
    StreamExt,
};

use serde::Deserialize;

use tokio::sync::mpsc::UnboundedSender;

use tokio_tungstenite::tungstenite::Message;

use crate::{
    core::{
        config::Config,
        events::{
            AppEvent,
            AppEventEnvelope,
            Quote,
            TapeTrade,
        },
    },

    data::live::alpaca::market_data_ws::{
        connect_alpaca_market_data_ws,
        sleep_and_increase_backoff,
        INITIAL_BACKOFF_SECS,
    },

    market::{
        candle::CandleConfig,
        candle_feed::process_trade_for_candles,
        candle_manager::CandleManager,
    },

    notifications::{
        env_helpers::{
            get_discord_reconnect_error_channel_id,
            get_slack_reconnect_error_channel_id,
        },
        send_remote_message,
    },

    shared::utils::{
        get_bot_name,
        get_local_timestamp,
    },
};


// ============================================================
// ALPACA MARKET DATA WIRE TYPES
// ============================================================

/// Raw Alpaca trade message received from the market-data WebSocket.
///
/// This type exists only at the Alpaca boundary. It is immediately
/// normalized into the venue-independent `TapeTrade`.
#[derive(Debug, Deserialize)]
struct AlpacaTradeMessage {
    #[serde(rename = "S")]
    symbol: String,

    #[serde(rename = "p")]
    price: f64,

    #[serde(rename = "s")]
    size: u64,

    #[serde(rename = "t")]
    timestamp: String,
}

/// Raw Alpaca quote message received from the market-data WebSocket.
///
/// This type exists only at the Alpaca boundary. It is immediately
/// normalized into the venue-independent `Quote`.
#[derive(Debug, Deserialize)]
struct AlpacaQuoteMessage {
    #[serde(rename = "S")]
    symbol: String,

    #[serde(rename = "bp")]
    bid_price: f64,

    #[serde(rename = "ap")]
    ask_price: f64,

    #[serde(rename = "bs")]
    bid_size: u64,

    #[serde(rename = "as")]
    ask_size: u64,

    #[serde(rename = "t")]
    timestamp: String,
}


// ============================================================
// RUN ALPACA LIVE MARKET DATA FEED
// ============================================================

/// Runs the production Alpaca live market-data feed.
///
/// This supervisor owns the lifetime of the Alpaca trades/quotes WebSocket.
///
/// Market data and execution updates intentionally use separate WebSocket
/// connections because either connection can fail or reconnect independently.
///
/// Raw Alpaca messages are normalized into canonical `TapeTrade` and `Quote`
/// events before entering the application event bus.
///
/// Every raw live trade also passes through the shared live candle path so
/// all configured candle timeframes are built from the complete trade stream.
///
/// The `CandleManager` is intentionally created outside the reconnect loop.
/// Temporary WebSocket interruptions therefore do not discard active candle
/// state.
///
/// If the market-data WebSocket disconnects, closes, or encounters a transport
/// error, a completely fresh connection is created and subscriptions are
/// restored automatically.
pub async fn run_alpaca_feed(
    cfg: Config,
    tx_master: UnboundedSender<AppEventEnvelope>,
    symbols: Vec<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

    // ============================================================
    // ALPACA CONFIG
    // ============================================================

    let api_key = env::var("ALPACA_API_KEY_ID")?;
    let api_secret = env::var("ALPACA_API_SECRET_KEY")?;

    let fake_mode = env::var("ALPACA_FAKE_MODE")
        .unwrap_or_else(|_| "false".to_string())
        .parse::<bool>()?;

    let real_stream_url = env::var("ALPACA_REAL_STREAM_URL")?;
    let fake_stream_url = env::var("ALPACA_FAKE_STREAM_URL")?;

    let (alpaca_stream, symbols) = if fake_mode {
        (fake_stream_url, vec!["FAKEPACA".to_string()])
    } else {
        (real_stream_url, symbols)
    };

    if symbols.is_empty() {
        return Err("Alpaca market-data feed requires at least one symbol".into());
    }

    println!(
        "[ALPACA MARKET] mode={} | symbols={:?}",
        if fake_mode { "FAKE" } else { "REAL" },
        symbols,
    );


    // ============================================================
    // CANDLE MANAGER
    // ============================================================

    // One CandleManager can maintain independent candle state for all
    // subscribed symbols and configured timeframes.
    let candle_config = CandleConfig::default();
    let mut candle_manager = CandleManager::new(&candle_config.timeframes);

    let timeframe_minutes: Vec<u64> = candle_config
        .timeframes
        .iter()
        .map(|timeframe| timeframe.as_secs() / 60)
        .collect();

    println!(
        "[ALPACA MARKET] candle timeframes={:?} minutes",
        timeframe_minutes
    );


    // ============================================================
    // RECONNECT STATE
    // ============================================================

    let mut backoff_secs = INITIAL_BACKOFF_SECS;
    let mut has_connected_once = false;


    // ============================================================
    // WEBSOCKET SUPERVISOR
    // ============================================================

    loop {
        println!(
            "[ALPACA MARKET] connecting | symbols={:?}",
            symbols
        );


        // ========================================================
        // CONNECT + AUTHENTICATE + SUBSCRIBE
        // ========================================================

        let ws_stream =
            match connect_alpaca_market_data_ws(
                &alpaca_stream,
                &api_key,
                &api_secret,
                &symbols,
            )
            .await
            {
                Ok(ws_stream) => ws_stream,

                Err(error) => {
                    eprintln!(
                        "[ALPACA MARKET] connection startup failed | error={} | retry={}s",
                        error,
                        backoff_secs
                    );

                    let remote_message = format!(
                        "🚨 ALPACA MARKET DATA ERROR – `{}`\n\
                        Market-data WebSocket startup failed\n\
                        Feed: `AlpacaMarketData`\n\
                        Symbols: `{:?}`\n\
                        Error: `{}`\n\
                        Retry: `{}s`\n\
                        Time: `{}`",
                        get_bot_name(),
                        symbols,
                        error,
                        backoff_secs,
                        get_local_timestamp(),
                    );

                    send_remote_message(
                        &cfg,
                        remote_message,
                        Some(get_slack_reconnect_error_channel_id()),
                        Some(get_discord_reconnect_error_channel_id()),
                    );

                    sleep_and_increase_backoff(
                        &mut backoff_secs
                    )
                    .await;

                    continue;
                }
            };


        let (mut write, mut read) = ws_stream.split();


        // ========================================================
        // CONNECTION ESTABLISHED
        // ========================================================

        println!(
            "[ALPACA MARKET] connected and subscribed | symbols={:?}",
            symbols
        );

        if has_connected_once {
            println!(
                "[ALPACA MARKET] reconnected | symbols={:?}",
                symbols
            );

            let remote_message = format!(
                "✅ ALPACA MARKET DATA RECONNECTED – `{}`\n\
                Market-data WebSocket connection restored\n\
                Feed: `AlpacaMarketData`\n\
                Symbols: `{:?}`\n\
                Status: `Connected and subscribed`\n\
                Time: `{}`",
                get_bot_name(),
                symbols,
                get_local_timestamp(),
            );

            send_remote_message(
                &cfg,
                remote_message,
                Some(get_slack_reconnect_error_channel_id()),
                Some(get_discord_reconnect_error_channel_id()),
            );
        } else {
            has_connected_once = true;
        }

        backoff_secs = INITIAL_BACKOFF_SECS;


        // ========================================================
        // RECEIVE LIVE MARKET DATA
        // ========================================================

        while let Some(message) = read.next().await {
            match message {

                // ====================================================
                // TEXT MESSAGE
                // ====================================================

                Ok(Message::Text(text)) => {
                    let messages: Vec<serde_json::Value> =
                        match serde_json::from_str(&text) {
                            Ok(messages) => messages,

                            Err(error) => {
                                eprintln!(
                                    "[ALPACA MARKET] JSON parse error | error={}",
                                    error
                                );

                                continue;
                            }
                        };

                    for value in messages {
                        match value["T"].as_str() {

                            // ========================================
                            // TRADE
                            // ========================================

                            Some("t") => {
                                let alpaca_trade: AlpacaTradeMessage =
                                    match serde_json::from_value(value) {
                                        Ok(trade) => trade,

                                        Err(error) => {
                                            eprintln!(
                                                "[ALPACA MARKET] trade parse error | error={}",
                                                error
                                            );

                                            continue;
                                        }
                                    };


                                // ====================================
                                // EXCHANGE TIMESTAMP
                                // ====================================

                                let timestamp_ms =
                                    match DateTime::parse_from_rfc3339(
                                        &alpaca_trade.timestamp
                                    ) {
                                        Ok(timestamp) => {
                                            timestamp.timestamp_millis()
                                        }

                                        Err(error) => {
                                            eprintln!(
                                                "[ALPACA MARKET][{}] invalid trade timestamp {} | error={}",
                                                alpaca_trade.symbol,
                                                alpaca_trade.timestamp,
                                                error,
                                            );

                                            continue;
                                        }
                                    };


                                // ====================================
                                // VALIDATE TRADE
                                // ====================================

                                let size = alpaca_trade.size as f64;

                                if !alpaca_trade.price.is_finite()
                                    || alpaca_trade.price <= 0.0
                                    || !size.is_finite()
                                    || size <= 0.0
                                {
                                    eprintln!(
                                        "[ALPACA MARKET] invalid trade | symbol={} | price={} | size={}",
                                        alpaca_trade.symbol,
                                        alpaca_trade.price,
                                        size,
                                    );

                                    continue;
                                }


                                // ====================================
                                // UPDATE LIVE CANDLES
                                // ====================================

                                // Uses every raw live trade before any strategy
                                // processing so OHLCV remains full-resolution.
                                process_trade_for_candles(
                                    &mut candle_manager,
                                    &tx_master,
                                    &alpaca_trade.symbol,
                                    timestamp_ms,
                                    alpaca_trade.price,
                                    size,
                                )?;


                                // ====================================
                                // PUBLISH CANONICAL TAPE TRADE
                                // ====================================

                                let tape_trade = TapeTrade {
                                    symbol: alpaca_trade.symbol.clone(),
                                    price: alpaca_trade.price,
                                    size,
                                    timestamp_ms,
                                };

                                let event = AppEventEnvelope {
                                    symbol: alpaca_trade.symbol,
                                    event: AppEvent::TapeTrade(tape_trade),
                                };

                                if tx_master.send(event).is_err() {
                                    println!(
                                        "[ALPACA MARKET] master channel closed"
                                    );

                                    return Ok(());
                                }
                            }


                            // ========================================
                            // QUOTE
                            // ========================================

                            Some("q") => {
                                let alpaca_quote: AlpacaQuoteMessage =
                                    match serde_json::from_value(value) {
                                        Ok(quote) => quote,

                                        Err(error) => {
                                            eprintln!(
                                                "[ALPACA MARKET] quote parse error | error={}",
                                                error
                                            );

                                            continue;
                                        }
                                    };


                                // ====================================
                                // EXCHANGE TIMESTAMP
                                // ====================================

                                let timestamp_ms =
                                    match DateTime::parse_from_rfc3339(
                                        &alpaca_quote.timestamp
                                    ) {
                                        Ok(timestamp) => {
                                            timestamp.timestamp_millis()
                                        }

                                        Err(error) => {
                                            eprintln!(
                                                "[ALPACA MARKET][{}] invalid quote timestamp {} | error={}",
                                                alpaca_quote.symbol,
                                                alpaca_quote.timestamp,
                                                error,
                                            );

                                            continue;
                                        }
                                    };


                                // ====================================
                                // NORMALIZE QUOTE
                                // ====================================

                                let quote = Quote {
                                    symbol: alpaca_quote.symbol.clone(),
                                    bid_price: alpaca_quote.bid_price,
                                    ask_price: alpaca_quote.ask_price,
                                    bid_size: alpaca_quote.bid_size as f64,
                                    ask_size: alpaca_quote.ask_size as f64,
                                    timestamp_ms,
                                };


                                // ====================================
                                // VALIDATE QUOTE
                                // ====================================

                                if !quote.bid_price.is_finite()
                                    || !quote.ask_price.is_finite()
                                    || !quote.bid_size.is_finite()
                                    || !quote.ask_size.is_finite()
                                    || quote.bid_price <= 0.0
                                    || quote.ask_price <= 0.0
                                    || quote.bid_size < 0.0
                                    || quote.ask_size < 0.0
                                    || quote.ask_price < quote.bid_price
                                {
                                    eprintln!(
                                        "[ALPACA MARKET] invalid quote | symbol={} | bid={} | ask={} | bid_size={} | ask_size={}",
                                        quote.symbol,
                                        quote.bid_price,
                                        quote.ask_price,
                                        quote.bid_size,
                                        quote.ask_size,
                                    );

                                    continue;
                                }


                                // ====================================
                                // PUBLISH CANONICAL QUOTE
                                // ====================================

                                let event = AppEventEnvelope {
                                    symbol: alpaca_quote.symbol,
                                    event: AppEvent::Quote(quote),
                                };

                                if tx_master.send(event).is_err() {
                                    println!(
                                        "[ALPACA MARKET] master channel closed"
                                    );

                                    return Ok(());
                                }
                            }


                            // ========================================
                            // OTHER ALPACA MESSAGE
                            // ========================================

                            _ => {}
                        }
                    }
                }


                // ====================================================
                // PING
                // ====================================================

                Ok(Message::Ping(payload)) => {
                    if let Err(error) =
                        write.send(
                            Message::Pong(payload)
                        )
                        .await
                    {
                        eprintln!(
                            "[ALPACA MARKET] pong failed | error={}",
                            error
                        );

                        break;
                    }
                }


                // ====================================================
                // CONNECTION CLOSED
                // ====================================================

                Ok(Message::Close(frame)) => {
                    eprintln!(
                        "[ALPACA MARKET] connection closed | frame={frame:?}"
                    );

                    break;
                }


                // ====================================================
                // WEBSOCKET ERROR
                // ====================================================

                Err(error) => {
                    eprintln!(
                        "[ALPACA MARKET] websocket error | error={}",
                        error
                    );

                    break;
                }


                // ====================================================
                // OTHER WEBSOCKET FRAME
                // ====================================================

                _ => {}
            }
        }


        // ============================================================
        // CONNECTION LOST
        // ============================================================

        eprintln!(
            "[ALPACA MARKET] disconnected | reconnecting in {}s",
            backoff_secs
        );

        let remote_message = format!(
            "🔌 ALPACA MARKET DATA DISCONNECTED – `{}`\n\
            Market-data WebSocket connection lost\n\
            Feed: `AlpacaMarketData`\n\
            Symbols: `{:?}`\n\
            Action: `Reconnecting`\n\
            Retry: `{}s`\n\
            Time: `{}`",
            get_bot_name(),
            symbols,
            backoff_secs,
            get_local_timestamp(),
        );

        send_remote_message(
            &cfg,
            remote_message,
            Some(get_slack_reconnect_error_channel_id()),
            Some(get_discord_reconnect_error_channel_id()),
        );

        sleep_and_increase_backoff(
            &mut backoff_secs
        )
        .await;
    }
}