#![allow(dead_code)]

use futures_util::{
    SinkExt,
    StreamExt,
};

use serde_json::{
    Value,
};

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

    exchange::hyperliquid::market_data_ws::{
        connect_hyperliquid_market_data_ws,
        sleep_and_increase_backoff,
        INITIAL_BACKOFF_SECS,
    },

    market::{
        candle::CandleConfig,
        candle_feed::{
            CandleFeedError,
            process_trade_for_candles,
        },
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
// RUN HYPERLIQUID MARKET DATA FEED
// ============================================================

/// Runs the production Hyperliquid live market-data feed.
///
/// One WebSocket subscribes every configured symbol to:
///
/// - fast `l2Book` updates for bid/ask data
/// - public `trades` for tape data
///
/// Market-data flow:
///
/// fast L2 -> `AppEvent::Quote`
///
/// public trade -> `AppEvent::TapeTrade`
///              -> shared CandleManager
///              -> `AppEvent::BarClosed`
///
/// The WebSocket automatically reconnects and restores every subscription
/// after connection failures.
///
/// The CandleManager intentionally lives outside the reconnect loop so
/// partially constructed candles survive temporary WebSocket interruptions.
pub async fn run_hyperliquid_feed(
    cfg: Config,
    tx_master: UnboundedSender<AppEventEnvelope>,
    symbols: Vec<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

    if symbols.is_empty() {
        return Err(
            "Hyperliquid market-data feed requires at least one symbol".into()
        );
    }


    // ============================================================
    // CANDLE MANAGER
    // ============================================================

    let candle_config = CandleConfig::default();
    let mut candle_manager = CandleManager::new(&candle_config.timeframes);

    let timeframe_minutes: Vec<u64> = candle_config.timeframes
        .iter()
        .map(|timeframe| timeframe.as_secs() / 60)
        .collect();


    println!(
        "[HYPERLIQUID MARKET] symbols={:?}",
        symbols
    );

    println!(
        "[HYPERLIQUID MARKET] candle timeframes={:?} minutes",
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
            "[HYPERLIQUID MARKET] connecting | symbols={}",
            symbols.len()
        );


        // ========================================================
        // CONNECT + SUBSCRIBE
        // ========================================================

        let ws_stream = match connect_hyperliquid_market_data_ws(&symbols).await {
            Ok(ws_stream) => ws_stream,

            Err(error) => {
                eprintln!(
                    "[HYPERLIQUID MARKET] connection/setup failed | error={} | retry={}s",
                    error,
                    backoff_secs
                );


                send_market_data_error(
                    &cfg,
                    "WebSocket connection or subscription failed",
                    &error.to_string(),
                    backoff_secs,
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
            "[HYPERLIQUID MARKET] connected and subscribed | symbols={}",
            symbols.len()
        );


        if has_connected_once {
            println!(
                "[HYPERLIQUID MARKET] reconnected | symbols={}",
                symbols.len()
            );


            let remote_message = format!(
                "✅ HYPERLIQUID MARKET DATA RECONNECTED – `{}`\n\
                Market-data WebSocket connection restored\n\
                Feed: `HyperliquidFastL2+Trades`\n\
                Symbols: `{}`\n\
                Status: `Connected and subscribed`\n\
                Time: `{}`",
                get_bot_name(),
                symbols.len(),
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


        // A successful connection resets future reconnect delay.
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

                    let value: Value = match serde_json::from_str(text.as_ref()) {
                        Ok(value) => value,

                        Err(error) => {
                            eprintln!(
                                "[HYPERLIQUID MARKET] JSON parse error | error={}",
                                error
                            );

                            continue;
                        }
                    };


                    match value.get("channel").and_then(Value::as_str) {

                        // ============================================
                        // FAST L2 BOOK
                        // ============================================

                        Some("l2Book") => {

                            let Some(data) = value.get("data") else {
                                continue;
                            };


                            let Some(symbol) = data.get("coin").and_then(Value::as_str) else {
                                continue;
                            };


                            let Some(timestamp_ms) = data.get("time").and_then(Value::as_i64) else {
                                continue;
                            };


                            let Some(levels) = data.get("levels").and_then(Value::as_array) else {
                                continue;
                            };


                            // Hyperliquid L2 layout:
                            //
                            // levels[0] = bids
                            // levels[1] = asks
                            //
                            // The first level on each side is the BBO.
                            let best_bid = levels
                                .first()
                                .and_then(Value::as_array)
                                .and_then(|bids| bids.first());


                            let best_ask = levels
                                .get(1)
                                .and_then(Value::as_array)
                                .and_then(|asks| asks.first());


                            let (Some(best_bid), Some(best_ask)) = (best_bid, best_ask) else {
                                continue;
                            };


                            let bid_price = parse_level_f64(best_bid, "px");
                            let bid_size = parse_level_f64(best_bid, "sz");
                            let ask_price = parse_level_f64(best_ask, "px");
                            let ask_size = parse_level_f64(best_ask, "sz");


                            let (
                                Some(bid_price),
                                Some(bid_size),
                                Some(ask_price),
                                Some(ask_size),
                            ) = (
                                bid_price,
                                bid_size,
                                ask_price,
                                ask_size,
                            )
                            else {
                                continue;
                            };


                            if !valid_quote(
                                bid_price,
                                ask_price,
                                bid_size,
                                ask_size,
                            ) {
                                eprintln!(
                                    "[HYPERLIQUID MARKET] invalid quote | symbol={} | bid={} | ask={} | bid_size={} | ask_size={}",
                                    symbol,
                                    bid_price,
                                    ask_price,
                                    bid_size,
                                    ask_size,
                                );

                                continue;
                            }


                            let symbol = symbol.to_string();

                            let quote = Quote {
                                symbol: symbol.clone(),
                                bid_price,
                                ask_price,
                                bid_size,
                                ask_size,
                                timestamp_ms,
                            };


                            let event = AppEventEnvelope {
                                symbol,
                                event: AppEvent::Quote(quote),
                            };


                            if tx_master.send(event).is_err() {
                                println!(
                                    "[HYPERLIQUID MARKET] master channel closed"
                                );

                                return Ok(());
                            }
                        }


                        // ============================================
                        // PUBLIC TAPE TRADES
                        // ============================================

                        Some("trades") => {

                            let Some(trades) = value.get("data").and_then(Value::as_array) else {
                                continue;
                            };


                            for trade in trades {

                                let Some(symbol) = trade.get("coin").and_then(Value::as_str) else {
                                    continue;
                                };


                                let Some(price) = parse_level_f64(trade, "px") else {
                                    continue;
                                };


                                let Some(size) = parse_level_f64(trade, "sz") else {
                                    continue;
                                };


                                let Some(timestamp_ms) = trade.get("time").and_then(Value::as_i64) else {
                                    continue;
                                };


                                if !price.is_finite()
                                    || price <= 0.0
                                    || !size.is_finite()
                                    || size <= 0.0
                                {
                                    continue;
                                }


                                // ====================================
                                // UPDATE SHARED LIVE CANDLES
                                // ====================================

                                match process_trade_for_candles(
                                    &mut candle_manager,
                                    &tx_master,
                                    symbol,
                                    timestamp_ms,
                                    price,
                                    size,
                                ) {
                                    Ok(()) => {}

                                    Err(CandleFeedError::InvalidTimestamp) => {
                                        eprintln!(
                                            "[HYPERLIQUID MARKET] invalid trade timestamp | symbol={} | ts_ms={}",
                                            symbol,
                                            timestamp_ms
                                        );

                                        continue;
                                    }

                                    Err(CandleFeedError::MasterChannelClosed) => {
                                        println!(
                                            "[HYPERLIQUID MARKET] master channel closed"
                                        );

                                        return Ok(());
                                    }
                                }


                                // ====================================
                                // SEND CANONICAL TAPE TRADE
                                // ====================================

                                let symbol = symbol.to_string();

                                let tape_trade = TapeTrade {
                                    symbol: symbol.clone(),
                                    price,
                                    size,
                                    timestamp_ms,
                                };


                                let event = AppEventEnvelope {
                                    symbol,
                                    event: AppEvent::TapeTrade(tape_trade),
                                };


                                if tx_master.send(event).is_err() {
                                    println!(
                                        "[HYPERLIQUID MARKET] master channel closed"
                                    );

                                    return Ok(());
                                }
                            }
                        }


                        // ============================================
                        // OTHER HYPERLIQUID MESSAGE
                        // ============================================

                        _ => {}
                    }
                }


                // ====================================================
                // PING
                // ====================================================

                Ok(Message::Ping(payload)) => {

                    if let Err(error) = write.send(Message::Pong(payload)).await {
                        eprintln!(
                            "[HYPERLIQUID MARKET] pong failed | error={}",
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
                        "[HYPERLIQUID MARKET] connection closed | frame={frame:?}"
                    );

                    break;
                }


                // ====================================================
                // WEBSOCKET ERROR
                // ====================================================

                Err(error) => {
                    eprintln!(
                        "[HYPERLIQUID MARKET] websocket error | error={}",
                        error
                    );

                    break;
                }


                // ====================================================
                // OTHER WEBSOCKET MESSAGE
                // ====================================================

                _ => {}
            }
        }


        // ============================================================
        // CONNECTION LOST
        // ============================================================

        eprintln!(
            "[HYPERLIQUID MARKET] disconnected | reconnecting in {}s",
            backoff_secs
        );


        let remote_message = format!(
            "🔌 HYPERLIQUID MARKET DATA DISCONNECTED – `{}`\n\
            Market-data WebSocket connection lost\n\
            Feed: `HyperliquidFastL2+Trades`\n\
            Symbols: `{}`\n\
            Action: `Reconnecting`\n\
            Retry: `{}s`\n\
            Time: `{}`",
            get_bot_name(),
            symbols.len(),
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


// ============================================================
// PARSE HYPERLIQUID NUMBER
// ============================================================

/// Hyperliquid sends prices and quantities as JSON strings.
fn parse_level_f64(
    value: &Value,
    field: &str,
) -> Option<f64> {
    value
        .get(field)
        .and_then(Value::as_str)
        .and_then(|value| value.parse::<f64>().ok())
}


// ============================================================
// VALIDATE QUOTE
// ============================================================

/// Basic validation before a quote enters the canonical event bus.
fn valid_quote(
    bid_price: f64,
    ask_price: f64,
    bid_size: f64,
    ask_size: f64,
) -> bool {
    bid_price.is_finite()
        && ask_price.is_finite()
        && bid_size.is_finite()
        && ask_size.is_finite()
        && bid_price > 0.0
        && ask_price > 0.0
        && bid_size >= 0.0
        && ask_size >= 0.0
        && bid_price <= ask_price
}


// ============================================================
// SEND MARKET DATA ERROR
// ============================================================

/// Sends a Hyperliquid market-data failure through the configured
/// Slack and/or Discord notification services.
fn send_market_data_error(
    cfg: &Config,
    description: &str,
    error: &str,
    retry_secs: u64,
) {
    let remote_message = format!(
        "🚨 HYPERLIQUID MARKET DATA ERROR – `{}`\n\
        {}\n\
        Feed: `HyperliquidFastL2+Trades`\n\
        Error: `{}`\n\
        Retry: `{}s`\n\
        Time: `{}`",
        get_bot_name(),
        description,
        error,
        retry_secs,
        get_local_timestamp(),
    );


    send_remote_message(
        cfg,
        remote_message,
        Some(get_slack_reconnect_error_channel_id()),
        Some(get_discord_reconnect_error_channel_id()),
    );
}