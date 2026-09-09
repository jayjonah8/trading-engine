#![allow(dead_code)]

use std::time::Duration;

use chrono::{
    DateTime,
    Utc,
};

use futures_util::{
    SinkExt,
    StreamExt,
};

use serde_json::{
    json,
    Value,
};

use tokio::sync::mpsc::UnboundedSender;

use tokio_tungstenite::{
    connect_async,
    tungstenite::Message,
};

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

    market::{
        candle::CandleConfig,
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

const HYPERLIQUID_MAINNET_WS: &str =
    "wss://api.hyperliquid.xyz/ws";


/// Runs the production Hyperliquid live market-data feed.
///
/// One WebSocket connection subscribes every configured symbol to:
///
/// - `l2Book` with `fast: true` for fast bid/ask updates
/// - `trades` for public tape trades
///
/// Fast L2 updates become canonical `AppEvent::Quote` events.
///
/// Trade updates become:
///
/// - `AppEvent::TapeTrade`
/// - input into the CandleManager
/// - `AppEvent::BarClosed` when configured candles complete
///
/// The WebSocket automatically reconnects and re-subscribes after transport
/// failures, server disconnects, or unexpected stream termination.
///
/// The CandleManager intentionally lives outside the reconnect loop so active
/// candle state survives short market-data interruptions.
pub async fn run_hyperliquid_feed(
    cfg: Config,
    tx_master: UnboundedSender<AppEventEnvelope>,
    symbols: Vec<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

    if symbols.is_empty() {
        return Err(
            "Hyperliquid market-data feed requires at least one symbol"
                .into()
        );
    }


    // ============================================================
    // CANDLE MANAGER
    // ============================================================

    let candle_config =
        CandleConfig::default();

    let mut candle_manager =
        CandleManager::new(
            &candle_config.timeframes
        );


    let timeframe_minutes: Vec<u64> =
        candle_config
            .timeframes
            .iter()
            .map(
                |timeframe| {
                    timeframe.as_secs() / 60
                }
            )
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
    // RECONNECT CONFIG
    // ============================================================

    let initial_backoff_secs: u64 =
        2;

    let max_backoff_secs: u64 =
        30;

    let mut backoff_secs =
        initial_backoff_secs;

    let mut has_connected_once =
        false;


    // ============================================================
    // WEBSOCKET SUPERVISOR
    // ============================================================

    loop {

        println!(
            "[HYPERLIQUID MARKET] connecting | symbols={}",
            symbols.len()
        );


        // ========================================================
        // CONNECT
        // ========================================================

        let (
            ws_stream,
            _,
        ) =
            match connect_async(
                HYPERLIQUID_MAINNET_WS
            )
            .await
            {
                Ok(connection) => {
                    connection
                }

                Err(error) => {
                    eprintln!(
                        "[HYPERLIQUID MARKET] connect failed | error={} | retry={}s",
                        error,
                        backoff_secs
                    );


                    send_market_data_error(
                        &cfg,
                        "WebSocket connection failed",
                        &error.to_string(),
                        backoff_secs,
                    );


                    sleep_and_increase_backoff(
                        &mut backoff_secs,
                        max_backoff_secs,
                    )
                    .await;


                    continue;
                }
            };


        let (
            mut write,
            mut read,
        ) =
            ws_stream.split();


        // ========================================================
        // SUBSCRIBE
        // ========================================================

        let mut subscription_failed =
            false;


        for symbol in &symbols {

            // ----------------------------------------------------
            // FAST L2
            // ----------------------------------------------------

            let l2_subscription =
                json!({
                    "method": "subscribe",
                    "subscription": {
                        "type": "l2Book",
                        "coin": symbol,
                        "fast": true
                    }
                });


            if let Err(error) =
                write
                    .send(
                        Message::Text(
                            l2_subscription
                                .to_string()
                                .into()
                        )
                    )
                    .await
            {
                eprintln!(
                    "[HYPERLIQUID MARKET] FAST L2 subscribe failed | symbol={} | error={}",
                    symbol,
                    error
                );


                send_market_data_error(
                    &cfg,
                    &format!(
                        "FAST L2 subscription failed for {}",
                        symbol
                    ),
                    &error.to_string(),
                    backoff_secs,
                );


                subscription_failed =
                    true;

                break;
            }


            // ----------------------------------------------------
            // TRADES
            // ----------------------------------------------------

            let trades_subscription =
                json!({
                    "method": "subscribe",
                    "subscription": {
                        "type": "trades",
                        "coin": symbol
                    }
                });


            if let Err(error) =
                write
                    .send(
                        Message::Text(
                            trades_subscription
                                .to_string()
                                .into()
                        )
                    )
                    .await
            {
                eprintln!(
                    "[HYPERLIQUID MARKET] trades subscribe failed | symbol={} | error={}",
                    symbol,
                    error
                );


                send_market_data_error(
                    &cfg,
                    &format!(
                        "Trade subscription failed for {}",
                        symbol
                    ),
                    &error.to_string(),
                    backoff_secs,
                );


                subscription_failed =
                    true;

                break;
            }


            println!(
                "[HYPERLIQUID MARKET] subscribed | symbol={} | fast_l2=true | trades=true",
                symbol
            );
        }


        if subscription_failed {
            sleep_and_increase_backoff(
                &mut backoff_secs,
                max_backoff_secs,
            )
            .await;

            continue;
        }


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


            let remote_message =
                format!(
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
                Some(
                    get_slack_reconnect_error_channel_id()
                ),
                Some(
                    get_discord_reconnect_error_channel_id()
                ),
            );
        } else {
            has_connected_once =
                true;
        }


        backoff_secs =
            initial_backoff_secs;


        // ========================================================
        // RECEIVE LIVE MARKET DATA
        // ========================================================

        while let Some(message) =
            read.next().await
        {
            match message {

                // ====================================================
                // TEXT MESSAGE
                // ====================================================

                Ok(
                    Message::Text(
                        text
                    )
                ) => {

                    let value: Value =
                        match serde_json::from_str(
                            text.as_ref()
                        ) {
                            Ok(value) => {
                                value
                            }

                            Err(error) => {
                                eprintln!(
                                    "[HYPERLIQUID MARKET] JSON parse error | error={}",
                                    error
                                );

                                continue;
                            }
                        };


                    match value
                        .get("channel")
                        .and_then(Value::as_str)
                    {

                        // ============================================
                        // FAST L2 BOOK
                        // ============================================

                        Some(
                            "l2Book"
                        ) => {

                            let Some(data) =
                                value.get(
                                    "data"
                                )
                            else {
                                continue;
                            };


                            let Some(symbol) =
                                data
                                    .get(
                                        "coin"
                                    )
                                    .and_then(
                                        Value::as_str
                                    )
                            else {
                                continue;
                            };


                            let Some(timestamp_ms) =
                                data
                                    .get(
                                        "time"
                                    )
                                    .and_then(
                                        Value::as_i64
                                    )
                            else {
                                continue;
                            };


                            let Some(levels) =
                                data
                                    .get(
                                        "levels"
                                    )
                                    .and_then(
                                        Value::as_array
                                    )
                            else {
                                continue;
                            };


                            // Hyperliquid L2 layout:
                            //
                            // levels[0] = bids
                            // levels[1] = asks
                            //
                            // The first level on each side is the current BBO.
                            let best_bid =
                                levels
                                    .first()
                                    .and_then(
                                        Value::as_array
                                    )
                                    .and_then(
                                        |bids| {
                                            bids.first()
                                        }
                                    );


                            let best_ask =
                                levels
                                    .get(
                                        1
                                    )
                                    .and_then(
                                        Value::as_array
                                    )
                                    .and_then(
                                        |asks| {
                                            asks.first()
                                        }
                                    );


                            let (
                                Some(best_bid),
                                Some(best_ask),
                            ) =
                                (
                                    best_bid,
                                    best_ask,
                                )
                            else {
                                continue;
                            };


                            let bid_price =
                                parse_level_f64(
                                    best_bid,
                                    "px",
                                );


                            let bid_size =
                                parse_level_f64(
                                    best_bid,
                                    "sz",
                                );


                            let ask_price =
                                parse_level_f64(
                                    best_ask,
                                    "px",
                                );


                            let ask_size =
                                parse_level_f64(
                                    best_ask,
                                    "sz",
                                );


                            let (
                                Some(bid_price),
                                Some(bid_size),
                                Some(ask_price),
                                Some(ask_size),
                            ) =
                                (
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


                            let symbol =
                                symbol.to_string();


                            let quote =
                                Quote {
                                    symbol:
                                        symbol.clone(),

                                    bid_price,

                                    ask_price,

                                    bid_size,

                                    ask_size,

                                    timestamp_ms,
                                };


                            let event =
                                AppEventEnvelope {
                                    symbol,

                                    event:
                                        AppEvent::Quote(
                                            quote
                                        ),
                                };


                            if tx_master
                                .send(
                                    event
                                )
                                .is_err()
                            {
                                println!(
                                    "[HYPERLIQUID MARKET] master channel closed"
                                );

                                return Ok(());
                            }
                        }


                        // ============================================
                        // TAPE TRADES
                        // ============================================

                        Some(
                            "trades"
                        ) => {

                            let Some(trades) =
                                value
                                    .get(
                                        "data"
                                    )
                                    .and_then(
                                        Value::as_array
                                    )
                            else {
                                continue;
                            };


                            for trade in trades {

                                let Some(symbol) =
                                    trade
                                        .get(
                                            "coin"
                                        )
                                        .and_then(
                                            Value::as_str
                                        )
                                else {
                                    continue;
                                };


                                let Some(price) =
                                    parse_level_f64(
                                        trade,
                                        "px",
                                    )
                                else {
                                    continue;
                                };


                                let Some(size) =
                                    parse_level_f64(
                                        trade,
                                        "sz",
                                    )
                                else {
                                    continue;
                                };


                                let Some(timestamp_ms) =
                                    trade
                                        .get(
                                            "time"
                                        )
                                        .and_then(
                                            Value::as_i64
                                        )
                                else {
                                    continue;
                                };


                                if !price.is_finite()
                                    || price <= 0.0
                                    || !size.is_finite()
                                    || size <= 0.0
                                {
                                    continue;
                                }


                                // Convert the venue timestamp into chrono only
                                // for candle construction.
                                let Some(timestamp) =
                                    DateTime::<Utc>::from_timestamp_millis(
                                        timestamp_ms
                                    )
                                else {
                                    eprintln!(
                                        "[HYPERLIQUID MARKET] invalid trade timestamp | symbol={} | ts_ms={}",
                                        symbol,
                                        timestamp_ms
                                    );

                                    continue;
                                };


                                // ====================================
                                // UPDATE LIVE CANDLES
                                // ====================================

                                let closed_bars =
                                    candle_manager.on_trade(
                                        symbol,
                                        timestamp,
                                        price,
                                        size,
                                    );


                                // ====================================
                                // SEND CLOSED BARS
                                // ====================================

                                for bar in closed_bars {
                                    let symbol =
                                        bar.symbol.clone();


                                    let event =
                                        AppEventEnvelope {
                                            symbol,

                                            event:
                                                AppEvent::BarClosed(
                                                    bar
                                                ),
                                        };


                                    if tx_master
                                        .send(
                                            event
                                        )
                                        .is_err()
                                    {
                                        println!(
                                            "[HYPERLIQUID MARKET] master channel closed"
                                        );

                                        return Ok(());
                                    }
                                }


                                // ====================================
                                // SEND TAPE TRADE
                                // ====================================

                                let symbol =
                                    symbol.to_string();


                                let tape_trade =
                                    TapeTrade {
                                        symbol:
                                            symbol.clone(),

                                        price,

                                        size,

                                        timestamp_ms,
                                    };


                                let event =
                                    AppEventEnvelope {
                                        symbol,

                                        event:
                                            AppEvent::TapeTrade(
                                                tape_trade
                                            ),
                                    };


                                if tx_master
                                    .send(
                                        event
                                    )
                                    .is_err()
                                {
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

                Ok(
                    Message::Ping(
                        payload
                    )
                ) => {

                    if let Err(error) =
                        write
                            .send(
                                Message::Pong(
                                    payload
                                )
                            )
                            .await
                    {
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

                Ok(
                    Message::Close(
                        frame
                    )
                ) => {

                    eprintln!(
                        "[HYPERLIQUID MARKET] connection closed | frame={frame:?}"
                    );

                    break;
                }


                // ====================================================
                // WEBSOCKET ERROR
                // ====================================================

                Err(
                    error
                ) => {

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


        let remote_message =
            format!(
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
            Some(
                get_slack_reconnect_error_channel_id()
            ),
            Some(
                get_discord_reconnect_error_channel_id()
            ),
        );


        sleep_and_increase_backoff(
            &mut backoff_secs,
            max_backoff_secs,
        )
        .await;
    }
}


/// Parses one Hyperliquid numeric string field.
///
/// Hyperliquid sends prices and quantities as JSON strings rather than
/// JSON numbers.
fn parse_level_f64(
    value: &Value,
    field: &str,
) -> Option<f64> {
    value
        .get(
            field
        )
        .and_then(
            Value::as_str
        )
        .and_then(
            |value| {
                value
                    .parse::<f64>()
                    .ok()
            }
        )
}


/// Basic sanity validation before a quote enters the canonical event bus.
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


/// Sends a market-data failure notification through the configured
/// Slack and/or Discord notification services.
fn send_market_data_error(
    cfg: &Config,
    description: &str,
    error: &str,
    retry_secs: u64,
) {
    let remote_message =
        format!(
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
        Some(
            get_slack_reconnect_error_channel_id()
        ),
        Some(
            get_discord_reconnect_error_channel_id()
        ),
    );
}


/// Sleeps for the current reconnect delay and then increases it exponentially.
///
/// 2s -> 4s -> 8s -> 16s -> 30s -> 30s...
async fn sleep_and_increase_backoff(
    backoff_secs: &mut u64,
    max_backoff_secs: u64,
) {
    tokio::time::sleep(
        Duration::from_secs(
            *backoff_secs
        )
    )
    .await;


    *backoff_secs =
        (
            *backoff_secs * 2
        )
        .min(
            max_backoff_secs
        );
}