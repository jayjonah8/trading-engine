use std::{
    env,
    time::Duration,
};

use chrono::{
    DateTime,
    Utc,
};

use futures_util::{
    SinkExt,
    StreamExt,
};

use serde_json::json;

use tokio::sync::mpsc::UnboundedSender;

use tokio_tungstenite::{
    connect_async,
    tungstenite::Message,
};

use crate::{
    core::events::{
        AppEvent,
        AppEventEnvelope,
        Quote,
        Trade,
    },

    market::{
        candle::CandleConfig,
        candle_manager::CandleManager,
    },
};

use crate::core::config::Config;

use crate::notifications::{
    send_remote_message,
    env_helpers::{
        get_slack_reconnect_error_channel_id,
        get_discord_reconnect_error_channel_id,
    },
};

use crate::shared::utils::{
    get_bot_name,
    get_local_timestamp,
};


/// Runs the Alpaca live market-data WebSocket feed.
///
/// The connection automatically reconnects if the WebSocket disconnects,
/// closes unexpectedly, or encounters a transport error.
///
/// Reconnection uses exponential backoff and automatically re-authenticates
/// and re-subscribes to the configured trade and quote streams.
///
/// The CandleManager is intentionally preserved across reconnects so active
/// candle state is not discarded when a temporary WebSocket interruption occurs.
///
/// The function only returns normally when the master event channel has closed.
/// Configuration errors are returned immediately because reconnecting cannot
/// resolve invalid or missing environment configuration.
pub async fn run_alpaca_feed(
    cfg: Config,
    tx_master: UnboundedSender<AppEventEnvelope>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

    // ============================================================
    // ALPACA CONFIG
    // ============================================================

    let api_key =
        env::var(
            "ALPACA_API_KEY_ID"
        )?;

    let api_secret =
        env::var(
            "ALPACA_API_SECRET_KEY"
        )?;

    let fake_mode =
        env::var(
            "ALPACA_FAKE_MODE"
        )
        .unwrap_or_else(
            |_| "false".to_string()
        )
        .parse::<bool>()?;

    let real_stream_url =
        env::var(
            "ALPACA_REAL_STREAM_URL"
        )?;

    let fake_stream_url =
        env::var(
            "ALPACA_FAKE_STREAM_URL"
        )?;


    let (
        alpaca_stream,
        symbol,
    ) =
        if fake_mode {
            (
                fake_stream_url,
                "FAKEPACA".to_string(),
            )
        } else {
            (
                real_stream_url,
                "AAPL".to_string(),
            )
        };


    println!(
        "Mode: {} | Symbol: {}",
        if fake_mode {
            "FAKE"
        } else {
            "REAL"
        },
        symbol
    );


    // ============================================================
    // CANDLE MANAGER
    // ============================================================

    // Uses the same configurable candle timeframes as the backtest.
    //
    // The CandleManager is created OUTSIDE the reconnect loop so temporary
    // WebSocket disconnects do not discard partially constructed candles.
    //
    // Uses the default candle setting timeframes and prune duration.
    // You can change this to a custom config if you want.
    //
    // Default:
    // 1-minute, 5-minute, 10-minute, 15-minute, 30-minute,
    // 1-hour, 2-hour, and 4-hour candles.
    let candle_config =
        CandleConfig::default();

    let mut candle_manager =
        CandleManager::new(
            &candle_config.timeframes
        );


    // Print configured candle timeframes in minutes.
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
        "[ALPACA MARKET] candle timeframes={:?} minutes",
        timeframe_minutes
    );


    // ============================================================
    // RECONNECT CONFIG
    // ============================================================

    // Start reconnect attempts quickly, then progressively slow them down
    // if Alpaca or the local internet connection remains unavailable.
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

        // ========================================================
        // CONNECT TO ALPACA
        // ========================================================

        println!(
            "[ALPACA MARKET] connecting | symbol={}",
            symbol
        );


        let (
            ws_stream,
            _,
        ) =
            match connect_async(
                &alpaca_stream
            )
            .await
            {
                Ok(connection) => {
                    connection
                }

                Err(error) => {
                    eprintln!(
                        "[ALPACA MARKET] connect failed | error={} | retry={}s",
                        error,
                        backoff_secs
                    );


                    let remote_message = format!(
                        "🚨 ALPACA MARKET DATA ERROR – `{}`\n\
                        WebSocket connection failed\n\
                        Feed: `AlpacaMarketData`\n\
                        Error: `{}`\n\
                        Retry: `{}s`\n\
                        Time: `{}`",
                        get_bot_name(),
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


                    tokio::time::sleep(
                        Duration::from_secs(
                            backoff_secs
                        )
                    )
                    .await;


                    backoff_secs =
                        (
                            backoff_secs * 2
                        )
                        .min(
                            max_backoff_secs
                        );


                    continue;
                }
            };


        let (
            mut write,
            mut read,
        ) =
            ws_stream.split();


        // ========================================================
        // AUTHENTICATE
        // ========================================================

        let auth =
            json!({
                "action": "auth",
                "key": api_key,
                "secret": api_secret,
            });


        if let Err(error) =
            write
                .send(
                    Message::Text(
                        auth
                            .to_string()
                            .into()
                    )
                )
                .await
        {
            eprintln!(
                "[ALPACA MARKET] auth send failed | error={} | retry={}s",
                error,
                backoff_secs
            );

            let remote_message = format!(
                "🚨 ALPACA MARKET DATA AUTH ERROR – `{}`\n\
                Authentication send failed\n\
                Feed: `AlpacaMarketData`\n\
                Error: `{}`\n\
                Retry: `{}s`\n\
                Time: `{}`",
                get_bot_name(),
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

            tokio::time::sleep(
                Duration::from_secs(
                    backoff_secs
                )
            )
            .await;


            backoff_secs =
                (
                    backoff_secs * 2
                )
                .min(
                    max_backoff_secs
                );


            continue;
        }


        // ========================================================
        // SUBSCRIBE
        // ========================================================

        let subscription =
            json!({
                "action": "subscribe",
                "trades": [&symbol],
                "quotes": [&symbol],
            });


        if let Err(error) =
            write
                .send(
                    Message::Text(
                        subscription
                            .to_string()
                            .into()
                    )
                )
                .await
        {
            eprintln!(
                "[ALPACA MARKET] subscribe send failed | error={} | retry={}s",
                error,
                backoff_secs
            );


            let remote_message = format!(
                "🚨 ALPACA MARKET DATA SUBSCRIPTION ERROR – `{}`\n\
                Subscription send failed\n\
                Feed: `AlpacaMarketData`\n\
                Symbol: `{}`\n\
                Error: `{}`\n\
                Retry: `{}s`\n\
                Time: `{}`",
                get_bot_name(),
                symbol,
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


            tokio::time::sleep(
                Duration::from_secs(
                    backoff_secs
                )
            )
            .await;


            backoff_secs =
                (
                    backoff_secs * 2
                )
                .min(
                    max_backoff_secs
                );


            continue;
        }


        // ========================================================
        // CONNECTION ESTABLISHED
        // ========================================================

        println!(
            "[ALPACA MARKET] connected and subscription sent | symbol={}",
            symbol
        );


        // The first successful connection is normal startup.
        //
        // Any successful connection after that means the market-data
        // WebSocket recovered from a previous interruption.
        if has_connected_once {
            println!(
                "[ALPACA MARKET] reconnected | symbol={}",
                symbol
            );


            let remote_message = format!(
                "✅ ALPACA MARKET DATA RECONNECTED – `{}`\n\
                Market-data WebSocket connection restored\n\
                Feed: `AlpacaMarketData`\n\
                Symbol: `{}`\n\
                Status: `Connected and subscribed`\n\
                Time: `{}`",
                get_bot_name(),
                symbol,
                get_local_timestamp(),
            );

            send_remote_message(
                &cfg,
                remote_message,
                Some(get_slack_reconnect_error_channel_id()),
                Some(get_discord_reconnect_error_channel_id()),
            );
        } else {
            has_connected_once =
                true;
        }


        // A successful connection/auth/subscription send resets the
        // reconnect delay for the next future failure.
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

                    let messages:
                        Vec<serde_json::Value> =
                        match serde_json::from_str(
                            &text
                        ) {
                            Ok(value) => {
                                value
                            }

                            Err(error) => {
                                eprintln!(
                                    "[ALPACA MARKET] JSON error: {error}"
                                );

                                continue;
                            }
                        };


                    for value in messages {
                        match value["T"].as_str() {

                            // ========================================
                            // TRADE
                            // ========================================

                            Some(
                                "t"
                            ) => {

                                let trade: Trade =
                                    match serde_json::from_value(
                                        value
                                    ) {
                                        Ok(trade) => {
                                            trade
                                        }

                                        Err(error) => {
                                            eprintln!(
                                                "[ALPACA MARKET] trade parse error: {error}"
                                            );

                                            continue;
                                        }
                                    };


                                // ====================================
                                // PARSE TRADE MARKET TIME
                                // ====================================

                                // Candle construction uses Alpaca's
                                // actual exchange trade timestamp.
                                let trade_time =
                                    match DateTime::parse_from_rfc3339(
                                        &trade.timestamp
                                    ) {
                                        Ok(timestamp) => {
                                            timestamp
                                                .with_timezone(
                                                    &Utc
                                                )
                                        }

                                        Err(error) => {
                                            eprintln!(
                                                "[ALPACA MARKET][{}] invalid trade timestamp {}: {}",
                                                trade.symbol,
                                                trade.timestamp,
                                                error,
                                            );

                                            continue;
                                        }
                                    };


                                // ====================================
                                // UPDATE LIVE CANDLES
                                // ====================================

                                // The raw live trade updates every configured timeframe.
                                //
                                // CandleManager closes completed candles first, then adds
                                // this trade to each active timeframe candle.
                                let closed_bars =
                                    candle_manager.on_trade(
                                        &trade.symbol,
                                        trade_time,
                                        trade.price,
                                        trade.size,
                                    );


                                // ====================================
                                // SEND BAR CLOSED EVENTS
                                // ====================================

                                // Only completed candles are published to the strategy.
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
                                            "[ALPACA MARKET] master channel closed"
                                        );

                                        return Ok(());
                                    }
                                }


                                // ====================================
                                // SEND NORMAL TRADE EVENT
                                // ====================================

                                // The strategy still receives every normal live trade
                                // independently of the candle system.
                                let trade_symbol =
                                    trade.symbol.clone();


                                let event =
                                    AppEventEnvelope {
                                        symbol:
                                            trade_symbol,

                                        event:
                                            AppEvent::Trade(
                                                trade
                                            ),
                                    };


                                if tx_master
                                    .send(
                                        event
                                    )
                                    .is_err()
                                {
                                    println!(
                                        "[ALPACA MARKET] master channel closed"
                                    );

                                    return Ok(());
                                }
                            }


                            // ========================================
                            // QUOTE
                            // ========================================

                            Some(
                                "q"
                            ) => {

                                let quote: Quote =
                                    match serde_json::from_value(
                                        value
                                    ) {
                                        Ok(quote) => {
                                            quote
                                        }

                                        Err(error) => {
                                            eprintln!(
                                                "[ALPACA MARKET] quote parse error: {error}"
                                            );

                                            continue;
                                        }
                                    };


                                let symbol =
                                    quote.symbol.clone();


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
                // CONNECTION CLOSED
                // ====================================================

                Ok(
                    Message::Close(
                        frame
                    )
                ) => {
                    eprintln!(
                        "[ALPACA MARKET] connection closed | frame={frame:?}"
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
                        "[ALPACA MARKET] websocket error | error={}",
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

        // Reaching this point means the read stream ended, the server sent
        // a close frame, or the WebSocket encountered an error.
        //
        // Keep the same CandleManager and tx_master alive, wait briefly,
        // then create a completely fresh Alpaca WebSocket connection.
        eprintln!(
            "[ALPACA MARKET] disconnected | reconnecting in {}s",
            backoff_secs
        );


        let remote_message = format!(
            "🔌 ALPACA MARKET DATA DISCONNECTED – `{}`\n\
            Market-data WebSocket connection lost\n\
            Feed: `AlpacaMarketData`\n\
            Action: `Reconnecting`\n\
            Retry: `{}s`\n\
            Time: `{}`",
            get_bot_name(),
            backoff_secs,
            get_local_timestamp(),
        );

        send_remote_message(
            &cfg,
            remote_message,
            Some(get_slack_reconnect_error_channel_id()),
            Some(get_discord_reconnect_error_channel_id()),
        );


        tokio::time::sleep(
            Duration::from_secs(
                backoff_secs
            )
        )
        .await;


        backoff_secs =
            (
                backoff_secs * 2
            )
            .min(
                max_backoff_secs
            );
    }
}


