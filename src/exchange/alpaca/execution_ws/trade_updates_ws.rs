#![allow(dead_code)]

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;

use tokio::{
    net::TcpStream,
    sync::{
        mpsc::UnboundedSender,
        watch,
    },
};

use tokio_tungstenite::{
    connect_async,
    tungstenite::Message,
    MaybeTlsStream,
    WebSocketStream,
};

use crate::{
    core::config::Config,

    exchange::alpaca::{
        execution_ws::health::ExecutionWsHealth,
        types::AlpacaConfig,
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
// WEBSOCKET TYPE
// ============================================================

pub(crate) type AlpacaTradeUpdatesWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;


// ============================================================
// RECONNECT CONFIG
// ============================================================

pub(crate) const INITIAL_BACKOFF_SECS: u64 = 2;
pub(crate) const MAX_BACKOFF_SECS: u64 = 10;


// ============================================================
// RUN ALPACA TRADE UPDATES WEBSOCKET
// ============================================================

/// Runs the Alpaca `trade_updates` WebSocket supervisor.
///
/// This connection is independent from Alpaca's market-data WebSocket.
///
/// The supervisor:
///
/// 1. Connects to the correct paper/live trading endpoint.
/// 2. Authenticates.
/// 3. Waits for authorization confirmation.
/// 4. Subscribes to `trade_updates`.
/// 5. Waits for subscription confirmation.
/// 6. Marks execution WebSocket health ready.
/// 7. Forwards raw `trade_updates` messages.
/// 8. Marks execution WebSocket health unavailable on disconnect.
/// 9. Reconnects automatically with exponential backoff.
///
/// The connection is NOT considered ready until authentication and
/// `trade_updates` subscription have both been confirmed by Alpaca.
///
/// Reconnection:
///
/// `2s -> 4s -> 8s -> 10s -> 10s...`
pub(crate) async fn run_alpaca_trade_updates_ws(
    cfg: Config,
    alpaca_config: AlpacaConfig,
    tx_updates: UnboundedSender<Value>,
    health_tx: watch::Sender<ExecutionWsHealth>,
) -> anyhow::Result<()> {
    let mut backoff_secs = INITIAL_BACKOFF_SECS;
    let mut has_connected_once = false;

    loop {
        // ========================================================
        // CONNECTING
        // ========================================================

        set_trade_updates_ready(&health_tx, false);

        println!(
            "[ALPACA EXECUTION WS] connecting | paper={}",
            alpaca_config.paper_trading
        );


        // ========================================================
        // CONNECT + AUTHENTICATE + SUBSCRIBE
        // ========================================================

        let ws_stream = match connect_alpaca_trade_updates_ws(&alpaca_config).await {
            Ok(ws_stream) => ws_stream,

            Err(error) => {
                set_trade_updates_ready(&health_tx, false);

                eprintln!(
                    "[ALPACA EXECUTION WS] startup failed | error={} | retry={}s",
                    error,
                    backoff_secs
                );

                send_trade_updates_error(
                    &cfg,
                    "trade_updates WebSocket startup failed",
                    &error.to_string(),
                    backoff_secs,
                );

                sleep_and_increase_backoff(&mut backoff_secs).await;

                continue;
            }
        };


        let (mut write, mut read) = ws_stream.split();


        // ========================================================
        // CONNECTION READY
        // ========================================================

        // connect_alpaca_trade_updates_ws() only returns after Alpaca has
        // confirmed both authentication and the trade_updates subscription.
        set_trade_updates_ready(&health_tx, true);

        println!(
            "[ALPACA EXECUTION WS] connected and subscribed | stream=trade_updates"
        );


        if has_connected_once {
            println!(
                "[ALPACA EXECUTION WS] reconnected | stream=trade_updates"
            );

            let remote_message = format!(
                "✅ ALPACA EXECUTION WS RECONNECTED – `{}`\n\
                Execution WebSocket connection restored\n\
                Stream: `trade_updates`\n\
                Status: `Connected and subscribed`\n\
                Time: `{}`",
                get_bot_name(),
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


        // Successful startup resets reconnect delay.
        backoff_secs = INITIAL_BACKOFF_SECS;


        // ========================================================
        // RECEIVE TRADE UPDATES
        // ========================================================

        while let Some(message) = read.next().await {
            match message {

                // ====================================================
                // TEXT
                // ====================================================

                Ok(Message::Text(text)) => {
                    let value: Value = match serde_json::from_str(text.as_ref()) {
                        Ok(value) => value,

                        Err(error) => {
                            eprintln!(
                                "[ALPACA EXECUTION WS] JSON parse error | error={}",
                                error
                            );

                            continue;
                        }
                    };


                    if value
                        .get("stream")
                        .and_then(Value::as_str)
                        != Some("trade_updates")
                    {
                        continue;
                    }


                    if tx_updates.send(value).is_err() {
                        set_trade_updates_ready(&health_tx, false);

                        println!(
                            "[ALPACA EXECUTION WS] update channel closed"
                        );

                        return Ok(());
                    }
                }


                // ====================================================
                // BINARY
                // ====================================================

                Ok(Message::Binary(bytes)) => {
                    let text = match String::from_utf8(bytes.to_vec()) {
                        Ok(text) => text,

                        Err(error) => {
                            eprintln!(
                                "[ALPACA EXECUTION WS] invalid binary UTF-8 | error={}",
                                error
                            );

                            continue;
                        }
                    };


                    let value: Value = match serde_json::from_str(&text) {
                        Ok(value) => value,

                        Err(error) => {
                            eprintln!(
                                "[ALPACA EXECUTION WS] binary JSON parse error | error={}",
                                error
                            );

                            continue;
                        }
                    };


                    if value
                        .get("stream")
                        .and_then(Value::as_str)
                        != Some("trade_updates")
                    {
                        continue;
                    }


                    if tx_updates.send(value).is_err() {
                        set_trade_updates_ready(&health_tx, false);

                        println!(
                            "[ALPACA EXECUTION WS] update channel closed"
                        );

                        return Ok(());
                    }
                }


                // ====================================================
                // PING
                // ====================================================

                Ok(Message::Ping(payload)) => {
                    if let Err(error) = write.send(Message::Pong(payload)).await {
                        eprintln!(
                            "[ALPACA EXECUTION WS] pong failed | error={}",
                            error
                        );

                        break;
                    }
                }


                // ====================================================
                // PONG
                // ====================================================

                Ok(Message::Pong(_)) => {}


                // ====================================================
                // CONNECTION CLOSED
                // ====================================================

                Ok(Message::Close(frame)) => {
                    eprintln!(
                        "[ALPACA EXECUTION WS] connection closed | frame={frame:?}"
                    );

                    break;
                }


                // ====================================================
                // WEBSOCKET ERROR
                // ====================================================

                Err(error) => {
                    eprintln!(
                        "[ALPACA EXECUTION WS] websocket error | error={}",
                        error
                    );

                    break;
                }


                // ====================================================
                // OTHER FRAME
                // ====================================================

                _ => {}
            }
        }


        // ========================================================
        // CONNECTION LOST
        // ========================================================

        set_trade_updates_ready(&health_tx, false);

        eprintln!(
            "[ALPACA EXECUTION WS] disconnected | reconnecting in {}s",
            backoff_secs
        );


        let remote_message = format!(
            "🔌 ALPACA EXECUTION WS DISCONNECTED – `{}`\n\
            Execution WebSocket connection lost\n\
            Stream: `trade_updates`\n\
            Action: `Reconnecting`\n\
            Retry: `{}s`\n\
            Warning: `New live orders should remain blocked until execution monitoring is restored`\n\
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


        sleep_and_increase_backoff(&mut backoff_secs).await;
    }
}


// ============================================================
// SET TRADE UPDATES HEALTH
// ============================================================

/// Updates the readiness state of Alpaca's execution WebSocket.
fn set_trade_updates_ready(
    health_tx: &watch::Sender<ExecutionWsHealth>,
    ready: bool,
) {
    health_tx.send_modify(|health| {
        health.trade_updates_ready = ready;
    });
}


// ============================================================
// CONNECT ALPACA TRADE UPDATES WEBSOCKET
// ============================================================

/// Creates a fresh authenticated and subscribed Alpaca `trade_updates`
/// WebSocket connection.
///
/// This function only returns after Alpaca confirms:
///
/// - successful authentication
/// - an active `trade_updates` subscription
///
/// Therefore a successful return means execution monitoring is ready.
pub(crate) async fn connect_alpaca_trade_updates_ws(
    config: &AlpacaConfig,
) -> anyhow::Result<AlpacaTradeUpdatesWebSocket> {
    // ============================================================
    // WEBSOCKET URL
    // ============================================================

    let ws_url = if config.paper_trading {
        "wss://paper-api.alpaca.markets/stream"
    } else {
        "wss://api.alpaca.markets/stream"
    };


    // ============================================================
    // CONNECT
    // ============================================================

    let (mut ws_stream, _) = tokio::time::timeout(
        Duration::from_secs(10),
        connect_async(ws_url),
    )
    .await
    .map_err(|_| {
        anyhow::anyhow!(
            "Timed out connecting to Alpaca trade_updates WebSocket"
        )
    })??;


    // ============================================================
    // AUTHENTICATE
    // ============================================================

    let auth = serde_json::json!({
        "action": "auth",
        "key": config.api_key,
        "secret": config.api_secret,
    });


    ws_stream
        .send(Message::Text(auth.to_string().into()))
        .await?;


    // ============================================================
    // WAIT FOR AUTHORIZATION
    // ============================================================

    wait_for_authorization(&mut ws_stream).await?;


    // ============================================================
    // SUBSCRIBE TO TRADE UPDATES
    // ============================================================

    let listen = serde_json::json!({
        "action": "listen",
        "data": {
            "streams": [
                "trade_updates"
            ]
        }
    });


    ws_stream
        .send(Message::Text(listen.to_string().into()))
        .await?;


    // ============================================================
    // WAIT FOR SUBSCRIPTION
    // ============================================================

    wait_for_trade_updates_subscription(&mut ws_stream).await?;


    Ok(ws_stream)
}


// ============================================================
// WAIT FOR AUTHORIZATION
// ============================================================

async fn wait_for_authorization(
    ws_stream: &mut AlpacaTradeUpdatesWebSocket,
) -> anyhow::Result<()> {
    tokio::time::timeout(
        Duration::from_secs(10),
        async {
            loop {
                let message = read_websocket_json(ws_stream).await?;


                let authorized =
                    message.get("stream").and_then(Value::as_str)
                        == Some("authorization")
                    &&
                    message
                        .get("data")
                        .and_then(|data| data.get("status"))
                        .and_then(Value::as_str)
                        == Some("authorized");


                if authorized {
                    return Ok(());
                }


                let unauthorized =
                    message.get("stream").and_then(Value::as_str)
                        == Some("authorization")
                    &&
                    message
                        .get("data")
                        .and_then(|data| data.get("status"))
                        .and_then(Value::as_str)
                        == Some("unauthorized");


                if unauthorized {
                    anyhow::bail!(
                        "Alpaca trade_updates authentication failed: {}",
                        message
                    );
                }
            }
        },
    )
    .await
    .map_err(|_| {
        anyhow::anyhow!(
            "Timed out waiting for Alpaca trade_updates authentication"
        )
    })?
}


// ============================================================
// WAIT FOR TRADE UPDATES SUBSCRIPTION
// ============================================================

async fn wait_for_trade_updates_subscription(
    ws_stream: &mut AlpacaTradeUpdatesWebSocket,
) -> anyhow::Result<()> {
    tokio::time::timeout(
        Duration::from_secs(10),
        async {
            loop {
                let message = read_websocket_json(ws_stream).await?;


                let listening =
                    message.get("stream").and_then(Value::as_str)
                        == Some("listening");


                let subscribed = message
                    .get("data")
                    .and_then(|data| data.get("streams"))
                    .and_then(Value::as_array)
                    .map(|streams| {
                        streams.iter().any(|stream| {
                            stream.as_str() == Some("trade_updates")
                        })
                    })
                    .unwrap_or(false);


                if listening && subscribed {
                    return Ok(());
                }
            }
        },
    )
    .await
    .map_err(|_| {
        anyhow::anyhow!(
            "Timed out waiting for Alpaca trade_updates subscription"
        )
    })?
}


// ============================================================
// READ WEBSOCKET JSON
// ============================================================

async fn read_websocket_json(
    ws_stream: &mut AlpacaTradeUpdatesWebSocket,
) -> anyhow::Result<Value> {
    loop {
        let message = ws_stream
            .next()
            .await
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Alpaca trade_updates WebSocket closed during startup"
                )
            })??;


        match message {
            Message::Text(text) => {
                return Ok(
                    serde_json::from_str(text.as_ref())?
                );
            }

            Message::Binary(bytes) => {
                let text = String::from_utf8(bytes.to_vec())?;

                return Ok(
                    serde_json::from_str(&text)?
                );
            }

            Message::Ping(payload) => {
                ws_stream
                    .send(Message::Pong(payload))
                    .await?;
            }

            Message::Pong(_) => {
                continue;
            }

            Message::Close(frame) => {
                anyhow::bail!(
                    "Alpaca trade_updates WebSocket closed during startup: {:?}",
                    frame
                );
            }

            _ => {
                continue;
            }
        }
    }
}


// ============================================================
// SEND TRADE UPDATES ERROR
// ============================================================

fn send_trade_updates_error(
    cfg: &Config,
    description: &str,
    error: &str,
    retry_secs: u64,
) {
    let remote_message = format!(
        "🚨 ALPACA EXECUTION WS ERROR – `{}`\n\
        {}\n\
        Stream: `trade_updates`\n\
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


// ============================================================
// RECONNECT BACKOFF
// ============================================================

/// `2s -> 4s -> 8s -> 10s -> 10s...`
async fn sleep_and_increase_backoff(
    backoff_secs: &mut u64,
) {
    tokio::time::sleep(
        Duration::from_secs(*backoff_secs)
    )
    .await;


    *backoff_secs = (*backoff_secs * 2).min(MAX_BACKOFF_SECS);
}