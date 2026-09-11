#![allow(dead_code)]

use std::time::Duration;

use futures_util::{
    SinkExt,
    StreamExt,
};

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
    MaybeTlsStream,
    WebSocketStream,
    tungstenite::Message,
};

use crate::{
    core::config::Config,

    exchange::alpaca::types::{
        AlpacaConfig,
        TradeUpdatesConnectionState,
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

pub(crate) type AlpacaTradeUpdatesWebSocket =
    WebSocketStream<MaybeTlsStream<TcpStream>>;


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
/// This connection is intentionally independent from the Alpaca market-data
/// WebSocket. Market data and execution updates can disconnect and recover
/// independently without affecting each other's lifecycle.
///
/// The supervisor:
///
/// 1. Connects to the correct paper or live Alpaca trading endpoint.
/// 2. Authenticates.
/// 3. Waits for Alpaca to confirm authorization.
/// 4. Subscribes to `trade_updates`.
/// 5. Waits for Alpaca to confirm the subscription.
/// 6. Marks execution monitoring as `Connected`.
/// 7. Forwards raw `trade_updates` JSON messages to the execution adapter.
/// 8. Marks execution monitoring as `Disconnected` if the stream is lost.
/// 9. Reconnects automatically using exponential backoff.
///
/// This module intentionally does NOT translate Alpaca execution messages into
/// engine `ExecEvent`s. Venue-specific execution mapping and order correlation
/// belong in the Alpaca adapter/mapping layer.
///
/// Reconnection uses exponential backoff:
///
/// `2s -> 4s -> 8s -> 10s -> 10s...`
pub(crate) async fn run_alpaca_trade_updates_ws(
    cfg: Config,
    alpaca_config: AlpacaConfig,
    tx_updates: UnboundedSender<Value>,
    tx_connection_state: watch::Sender<TradeUpdatesConnectionState>,
) -> anyhow::Result<()> {
    let mut backoff_secs = INITIAL_BACKOFF_SECS;
    let mut has_connected_once = false;

    loop {

        // ========================================================
        // CONNECTING
        // ========================================================

        tx_connection_state.send_replace(
            TradeUpdatesConnectionState::Connecting
        );

        println!(
            "[ALPACA EXECUTION WS] connecting | paper={}",
            alpaca_config.paper_trading
        );


        // ========================================================
        // CONNECT + AUTHENTICATE + SUBSCRIBE
        // ========================================================

        let ws_stream =
            match connect_alpaca_trade_updates_ws(
                &alpaca_config
            )
            .await
            {
                Ok(ws_stream) => {
                    ws_stream
                }

                Err(error) => {
                    tx_connection_state.send_replace(
                        TradeUpdatesConnectionState::Disconnected
                    );

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

                    sleep_and_increase_backoff(
                        &mut backoff_secs
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
        // CONNECTION ESTABLISHED
        // ========================================================

        tx_connection_state.send_replace(
            TradeUpdatesConnectionState::Connected
        );

        println!(
            "[ALPACA EXECUTION WS] connected and subscribed | stream=trade_updates"
        );


        if has_connected_once {
            println!(
                "[ALPACA EXECUTION WS] reconnected | stream=trade_updates"
            );

            let remote_message =
                format!(
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
                Some(
                    get_slack_reconnect_error_channel_id()
                ),
                Some(
                    get_discord_reconnect_error_channel_id()
                ),
            );
        } else {
            has_connected_once = true;
        }


        // Successful startup resets reconnect delay.
        backoff_secs = INITIAL_BACKOFF_SECS;


        // ========================================================
        // RECEIVE TRADE UPDATES
        // ========================================================

        while let Some(message) =
            read.next().await
        {
            match message {

                // ====================================================
                // TEXT
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
                                    "[ALPACA EXECUTION WS] JSON parse error | error={}",
                                    error
                                );

                                continue;
                            }
                        };


                    // Ignore unexpected non-trade_updates control messages
                    // after startup.
                    if value
                        .get("stream")
                        .and_then(Value::as_str)
                        != Some("trade_updates")
                    {
                        continue;
                    }


                    if tx_updates
                        .send(value)
                        .is_err()
                    {
                        tx_connection_state.send_replace(
                            TradeUpdatesConnectionState::Disconnected
                        );

                        println!(
                            "[ALPACA EXECUTION WS] update channel closed"
                        );

                        return Ok(());
                    }
                }


                // ====================================================
                // BINARY
                // ====================================================

                Ok(
                    Message::Binary(
                        bytes
                    )
                ) => {
                    let text =
                        match String::from_utf8(
                            bytes.to_vec()
                        ) {
                            Ok(text) => {
                                text
                            }

                            Err(error) => {
                                eprintln!(
                                    "[ALPACA EXECUTION WS] invalid binary UTF-8 | error={}",
                                    error
                                );

                                continue;
                            }
                        };


                    let value: Value =
                        match serde_json::from_str(
                            &text
                        ) {
                            Ok(value) => {
                                value
                            }

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


                    if tx_updates
                        .send(value)
                        .is_err()
                    {
                        tx_connection_state.send_replace(
                            TradeUpdatesConnectionState::Disconnected
                        );

                        println!(
                            "[ALPACA EXECUTION WS] update channel closed"
                        );

                        return Ok(());
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
                            "[ALPACA EXECUTION WS] pong failed | error={}",
                            error
                        );

                        break;
                    }
                }


                // ====================================================
                // PONG
                // ====================================================

                Ok(
                    Message::Pong(
                        _
                    )
                ) => {}


                // ====================================================
                // CONNECTION CLOSED
                // ====================================================

                Ok(
                    Message::Close(
                        frame
                    )
                ) => {
                    eprintln!(
                        "[ALPACA EXECUTION WS] connection closed | frame={frame:?}"
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


        // ============================================================
        // CONNECTION LOST
        // ============================================================

        tx_connection_state.send_replace(
            TradeUpdatesConnectionState::Disconnected
        );

        eprintln!(
            "[ALPACA EXECUTION WS] disconnected | reconnecting in {}s",
            backoff_secs
        );


        let remote_message =
            format!(
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
            Some(
                get_slack_reconnect_error_channel_id()
            ),
            Some(
                get_discord_reconnect_error_channel_id()
            ),
        );


        sleep_and_increase_backoff(
            &mut backoff_secs
        )
        .await;
    }
}


// ============================================================
// CONNECT ALPACA TRADE UPDATES WEBSOCKET
// ============================================================

/// Creates a fresh authenticated and subscribed Alpaca `trade_updates`
/// WebSocket connection.
///
/// The function only returns after Alpaca has confirmed:
///
/// - successful authentication
/// - an active `trade_updates` subscription
///
/// This ensures the execution adapter never considers the stream healthy
/// before Alpaca has actually confirmed that execution monitoring is active.
///
/// Reconnection policy does not live here. The supervisor above calls this
/// function again whenever a completely fresh connection is required.
pub(crate) async fn connect_alpaca_trade_updates_ws(
    config: &AlpacaConfig,
) -> anyhow::Result<AlpacaTradeUpdatesWebSocket> {

    // ============================================================
    // WEBSOCKET URL
    // ============================================================

    let ws_url =
        if config.paper_trading {
            "wss://paper-api.alpaca.markets/stream"
        } else {
            "wss://api.alpaca.markets/stream"
        };


    // ============================================================
    // CONNECT
    // ============================================================

    let (
        mut ws_stream,
        _,
    ) =
        tokio::time::timeout(
            Duration::from_secs(
                10
            ),
            connect_async(
                ws_url
            ),
        )
        .await
        .map_err(
            |_| {
                anyhow::anyhow!(
                    "Timed out connecting to Alpaca trade_updates WebSocket"
                )
            }
        )??;


    // ============================================================
    // AUTHENTICATE
    // ============================================================

    let auth =
        serde_json::json!({
            "action": "auth",
            "key": config.api_key,
            "secret": config.api_secret,
        });


    ws_stream
        .send(
            Message::Text(
                auth
                    .to_string()
                    .into()
            )
        )
        .await?;


    // ============================================================
    // WAIT FOR AUTHORIZATION
    // ============================================================

    wait_for_authorization(
        &mut ws_stream
    )
    .await?;


    // ============================================================
    // SUBSCRIBE TO TRADE UPDATES
    // ============================================================

    let listen =
        serde_json::json!({
            "action": "listen",
            "data": {
                "streams": [
                    "trade_updates"
                ]
            }
        });


    ws_stream
        .send(
            Message::Text(
                listen
                    .to_string()
                    .into()
            )
        )
        .await?;


    // ============================================================
    // WAIT FOR SUBSCRIPTION
    // ============================================================

    wait_for_trade_updates_subscription(
        &mut ws_stream
    )
    .await?;


    Ok(
        ws_stream
    )
}


// ============================================================
// WAIT FOR AUTHORIZATION
// ============================================================

/// Waits until Alpaca confirms that the trading WebSocket has been
/// successfully authenticated.
///
/// Other control messages are ignored while waiting.
///
/// An explicit unauthorized response, socket failure, or timeout is returned
/// to the supervisor as an error.
async fn wait_for_authorization(
    ws_stream: &mut AlpacaTradeUpdatesWebSocket,
) -> anyhow::Result<()> {
    tokio::time::timeout(
        Duration::from_secs(
            10
        ),
        async {
            loop {
                let message =
                    read_websocket_json(
                        ws_stream
                    )
                    .await?;


                let authorized =
                    message
                        .get("stream")
                        .and_then(Value::as_str)
                        == Some("authorization")
                    &&
                    message
                        .get("data")
                        .and_then(
                            |data| {
                                data.get(
                                    "status"
                                )
                            }
                        )
                        .and_then(Value::as_str)
                        == Some("authorized");


                if authorized {
                    return Ok(());
                }


                let unauthorized =
                    message
                        .get("stream")
                        .and_then(Value::as_str)
                        == Some("authorization")
                    &&
                    message
                        .get("data")
                        .and_then(
                            |data| {
                                data.get(
                                    "status"
                                )
                            }
                        )
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
    .map_err(
        |_| {
            anyhow::anyhow!(
                "Timed out waiting for Alpaca trade_updates authentication"
            )
        }
    )?
}


// ============================================================
// WAIT FOR TRADE UPDATES SUBSCRIPTION
// ============================================================

/// Waits until Alpaca confirms that the `trade_updates` stream is active.
///
/// The connection is not considered usable until the subscription
/// acknowledgement explicitly contains `trade_updates`.
async fn wait_for_trade_updates_subscription(
    ws_stream: &mut AlpacaTradeUpdatesWebSocket,
) -> anyhow::Result<()> {
    tokio::time::timeout(
        Duration::from_secs(
            10
        ),
        async {
            loop {
                let message =
                    read_websocket_json(
                        ws_stream
                    )
                    .await?;


                let listening =
                    message
                        .get("stream")
                        .and_then(Value::as_str)
                        == Some("listening");


                let subscribed =
                    message
                        .get("data")
                        .and_then(
                            |data| {
                                data.get(
                                    "streams"
                                )
                            }
                        )
                        .and_then(Value::as_array)
                        .map(
                            |streams| {
                                streams
                                    .iter()
                                    .any(
                                        |stream| {
                                            stream.as_str()
                                                == Some("trade_updates")
                                        }
                                    )
                            }
                        )
                        .unwrap_or(
                            false
                        );


                if listening && subscribed {
                    return Ok(());
                }
            }
        },
    )
    .await
    .map_err(
        |_| {
            anyhow::anyhow!(
                "Timed out waiting for Alpaca trade_updates subscription"
            )
        }
    )?
}


// ============================================================
// READ WEBSOCKET JSON
// ============================================================

/// Reads the next usable Alpaca trading WebSocket message.
///
/// Text and binary frames are parsed as JSON.
///
/// Ping frames are answered immediately so authentication or subscription
/// startup cannot fail simply because Alpaca sends a heartbeat.
///
/// Pong frames are ignored.
///
/// Connection closure and transport errors are propagated to the caller.
async fn read_websocket_json(
    ws_stream: &mut AlpacaTradeUpdatesWebSocket,
) -> anyhow::Result<Value> {
    loop {
        let message =
            ws_stream
                .next()
                .await
                .ok_or_else(
                    || {
                        anyhow::anyhow!(
                            "Alpaca trade_updates WebSocket closed during startup"
                        )
                    }
                )??;


        match message {

            // ====================================================
            // TEXT
            // ====================================================

            Message::Text(
                text
            ) => {
                return Ok(
                    serde_json::from_str(
                        text.as_ref()
                    )?
                );
            }


            // ====================================================
            // BINARY
            // ====================================================

            Message::Binary(
                bytes
            ) => {
                let text =
                    String::from_utf8(
                        bytes.to_vec()
                    )?;


                return Ok(
                    serde_json::from_str(
                        &text
                    )?
                );
            }


            // ====================================================
            // PING
            // ====================================================

            Message::Ping(
                payload
            ) => {
                ws_stream
                    .send(
                        Message::Pong(
                            payload
                        )
                    )
                    .await?;
            }


            // ====================================================
            // PONG
            // ====================================================

            Message::Pong(
                _
            ) => {
                continue;
            }


            // ====================================================
            // CLOSE
            // ====================================================

            Message::Close(
                frame
            ) => {
                anyhow::bail!(
                    "Alpaca trade_updates WebSocket closed during startup: {:?}",
                    frame
                );
            }


            // ====================================================
            // OTHER
            // ====================================================

            _ => {
                continue;
            }
        }
    }
}


// ============================================================
// SEND TRADE UPDATES ERROR
// ============================================================

/// Sends a remote notification when the Alpaca execution WebSocket cannot
/// establish or restore execution monitoring.
///
/// This helper is reserved for WebSocket lifecycle failures. Correlated
/// order/execution errors should eventually flow through canonical `ExecEvent`s
/// and be handled by the engine.
fn send_trade_updates_error(
    cfg: &Config,
    description: &str,
    error: &str,
    retry_secs: u64,
) {
    let remote_message =
        format!(
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
        Some(
            get_slack_reconnect_error_channel_id()
        ),
        Some(
            get_discord_reconnect_error_channel_id()
        ),
    );
}


// ============================================================
// RECONNECT BACKOFF
// ============================================================

/// Sleeps for the current reconnect delay and then increases it
/// exponentially up to the configured maximum.
///
/// `2s -> 4s -> 8s -> 10s -> 10s...`
async fn sleep_and_increase_backoff(
    backoff_secs: &mut u64,
) {
    tokio::time::sleep(
        Duration::from_secs(
            *backoff_secs
        )
    )
    .await;


    *backoff_secs =
        (*backoff_secs * 2)
            .min(
                MAX_BACKOFF_SECS
            );
}