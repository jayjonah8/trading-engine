use std::{
    collections::HashMap,
    time::Duration,
};

use futures_util::{
    SinkExt,
    StreamExt,
};

use tokio::net::TcpStream;

use tokio_tungstenite::{
    connect_async,
    MaybeTlsStream,
    WebSocketStream,
    tungstenite::Message,
};

use crate::{
    core::exec::{
        ExecEvent,
    },

    exchange::alpaca_utils::{
        AlpacaConfig,
    },
};

use crate::core::config::Config;

use crate::notifications::{
    send_remote_message,
    env_helpers::{
        get_slack_error_channel_id,
        get_discord_error_channel_id,
    },
};

use crate::shared::utils::{
    get_bot_name,
    get_local_timestamp,
};


// ============================================================
// ORDER CORRELATION
// ============================================================

/// Identifies whether an Alpaca order belongs to an opening or closing request.
///
/// The adapter keeps only this small amount of correlation state so incoming
/// WebSocket executions can be translated into the correct engine ExecEvent.
#[derive(Debug, Clone)]
pub(crate) enum OrderKind {
    Open,
    Close,
}


/// Minimal information needed to correlate an Alpaca WebSocket order update
/// back to the engine command that originally created the order.
///
/// This state belongs to the execution adapter rather than the engine because
/// it exists only to translate Alpaca order IDs back into engine identifiers.
#[derive(Debug, Clone)]
pub(crate) struct OrderCorrelation {
    /// Engine-generated client order ID.
    pub cid: String,

    /// Symbol associated with the order.
    pub symbol: String,

    /// Whether this order opens or closes a position.
    pub kind: OrderKind,
}


// ============================================================
// WEBSOCKET TYPE
// ============================================================

pub(crate) type AlpacaExecutionWebSocket =
    WebSocketStream<
        MaybeTlsStream<TcpStream>
    >;


// ============================================================
// CONNECT ALPACA EXECUTION WEBSOCKET
// ============================================================

/// Creates a fresh Alpaca trade_updates WebSocket connection.
///
/// Every connection performs the complete startup sequence:
///
/// 1. Connect to the correct paper or live Alpaca WebSocket.
/// 2. Authenticate with the configured Alpaca credentials.
/// 3. Wait for Alpaca to confirm authorization.
/// 4. Subscribe to `trade_updates`.
/// 5. Wait for Alpaca to confirm the subscription.
///
/// The function only returns the WebSocket after execution updates are
/// confirmed active. This makes it safe for the execution adapter to resume
/// processing new order commands after a reconnect.
///
/// A timeout is used for startup responses so a stalled connection does not
/// permanently block the execution adapter.
pub(crate) async fn connect_alpaca_execution_ws(
    config: &AlpacaConfig,
) -> anyhow::Result<AlpacaExecutionWebSocket> {

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


    let auth_response =
        tokio::time::timeout(
            Duration::from_secs(
                10
            ),
            read_websocket_json(
                &mut ws_stream
            ),
        )
        .await
        .map_err(
            |_| {
                anyhow::anyhow!(
                    "Timed out waiting for Alpaca WebSocket authentication"
                )
            }
        )??;


    let auth_ok =
        auth_response
            .get(
                "stream"
            )
            .and_then(
                |value| {
                    value.as_str()
                }
            )
            == Some(
                "authorization"
            )
        &&
        auth_response
            .get(
                "data"
            )
            .and_then(
                |data| {
                    data.get(
                        "status"
                    )
                }
            )
            .and_then(
                |value| {
                    value.as_str()
                }
            )
            == Some(
                "authorized"
            );


    if !auth_ok {
        anyhow::bail!(
            "Alpaca WebSocket authentication failed: {}",
            auth_response
        );
    }


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


    let listen_response =
        tokio::time::timeout(
            Duration::from_secs(
                10
            ),
            read_websocket_json(
                &mut ws_stream
            ),
        )
        .await
        .map_err(
            |_| {
                anyhow::anyhow!(
                    "Timed out waiting for Alpaca trade_updates subscription"
                )
            }
        )??;


    let listening =
        listen_response
            .get(
                "stream"
            )
            .and_then(
                |value| {
                    value.as_str()
                }
            )
            == Some(
                "listening"
            );


    let subscribed =
        listen_response
            .get(
                "data"
            )
            .and_then(
                |data| {
                    data.get(
                        "streams"
                    )
                }
            )
            .and_then(
                |value| {
                    value.as_array()
                }
            )
            .map(
                |streams| {
                    streams
                        .iter()
                        .any(
                            |stream| {
                                stream.as_str()
                                    == Some(
                                        "trade_updates"
                                    )
                            }
                        )
                }
            )
            .unwrap_or(
                false
            );


    if !listening || !subscribed {
        anyhow::bail!(
            "Alpaca trade_updates subscription failed: {}",
            listen_response
        );
    }


    Ok(
        ws_stream
    )
}


// ============================================================
// HANDLE ALPACA TRADE UPDATE
// ============================================================

/// Converts an Alpaca `trade_updates` message into the appropriate engine
/// execution event.
///
/// The Alpaca order ID is matched against `order_map` so opening and closing
/// fills can be routed to the correct PendingOpen or PendingClose.
///
/// Final fills and terminal order failures remove their temporary correlation
/// entry because no additional execution updates are expected for that order.
///
/// 
/// # Fill Accounting
///
/// Alpaca provides both individual execution data and cumulative order state:
///
/// - `fill_sz` / `fill_px` represent the current individual execution.
/// - `total_sz` / `avg_px` represent the cumulative state of the entire order.
///
/// Individual fills must use `fill_sz` / `fill_px` for incremental accounting,
/// while `total_sz` / `avg_px` should directly replace cumulative order state.
///
/// For example, partial executions may report:
///
/// `fill_sz: 38 -> 2 -> 74`
///
/// while cumulative filled size reports:
///
/// `total_sz: 38 -> 40 -> 114`
///
/// `total_sz` must never be added to the previous filled quantity because it
/// already includes all executions that occurred before the current update.
pub(crate) fn handle_trade_update(
    cfg: &Config,
    message: serde_json::Value,
    order_map: &mut HashMap<String, OrderCorrelation>,
    tx_evt: &tokio::sync::mpsc::UnboundedSender<ExecEvent>,
) {
    // Ignore WebSocket authorization / listening responses.
    if message
        .get(
            "stream"
        )
        .and_then(
            |value| {
                value.as_str()
            }
        )
        != Some(
            "trade_updates"
        )
    {
        return;
    }


    let Some(data) =
        message.get(
            "data"
        )
    else {
        return;
    };


    let Some(event_type) =
        data
            .get(
                "event"
            )
            .and_then(
                |value| {
                    value.as_str()
                }
            )
    else {
        return;
    };


    let Some(order) =
        data.get(
            "order"
        )
    else {
        return;
    };


    let Some(oid) =
        order
            .get(
                "id"
            )
            .and_then(
                |value| {
                    value.as_str()
                }
            )
            .map(
                str::to_string
            )
    else {
        return;
    };


    let Some(correlation) =
        order_map
            .get(
                &oid
            )
            .cloned()
    else {
        eprintln!(
            "[ADAPTER] Unknown Alpaca order in trade_updates. oid={}",
            oid
        );

        let remote_message = format!(
            "🚨 ALPACA EXECUTION ERROR – `{}`\n\
            Received execution update for unknown order\n\
            Adapter: `AlpacaExchangeAdapter - handle_trade_update()`\n\
            Stream: `trade_updates`\n\
            OID: `{}`\n\
            Error: `Order not found in adapter correlation map`\n\
            Time: `{}`",
            get_bot_name(),
            oid,
            get_local_timestamp(),
        );

        send_remote_message(
            cfg,
            remote_message,
            Some(get_slack_error_channel_id()),
            Some(get_discord_error_channel_id()),
        );

        return;
    };


    match event_type {

        // ========================================================
        // ORDER RESTING
        // ========================================================

        "new" => {
            let _ =
                tx_evt.send(
                    ExecEvent::Resting {
                        cid:
                            correlation.cid,

                        symbol:
                            correlation.symbol,

                        oid,
                    }
                );
        }


        // ========================================================
        // PARTIAL FILL
        // ========================================================

        "partial_fill" => {
            let Some(fill_px) =
                parse_string_f64(
                    data.get(
                        "price"
                    )
                )
            else {
                send_parse_error(
                    tx_evt,
                    &correlation,
                    "partial_fill missing/invalid price",
                );

                return;
            };


            let Some(fill_sz) =
                parse_string_f64(
                    data.get(
                        "qty"
                    )
                )
            else {
                send_parse_error(
                    tx_evt,
                    &correlation,
                    "partial_fill missing/invalid qty",
                );

                return;
            };


            let Some(avg_px) =
                parse_string_f64(
                    order.get(
                        "filled_avg_price"
                    )
                )
            else {
                send_parse_error(
                    tx_evt,
                    &correlation,
                    "partial_fill missing/invalid filled_avg_price",
                );

                return;
            };


            let Some(total_sz) =
                parse_string_f64(
                    order.get(
                        "filled_qty"
                    )
                )
            else {
                send_parse_error(
                    tx_evt,
                    &correlation,
                    "partial_fill missing/invalid filled_qty",
                );

                return;
            };


            let Some(execution_id) =
                data
                    .get(
                        "execution_id"
                    )
                    .and_then(
                        |value| {
                            value.as_str()
                        }
                    )
                    .map(
                        str::to_string
                    )
            else {
                send_parse_error(
                    tx_evt,
                    &correlation,
                    "partial_fill missing execution_id",
                );

                return;
            };


            let event =
                match correlation.kind {
                    OrderKind::Open => {
                        ExecEvent::OpenPartialFill {
                            cid:
                                correlation.cid,

                            symbol:
                                correlation.symbol,

                            fill_px,

                            fill_sz,

                            avg_px,

                            total_sz,

                            oid,

                            execution_id,
                        }
                    }


                    OrderKind::Close => {
                        ExecEvent::ClosePartialFill {
                            cid:
                                correlation.cid,

                            symbol:
                                correlation.symbol,

                            fill_px,

                            fill_sz,

                            avg_px,

                            total_sz,

                            oid,

                            execution_id,
                        }
                    }
                };


            let _ =
                tx_evt.send(
                    event
                );
        }


        // ========================================================
        // FINAL FILL
        // ========================================================

        "fill" => {
            let Some(fill_px) =
                parse_string_f64(
                    data.get(
                        "price"
                    )
                )
            else {
                send_parse_error(
                    tx_evt,
                    &correlation,
                    "fill missing/invalid price",
                );

                return;
            };


            let Some(fill_sz) =
                parse_string_f64(
                    data.get(
                        "qty"
                    )
                )
            else {
                send_parse_error(
                    tx_evt,
                    &correlation,
                    "fill missing/invalid qty",
                );

                return;
            };


            let Some(avg_px) =
                parse_string_f64(
                    order.get(
                        "filled_avg_price"
                    )
                )
            else {
                send_parse_error(
                    tx_evt,
                    &correlation,
                    "fill missing/invalid filled_avg_price",
                );

                return;
            };


            let Some(total_sz) =
                parse_string_f64(
                    order.get(
                        "filled_qty"
                    )
                )
            else {
                send_parse_error(
                    tx_evt,
                    &correlation,
                    "fill missing/invalid filled_qty",
                );

                return;
            };


            let Some(execution_id) =
                data
                    .get(
                        "execution_id"
                    )
                    .and_then(
                        |value| {
                            value.as_str()
                        }
                    )
                    .map(
                        str::to_string
                    )
            else {
                send_parse_error(
                    tx_evt,
                    &correlation,
                    "fill missing execution_id",
                );

                return;
            };


            let event =
                match correlation.kind {
                    OrderKind::Open => {
                        ExecEvent::OpenFill {
                            cid:
                                correlation.cid,

                            symbol:
                                correlation.symbol,

                            fill_px,

                            fill_sz,

                            avg_px,

                            total_sz,

                            oid:
                                oid.clone(),

                            execution_id,
                        }
                    }


                    OrderKind::Close => {
                        ExecEvent::CloseFill {
                            cid:
                                correlation.cid,

                            symbol:
                                correlation.symbol,

                            fill_px,

                            fill_sz,

                            avg_px,

                            total_sz,

                            oid:
                                oid.clone(),

                            execution_id,
                        }
                    }
                };


            let _ =
                tx_evt.send(
                    event
                );


            // The order is completely filled, so the adapter no longer
            // needs its temporary correlation entry.
            order_map.remove(
                &oid
            );
        }


        // ========================================================
        // TERMINAL ORDER FAILURE
        // ========================================================

        "rejected"
        | "canceled"
        | "expired" => {
            let _ =
                tx_evt.send(
                    ExecEvent::Error {
                        cid:
                            correlation.cid,

                        symbol:
                            correlation.symbol,

                        message:
                            format!(
                                "Alpaca order {} event={}",
                                oid,
                                event_type,
                            ),
                    }
                );


            order_map.remove(
                &oid
            );
        }


        // Alpaca also sends lifecycle events such as pending_new,
        // accepted, pending_cancel, etc. They do not change our engine's
        // execution accounting, so they are intentionally ignored here.
        _ => {}
    }
}


// ============================================================
// PARSE ALPACA NUMBER
// ============================================================

/// Alpaca commonly returns order quantities and prices as JSON strings.
///
/// Example:
///
///     "qty": "38"
///     "price": "320.74"
///
/// This helper also accepts a real JSON number for defensive parsing.
fn parse_string_f64(
    value: Option<&serde_json::Value>,
) -> Option<f64> {
    match value? {
        serde_json::Value::String(
            value
        ) => {
            value
                .parse::<f64>()
                .ok()
        }

        serde_json::Value::Number(
            value
        ) => {
            value.as_f64()
        }

        _ => {
            None
        }
    }
}


// ============================================================
// SEND PARSE ERROR
// ============================================================

/// Converts malformed order-specific WebSocket data into ExecEvent::Error.
///
/// The engine owns the actual execution-error log and remote notification so
/// the adapter does not duplicate those actions for a correlated order.
fn send_parse_error(
    tx_evt: &tokio::sync::mpsc::UnboundedSender<ExecEvent>,
    correlation: &OrderCorrelation,
    message: &str,
) {
    let _ =
        tx_evt.send(
            ExecEvent::Error {
                cid:
                    correlation.cid.clone(),

                symbol:
                    correlation.symbol.clone(),

                message:
                    message.to_string(),
            }
        );
}


// ============================================================
// READ WEBSOCKET JSON
// ============================================================

/// Waits for the next usable Alpaca WebSocket message and parses it as JSON.
///
/// This is used during connection startup while waiting for Alpaca's
/// authorization and subscription confirmations.
///
/// Text and binary frames are supported. Ping/Pong frames are ignored.
/// Connection closure or transport failure is returned to the caller.
async fn read_websocket_json<S>(
    ws_read: &mut S,
) -> anyhow::Result<serde_json::Value>
where
    S:
        StreamExt<
            Item =
                Result<
                    Message,
                    tokio_tungstenite::tungstenite::Error,
                >
        >
        + Unpin,
{
    loop {
        let message =
            ws_read
                .next()
                .await
                .ok_or_else(
                    || {
                        anyhow::anyhow!(
                            "Alpaca WebSocket closed"
                        )
                    }
                )??;


        match message {
            Message::Text(
                text
            ) => {
                return Ok(
                    serde_json::from_str(
                        &text
                    )?
                );
            }


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


            Message::Ping(
                _
            ) => {
                continue;
            }


            Message::Pong(
                _
            ) => {
                continue;
            }


            Message::Close(
                frame
            ) => {
                anyhow::bail!(
                    "Alpaca WebSocket closed during startup: {:?}",
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
// WEBSOCKET MESSAGE TO JSON
// ============================================================

/// Converts a normal Alpaca trade_updates WebSocket frame into JSON.
///
/// Paper and live trade_updates may arrive as text or binary frames.
/// Ping/Pong frames are intentionally ignored.
///
/// A close frame is returned as an error so the adapter supervisor can
/// reconnect the execution WebSocket.
pub(crate) fn websocket_message_to_json(
    message: Message,
) -> anyhow::Result<Option<serde_json::Value>> {
    match message {
        Message::Text(
            text
        ) => {
            Ok(
                Some(
                    serde_json::from_str(
                        &text
                    )?
                )
            )
        }


        Message::Binary(
            bytes
        ) => {
            let text =
                String::from_utf8(
                    bytes.to_vec()
                )?;


            Ok(
                Some(
                    serde_json::from_str(
                        &text
                    )?
                )
            )
        }


        Message::Ping(
            _
        ) => {
            Ok(
                None
            )
        }


        Message::Pong(
            _
        ) => {
            Ok(
                None
            )
        }


        Message::Close(
            frame
        ) => {
            anyhow::bail!(
                "Alpaca WebSocket closed: {:?}",
                frame
            );
        }


        _ => {
            Ok(
                None
            )
        }
    }
}