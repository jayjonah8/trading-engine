#![allow(dead_code)]

use std::time::Duration;

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

use serde_json::{
    json,
    Value,
};


// ============================================================
// ALPACA MARKET DATA WEBSOCKET TYPE
// ============================================================

pub(crate) type AlpacaMarketDataWebSocket =
    WebSocketStream<MaybeTlsStream<TcpStream>>;


// ============================================================
// RECONNECT CONFIG
// ============================================================

pub(crate) const INITIAL_BACKOFF_SECS: u64 = 2;
pub(crate) const MAX_BACKOFF_SECS: u64 = 10;


// ============================================================
// CONNECT ALPACA MARKET DATA WEBSOCKET
// ============================================================

/// Creates a fresh Alpaca market-data WebSocket connection.
///
/// Every connection performs the complete startup sequence:
///
/// 1. Connect to the configured Alpaca market-data endpoint.
/// 2. Send Alpaca authentication credentials.
/// 3. Wait for Alpaca to confirm authentication.
/// 4. Subscribe to trades and quotes for all configured symbols.
/// 5. Wait for Alpaca to confirm all requested subscriptions.
///
/// The function only returns after the market-data connection is fully
/// authenticated and every requested trade/quote subscription has been
/// confirmed.
///
/// This function does not own long-running reconnection policy. If the
/// returned WebSocket later disconnects, the market-data supervisor should
/// call this function again to create a completely fresh connection.
///
/// The `CandleManager` intentionally does not live here so active candle
/// state can survive temporary WebSocket reconnects.
pub(crate) async fn connect_alpaca_market_data_ws(
    ws_url: &str,
    api_key: &str,
    api_secret: &str,
    symbols: &[String],
) -> anyhow::Result<AlpacaMarketDataWebSocket> {

    // ============================================================
    // VALIDATE SYMBOLS
    // ============================================================

    if symbols.is_empty() {
        anyhow::bail!(
            "Alpaca market-data WebSocket requires at least one symbol"
        );
    }


    // ============================================================
    // CONNECT
    // ============================================================

    let (mut ws_stream, _) =
        tokio::time::timeout(
            Duration::from_secs(10),
            connect_async(ws_url),
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "Timed out connecting to Alpaca market-data WebSocket"
            )
        })??;


    // ============================================================
    // AUTHENTICATE
    // ============================================================

    let auth = json!({
        "action": "auth",
        "key": api_key,
        "secret": api_secret,
    });

    ws_stream
        .send(
            Message::Text(
                auth.to_string().into()
            )
        )
        .await?;


    // ============================================================
    // WAIT FOR AUTH CONFIRMATION
    // ============================================================

    wait_for_authentication(
        &mut ws_stream
    )
    .await?;


    // ============================================================
    // SUBSCRIBE
    // ============================================================

    let subscription = json!({
        "action": "subscribe",
        "trades": symbols,
        "quotes": symbols,
    });

    ws_stream
        .send(
            Message::Text(
                subscription.to_string().into()
            )
        )
        .await?;


    // ============================================================
    // WAIT FOR SUBSCRIPTION CONFIRMATION
    // ============================================================

    wait_for_subscription(
        &mut ws_stream,
        symbols,
    )
    .await?;


    Ok(ws_stream)
}


// ============================================================
// WAIT FOR AUTHENTICATION
// ============================================================

/// Waits until Alpaca confirms that the market-data WebSocket
/// has been successfully authenticated.
///
/// Control/status messages unrelated to authentication are ignored while
/// waiting. Authentication errors and connection failures are returned
/// immediately.
async fn wait_for_authentication(
    ws_stream: &mut AlpacaMarketDataWebSocket,
) -> anyhow::Result<()> {
    tokio::time::timeout(
        Duration::from_secs(10),
        async {
            loop {
                let messages =
                    read_market_data_messages(
                        ws_stream
                    )
                    .await?;

                for message in messages {
                    match message
                        .get("T")
                        .and_then(Value::as_str)
                    {
                        Some("success") => {
                            let msg =
                                message
                                    .get("msg")
                                    .and_then(Value::as_str);

                            if msg == Some("authenticated") {
                                return Ok(());
                            }
                        }

                        Some("error") => {
                            anyhow::bail!(
                                "Alpaca market-data authentication failed: {}",
                                message
                            );
                        }

                        _ => {}
                    }
                }
            }
        },
    )
    .await
    .map_err(|_| {
        anyhow::anyhow!(
            "Timed out waiting for Alpaca market-data authentication"
        )
    })?
}


// ============================================================
// WAIT FOR SUBSCRIPTION
// ============================================================

/// Waits until Alpaca confirms both trade and quote subscriptions
/// for every requested symbol.
///
/// The connection is not considered ready until every configured symbol
/// appears in both Alpaca subscription acknowledgement lists.
async fn wait_for_subscription(
    ws_stream: &mut AlpacaMarketDataWebSocket,
    symbols: &[String],
) -> anyhow::Result<()> {
    tokio::time::timeout(
        Duration::from_secs(10),
        async {
            loop {
                let messages =
                    read_market_data_messages(
                        ws_stream
                    )
                    .await?;

                for message in messages {
                    match message
                        .get("T")
                        .and_then(Value::as_str)
                    {
                        Some("subscription") => {
                            let subscribed_trades =
                                message
                                    .get("trades")
                                    .and_then(Value::as_array);

                            let subscribed_quotes =
                                message
                                    .get("quotes")
                                    .and_then(Value::as_array);

                            let trades_ok =
                                subscribed_trades
                                    .map(|subscribed| {
                                        symbols.iter().all(|symbol| {
                                            subscribed.iter().any(|value| {
                                                value.as_str() == Some(symbol.as_str())
                                            })
                                        })
                                    })
                                    .unwrap_or(false);

                            let quotes_ok =
                                subscribed_quotes
                                    .map(|subscribed| {
                                        symbols.iter().all(|symbol| {
                                            subscribed.iter().any(|value| {
                                                value.as_str() == Some(symbol.as_str())
                                            })
                                        })
                                    })
                                    .unwrap_or(false);

                            if trades_ok && quotes_ok {
                                return Ok(());
                            }
                        }

                        Some("error") => {
                            anyhow::bail!(
                                "Alpaca market-data subscription failed: {}",
                                message
                            );
                        }

                        _ => {}
                    }
                }
            }
        },
    )
    .await
    .map_err(|_| {
        anyhow::anyhow!(
            "Timed out waiting for Alpaca market-data subscription confirmation"
        )
    })?
}


// ============================================================
// READ MARKET DATA MESSAGES
// ============================================================

/// Reads the next usable Alpaca market-data WebSocket frame and parses
/// the normal JSON array payload returned by Alpaca.
///
/// Alpaca market-data responses are normally arrays, even when only one
/// control or market-data message is present.
///
/// Ping frames are answered immediately so startup cannot fail simply
/// because the server sent a heartbeat while authentication or subscription
/// confirmation was pending.
///
/// Connection closure and transport errors are returned to the caller.
async fn read_market_data_messages(
    ws_stream: &mut AlpacaMarketDataWebSocket,
) -> anyhow::Result<Vec<Value>> {
    loop {
        let message =
            ws_stream
                .next()
                .await
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Alpaca market-data WebSocket closed during startup"
                    )
                })??;

        match message {
            Message::Text(text) => {
                return Ok(
                    serde_json::from_str(
                        text.as_ref()
                    )?
                );
            }

            Message::Binary(bytes) => {
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

            Message::Ping(payload) => {
                ws_stream
                    .send(
                        Message::Pong(
                            payload
                        )
                    )
                    .await?;
            }

            Message::Pong(_) => {
                continue;
            }

            Message::Close(frame) => {
                anyhow::bail!(
                    "Alpaca market-data WebSocket closed during startup: {:?}",
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
// RECONNECT BACKOFF
// ============================================================

/// Sleeps for the current reconnect delay and then increases it
/// exponentially up to the configured maximum.
///
/// 2s -> 4s -> 8s -> 10s -> 10s...
pub(crate) async fn sleep_and_increase_backoff(
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
            .min(MAX_BACKOFF_SECS);
}