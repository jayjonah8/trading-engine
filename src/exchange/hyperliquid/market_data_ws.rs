#![allow(dead_code)]

use std::time::Duration;

use futures_util::SinkExt;

use serde_json::json;

use tokio::net::TcpStream;

use tokio_tungstenite::{
    connect_async,
    MaybeTlsStream,
    WebSocketStream,
    tungstenite::Message,
};


// ============================================================
// HYPERLIQUID MARKET DATA CONFIG
// ============================================================

const HYPERLIQUID_MAINNET_WS: &str = "wss://api.hyperliquid.xyz/ws";

pub(crate) const INITIAL_BACKOFF_SECS: u64 = 2;
pub(crate) const MAX_BACKOFF_SECS: u64 = 10;


// ============================================================
// WEBSOCKET TYPE
// ============================================================

pub(crate) type HyperliquidMarketDataWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;


// ============================================================
// CONNECT HYPERLIQUID MARKET DATA WEBSOCKET
// ============================================================

/// Creates a fresh Hyperliquid market-data WebSocket connection.
///
/// Every connection performs the complete market-data startup sequence:
///
/// 1. Connect to Hyperliquid.
/// 2. Subscribe every configured symbol to fast `l2Book`.
/// 3. Subscribe every configured symbol to public `trades`.
/// 4. Return the fully initialized WebSocket.
///
/// This function is called again after every reconnect so subscriptions
/// are automatically restored after a transport failure.
pub(crate) async fn connect_hyperliquid_market_data_ws(
    symbols: &[String],
) -> anyhow::Result<HyperliquidMarketDataWebSocket> {

    if symbols.is_empty() {
        anyhow::bail!(
            "Hyperliquid market-data WebSocket requires at least one symbol"
        );
    }


    // ============================================================
    // CONNECT
    // ============================================================

    let (mut ws_stream, _) = tokio::time::timeout(
        Duration::from_secs(10),
        connect_async(HYPERLIQUID_MAINNET_WS),
    )
    .await
    .map_err(
        |_| {
            anyhow::anyhow!(
                "Timed out connecting to Hyperliquid market-data WebSocket"
            )
        }
    )??;


    // ============================================================
    // SUBSCRIBE ALL SYMBOLS
    // ============================================================

    for symbol in symbols {

        // --------------------------------------------------------
        // FAST L2
        // --------------------------------------------------------

        let l2_subscription = json!({
            "method": "subscribe",
            "subscription": {
                "type": "l2Book",
                "coin": symbol,
                "fast": true
            }
        });


        tokio::time::timeout(
            Duration::from_secs(10),
            ws_stream.send(
                Message::Text(
                    l2_subscription.to_string().into()
                )
            ),
        )
        .await
        .map_err(
            |_| {
                anyhow::anyhow!(
                    "Timed out subscribing to Hyperliquid FAST L2 for {}",
                    symbol
                )
            }
        )??;


        // --------------------------------------------------------
        // PUBLIC TRADES
        // --------------------------------------------------------

        let trades_subscription = json!({
            "method": "subscribe",
            "subscription": {
                "type": "trades",
                "coin": symbol
            }
        });


        tokio::time::timeout(
            Duration::from_secs(10),
            ws_stream.send(
                Message::Text(
                    trades_subscription.to_string().into()
                )
            ),
        )
        .await
        .map_err(
            |_| {
                anyhow::anyhow!(
                    "Timed out subscribing to Hyperliquid trades for {}",
                    symbol
                )
            }
        )??;


        println!(
            "[HYPERLIQUID MARKET] subscribed | symbol={} | fast_l2=true | trades=true",
            symbol
        );
    }


    Ok(ws_stream)
}


// ============================================================
// RECONNECT BACKOFF
// ============================================================

/// Sleeps for the current reconnect delay and increases it exponentially.
///
/// Backoff:
///
/// 2s -> 4s -> 8s -> 10s -> 10s...
pub(crate) async fn sleep_and_increase_backoff(
    backoff_secs: &mut u64,
) {
    tokio::time::sleep(
        Duration::from_secs(*backoff_secs)
    )
    .await;

    *backoff_secs = (*backoff_secs * 2).min(MAX_BACKOFF_SECS);
}