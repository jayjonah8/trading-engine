#![allow(dead_code)]

use anyhow::Result;

use futures_util::{
    SinkExt,
    StreamExt,
};

use serde_json::{
    json,
    Value,
};

use tokio::time::{
    timeout,
    Duration,
};

use tokio_tungstenite::{
    connect_async,
    tungstenite::Message,
};

// cargo test test_fast_live_price_feed -- --nocapture

/// Tests Hyperliquid's fast L2 WebSocket feed.
///
/// The `fast: true` subscription requests Hyperliquid's faster shallow
/// order-book feed rather than the slower standard L2 snapshots.
///
/// Add or remove as many symbols as desired from `coins`.
#[tokio::test]
async fn test_fast_live_price_feed() -> Result<()> {
    // ============================================================
    // CONFIG
    // ============================================================

    let coins = vec![
        "BTC",
        "ETH",
        "SOL",
        "HYPE",
        "ZEC",
        "DOGE",
        "AVAX",
        "NEAR",
        "LIT",
    ];

    let ws_url = "wss://api.hyperliquid.xyz/ws";

    println!();
    println!("========================================");
    println!("HYPERLIQUID FAST L2 PRICE FEED");
    println!("========================================");
    println!("Coins: {:?}", coins);
    println!("Connecting...");
    println!();

    // ============================================================
    // CONNECT
    // ============================================================

    let (ws, _) = connect_async(ws_url).await?;

    let (mut write, mut read) = ws.split();

    println!("[WS] connected");

    // ============================================================
    // SUBSCRIBE
    // ============================================================

    for coin in &coins {
        let subscription = json!({
            "method": "subscribe",
            "subscription": {
                "type": "l2Book",
                "coin": coin,
                "fast": true
            }
        });

        write
            .send(Message::Text(
                subscription.to_string().into()
            ))
            .await?;

        println!(
            "[WS] subscribed FAST L2: {}",
            coin
        );
    }

    println!();
    println!("[WS] fast subscriptions active");
    println!();

    // ============================================================
    // RECEIVE
    // ============================================================

    let feed = async {
        while let Some(message) = read.next().await {
            let message = match message {
                Ok(message) => message,
                Err(err) => {
                    eprintln!(
                        "[WS] receive error: {}",
                        err
                    );

                    break;
                }
            };

            let Message::Text(text) = message else {
                continue;
            };

            let Ok(value) =
                serde_json::from_str::<Value>(&text)
            else {
                continue;
            };

            // Ignore subscription acknowledgements and unrelated messages.
            if value
                .get("channel")
                .and_then(Value::as_str)
                != Some("l2Book")
            {
                continue;
            }

            let Some(data) = value.get("data") else {
                continue;
            };

            let coin = data
                .get("coin")
                .and_then(Value::as_str)
                .unwrap_or("?");

            let ts_ms = data
                .get("time")
                .and_then(Value::as_u64)
                .unwrap_or(0);

            let Some(levels) =
                data.get("levels").and_then(Value::as_array)
            else {
                continue;
            };

            // Hyperliquid returns:
            //
            // levels[0] = bids
            // levels[1] = asks
            let best_bid = levels
                .get(0)
                .and_then(Value::as_array)
                .and_then(|bids| bids.first())
                .and_then(|level| level.get("px"))
                .and_then(Value::as_str);

            let best_ask = levels
                .get(1)
                .and_then(Value::as_array)
                .and_then(|asks| asks.first())
                .and_then(|level| level.get("px"))
                .and_then(Value::as_str);

            let (Some(best_bid), Some(best_ask)) =
                (best_bid, best_ask)
            else {
                continue;
            };

            let Ok(bid) = best_bid.parse::<f64>() else {
                continue;
            };

            let Ok(ask) = best_ask.parse::<f64>() else {
                continue;
            };

            let mid = (bid + ask) / 2.0;

            println!(
                "[FAST L2] {:<6} bid={:<12.4} ask={:<12.4} mid={:<12.4} ts={}",
                coin,
                bid,
                ask,
                mid,
                ts_ms,
            );
        }
    };

    // Run the live feed for 30 seconds.
    let _ = timeout(
        Duration::from_secs(30),
        feed,
    )
    .await;

    println!();
    println!("========================================");
    println!("FAST PRICE FEED TEST COMPLETE");
    println!("========================================");

    Ok(())
}