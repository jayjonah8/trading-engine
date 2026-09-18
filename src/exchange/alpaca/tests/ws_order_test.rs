use anyhow::{
    anyhow,
    Result,
};

use futures_util::{
    SinkExt,
    StreamExt,
};

use reqwest::Client;

use serde::{
    Deserialize,
    Serialize,
};

use tokio::time::{
    timeout,
    Duration,
};

use tokio_tungstenite::{
    connect_async,
    tungstenite::Message,
};



// ============================================================
// ALPACA TRADE UPDATE TESTS
// ============================================================
//
// This file tests the full Alpaca order lifecycle using both:
//
// 1. REST requests to submit open/close orders.
// 2. Alpaca's WebSocket `trade_updates` stream to receive
//    the later order status and fill information.
//
// The important behavior being tested is that the immediate REST
// response does NOT necessarily contain the final fill data.
//
// For example, an order may initially return:
//
//     status = "pending_new"
//     filled_qty = "0"
//     filled_avg_price = null
//
// Then, after Alpaca actually executes the order, the WebSocket
// sends a later `trade_updates` message containing:
//
//     event = "fill"
//     status = "filled"
//     filled_qty = actual executed shares
//     filled_avg_price = actual average execution price
//
// This is the same lifecycle the production execution adapter
// will eventually use:
//
//     REST submit
//         ↓
//     order accepted
//         ↓
//     WebSocket trade update
//         ↓
//     actual fill data
//
// The open test:
// - connects to the Alpaca WebSocket
// - authenticates
// - subscribes to `trade_updates`
// - submits a market BUY order over REST
// - prints the immediate REST response
// - waits for the matching WebSocket fill
// - prints the final fill quantity and average fill price
//
// The close test:
// - connects to the Alpaca WebSocket
// - authenticates
// - subscribes to `trade_updates`
// - closes an existing AAPL position over REST
// - prints the immediate REST response
// - waits for the matching WebSocket fill
// - prints the final close quantity and average fill price
//
// IMPORTANT:
// These tests submit real orders to either the Alpaca PAPER or
// LIVE account depending on `LIVE_TRADING`.
//
// Keep `LIVE_TRADING = false` unless intentionally testing with
// real money.



// ============================================================
// RUN TESTS
// ============================================================
//
// Open AAPL trade + listen for WebSocket fill:
//
// cargo test test_alpaca_open_with_trade_updates -- --ignored --nocapture
//
// Close AAPL position + listen for WebSocket fill:
//
// cargo test test_alpaca_close_with_trade_updates -- --ignored --nocapture
//
// Run both Alpaca trade update tests:
//
// cargo test alpaca_trade_updates -- --ignored --nocapture



// ============================================================
// ALPACA ORDER REQUEST
// ============================================================

#[derive(Debug, Serialize)]
struct AlpacaOrderRequest {
    // Stock ticker symbol.
    symbol: String,

    // Dollar amount to trade.
    notional: String,

    // "buy" or "sell".
    side: String,

    // "market", "limit", etc.
    #[serde(rename = "type")]
    order_type: String,

    // "day", etc.
    time_in_force: String,

    // Our own identifier so the WebSocket update
    // can be matched back to this exact order.
    client_order_id: String,
}


// ============================================================
// ALPACA REST ORDER RESPONSE
// ============================================================

#[derive(Debug, Deserialize)]
struct AlpacaOrderResponse {
    id: String,

    client_order_id: String,

    symbol: String,

    qty: Option<String>,

    notional: Option<String>,

    filled_qty: String,

    filled_avg_price: Option<String>,

    side: String,

    #[serde(rename = "type")]
    order_type: String,

    time_in_force: String,

    status: String,
}


// ============================================================
// ALPACA WEBSOCKET ENVELOPE
// ============================================================

#[derive(Debug, Deserialize)]
struct AlpacaStreamEnvelope {
    stream: String,

    data: serde_json::Value,
}


// ============================================================
// ALPACA TRADE UPDATE
// ============================================================

#[derive(Debug, Deserialize)]
struct AlpacaTradeUpdate {
    event: String,

    order: AlpacaTradeUpdateOrder,
}


// ============================================================
// ALPACA TRADE UPDATE ORDER
// ============================================================

#[derive(Debug, Deserialize)]
struct AlpacaTradeUpdateOrder {
    id: String,

    client_order_id: String,

    symbol: String,

    qty: Option<String>,

    notional: Option<String>,

    filled_qty: String,

    filled_avg_price: Option<String>,

    side: String,

    status: String,

    position_intent: Option<String>,

    filled_at: Option<String>,
}


// ============================================================
// ALPACA TRADING MODE
// ============================================================

// false = Paper trading
// true  = Live trading
const LIVE_TRADING: bool = false;


// ============================================================
// CONFIG
// ============================================================

struct AlpacaTestConfig {
    api_key: String,

    api_secret: String,

    rest_url: String,

    websocket_url: String,
}


// ============================================================
// GET ALPACA TEST CONFIG
// ============================================================
//
// Loads the correct Alpaca REST/WebSocket endpoints and API keys
// based on `LIVE_TRADING`.
//
// false = paper account
// true  = live account
fn get_alpaca_test_config() -> Result<AlpacaTestConfig> {
    if LIVE_TRADING {
        Ok(
            AlpacaTestConfig {
                api_key:
                    std::env::var(
                        "ALPACA_API_KEY_ID"
                    )?,

                api_secret:
                    std::env::var(
                        "ALPACA_API_SECRET_KEY"
                    )?,

                rest_url:
                    "https://api.alpaca.markets"
                        .to_string(),

                websocket_url:
                    "wss://api.alpaca.markets/stream"
                        .to_string(),
            }
        )
    } else {
        Ok(
            AlpacaTestConfig {
                api_key:
                    std::env::var(
                        "ALPACA_PAPER_API_KEY_ID"
                    )?,

                api_secret:
                    std::env::var(
                        "ALPACA_PAPER_API_SECRET_KEY"
                    )?,

                rest_url:
                    "https://paper-api.alpaca.markets"
                        .to_string(),

                websocket_url:
                    "wss://paper-api.alpaca.markets/stream"
                        .to_string(),
            }
        )
    }
}



// ============================================================
// READ NEXT WEBSOCKET MESSAGE
// ============================================================
//
// Waits for the next useful Alpaca WebSocket message and converts
// either a text or binary frame into a String.
//
// Ping/Pong frames are ignored.
//
// Returns an error if Alpaca closes the WebSocket connection.
async fn read_ws_text<S>(
    read: &mut S,
) -> Result<String>
where
    S: StreamExt<
            Item = Result<
                Message,
                tokio_tungstenite::tungstenite::Error,
            >
        >
        + Unpin,
{
    loop {
        let message =
            read
                .next()
                .await
                .ok_or_else(
                    || anyhow!(
                        "Alpaca WebSocket closed"
                    )
                )??;


        match message {
            Message::Text(text) => {
                return Ok(
                    text.to_string()
                );
            }

            Message::Binary(bytes) => {
                return Ok(
                    String::from_utf8(
                        bytes.to_vec()
                    )?
                );
            }

            Message::Ping(_) => {
                continue;
            }

            Message::Pong(_) => {
                continue;
            }

            Message::Close(frame) => {
                return Err(
                    anyhow!(
                        "Alpaca WebSocket closed: {:?}",
                        frame
                    )
                );
            }

            _ => {
                continue;
            }
        }
    }
}


// ============================================================
// CONNECT TO ALPACA TRADE UPDATE STREAM
// ============================================================
//
// Opens the Alpaca trading WebSocket, authenticates using the
// configured API keys, and subscribes to the `trade_updates` stream.
//
// The returned reader is then used by the tests to wait for order
// lifecycle events such as:
// - new
// - partial_fill
// - fill
// - canceled
// - rejected
// - expired
async fn connect_trade_updates(
    config: &AlpacaTestConfig,
) -> Result<(
    impl SinkExt<
            Message,
            Error =
                tokio_tungstenite::tungstenite::Error,
        >
        + Unpin,

    impl StreamExt<
            Item = Result<
                Message,
                tokio_tungstenite::tungstenite::Error,
            >
        >
        + Unpin,
)> {
    let (
        websocket,
        _response,
    ) =
        connect_async(
            &config.websocket_url
        )
        .await?;


    let (
        mut write,
        mut read,
    ) =
        websocket.split();


    println!(
        "\nALPACA WS CONNECTED"
    );


    // ========================================================
    // AUTH
    // ========================================================

    let auth =
        serde_json::json!({
            "action": "auth",
            "key": config.api_key,
            "secret": config.api_secret
        });


    write
        .send(
            Message::Text(
                auth
                    .to_string()
                    .into()
            )
        )
        .await?;


    let auth_response =
        read_ws_text(
            &mut read
        )
        .await?;


    println!(
        "\nALPACA WS AUTH RESPONSE:"
    );

    println!(
        "{}",
        auth_response
    );


    // ========================================================
    // LISTEN
    // ========================================================

    let listen =
        serde_json::json!({
            "action": "listen",
            "data": {
                "streams": [
                    "trade_updates"
                ]
            }
        });


    write
        .send(
            Message::Text(
                listen
                    .to_string()
                    .into()
            )
        )
        .await?;


    let listen_response =
        read_ws_text(
            &mut read
        )
        .await?;


    println!(
        "\nALPACA WS LISTEN RESPONSE:"
    );

    println!(
        "{}",
        listen_response
    );


    Ok(
        (
            write,
            read,
        )
    )
}


// ============================================================
// WAIT FOR A SPECIFIC ORDER TO FILL
// ============================================================
//
// Continuously reads Alpaca `trade_updates` messages until it finds
// an update whose `client_order_id` matches the order being tested.
//
// Unrelated orders are ignored.
//
// The function keeps waiting through intermediate states such as
// `new` or `partial_fill` and returns only when:
// - the order fully fills, or
// - the order is canceled/rejected/expired.
async fn wait_for_order_fill<S>(
    read: &mut S,
    client_order_id: &str,
) -> Result<AlpacaTradeUpdate>
where
    S: StreamExt<
            Item = Result<
                Message,
                tokio_tungstenite::tungstenite::Error,
            >
        >
        + Unpin,
{
    loop {
        let text =
            read_ws_text(
                read
            )
            .await?;


        println!(
            "\n----------------------------------------"
        );

        println!(
            "ALPACA WS RAW MESSAGE"
        );

        println!(
            "----------------------------------------"
        );

        println!(
            "{}",
            text
        );


        let envelope:
            AlpacaStreamEnvelope =
            match serde_json::from_str(
                &text
            ) {
                Ok(value) => value,

                Err(_) => {
                    continue;
                }
            };


        if envelope.stream
            != "trade_updates"
        {
            continue;
        }


        let update:
            AlpacaTradeUpdate =
            serde_json::from_value(
                envelope.data
            )?;


        // Ignore unrelated orders.
        if update
            .order
            .client_order_id
            != client_order_id
        {
            println!(
                "Ignoring unrelated order: {}",
                update.order.client_order_id
            );

            continue;
        }


        println!(
            "\nMATCHED TRADE UPDATE"
        );

        println!(
            "event               = {}",
            update.event
        );

        println!(
            "status              = {}",
            update.order.status
        );

        println!(
            "symbol              = {}",
            update.order.symbol
        );

        println!(
            "side                = {}",
            update.order.side
        );

        println!(
            "order_id            = {}",
            update.order.id
        );

        println!(
            "client_order_id     = {}",
            update.order.client_order_id
        );

        println!(
            "qty                 = {:?}",
            update.order.qty
        );

        println!(
            "notional            = {:?}",
            update.order.notional
        );

        println!(
            "filled_qty          = {}",
            update.order.filled_qty
        );

        println!(
            "filled_avg_price    = {:?}",
            update.order.filled_avg_price
        );

        println!(
            "position_intent     = {:?}",
            update.order.position_intent
        );

        println!(
            "filled_at           = {:?}",
            update.order.filled_at
        );


        match update.event.as_str() {

            "fill" => {
                return Ok(
                    update
                );
            }


            "rejected"
            |
            "canceled"
            |
            "expired" => {
                return Err(
                    anyhow!(
                        "Order ended without filling: event={} status={}",
                        update.event,
                        update.order.status
                    )
                );
            }


            // Keep listening for:
            //
            // new
            // accepted
            // pending_new
            // partial_fill
            // etc.
            _ => {}
        }
    }
}


// ============================================================
// OPEN ORDER + WEBSOCKET FILL TEST
// ============================================================
//
// Tests the complete opening-order lifecycle.
//
// Flow:
//
// 1. Connect to Alpaca's trading WebSocket.
// 2. Authenticate.
// 3. Subscribe to `trade_updates`.
// 4. Generate a unique `client_order_id`.
// 5. Submit a dollar-sized AAPL market BUY over REST.
// 6. Print the immediate REST response.
// 7. Wait for the matching WebSocket order updates.
// 8. Stop when Alpaca reports `event = "fill"`.
// 9. Print the final:
//    - filled quantity
//    - average fill price
//    - fill timestamp
//
// This demonstrates why the immediate REST response may contain
// `filled_avg_price = null`, while the later WebSocket fill contains
// the actual execution price.
//
// Run:
//
// cargo test test_alpaca_open_with_trade_updates -- --ignored --nocapture
#[tokio::test]
#[ignore = "submits a real Alpaca order"]
async fn test_alpaca_open_with_trade_updates() -> Result<()> {
    dotenvy::dotenv().ok();


    let config =
        get_alpaca_test_config()?;


    println!(
        "\n========================================"
    );

    println!(
        "ALPACA OPEN + WEBSOCKET TEST"
    );

    println!(
        "========================================"
    );

    println!(
        "MODE: {}",
        if LIVE_TRADING {
            "LIVE"
        } else {
            "PAPER"
        }
    );


    // ========================================================
    // CONNECT WEBSOCKET BEFORE SUBMITTING ORDER
    // ========================================================

    let (
        _write,
        mut read,
    ) =
        connect_trade_updates(
            &config
        )
        .await?;


    // ========================================================
    // CREATE UNIQUE CLIENT ORDER ID
    // ========================================================

    let client_order_id =
        format!(
            "test-open-{}",
            chrono::Utc::now()
                .timestamp_millis()
        );


    println!(
        "\nCLIENT ORDER ID: {}",
        client_order_id
    );


    // ========================================================
    // SUBMIT OPEN ORDER
    // ========================================================

    let client =
        Client::new();


    let order =
        AlpacaOrderRequest {
            symbol:
                "AAPL".to_string(),

            // Small paper trade.
            notional:
                "50000".to_string(),

            side:
                "buy".to_string(),

            order_type:
                "market".to_string(),

            time_in_force:
                "day".to_string(),

            client_order_id:
                client_order_id.clone(),
        };


    let response =
        client
            .post(
                format!(
                    "{}/v2/orders",
                    config.rest_url
                )
            )
            .header(
                "APCA-API-KEY-ID",
                &config.api_key,
            )
            .header(
                "APCA-API-SECRET-KEY",
                &config.api_secret,
            )
            .json(
                &order
            )
            .send()
            .await?;


    let status =
        response.status();


    let body =
        response
            .text()
            .await?;


    println!(
        "\n========================================"
    );

    println!(
        "IMMEDIATE REST OPEN RESPONSE"
    );

    println!(
        "========================================"
    );

    println!(
        "HTTP STATUS: {}",
        status
    );

    println!(
        "{}",
        body
    );


    assert!(
        status.is_success(),
        "Alpaca open order failed: {}",
        body
    );


    let initial_order:
        AlpacaOrderResponse =
        serde_json::from_str(
            &body
        )?;


    println!(
        "\nINITIAL OPEN STATE"
    );

    println!(
        "order_id          = {}",
        initial_order.id
    );

    println!(
        "client_order_id   = {}",
        initial_order.client_order_id
    );

    println!(
        "symbol            = {}",
        initial_order.symbol
    );

    println!(
        "notional          = {:?}",
        initial_order.notional
    );

    println!(
        "qty               = {:?}",
        initial_order.qty
    );

    println!(
        "filled_qty        = {}",
        initial_order.filled_qty
    );

    println!(
        "filled_avg_price  = {:?}",
        initial_order.filled_avg_price
    );

    println!(
        "status            = {}",
        initial_order.status
    );


    // ========================================================
    // WAIT FOR REAL FILL
    // ========================================================

    println!(
        "\nWaiting for OPEN fill over WebSocket..."
    );


    let fill =
        timeout(
            Duration::from_secs(
                30
            ),

            wait_for_order_fill(
                &mut read,
                &client_order_id,
            ),
        )
        .await
        .map_err(
            |_| anyhow!(
                "Timed out waiting for OPEN fill"
            )
        )??;


    println!(
        "\n========================================"
    );

    println!(
        "FINAL OPEN FILL"
    );

    println!(
        "========================================"
    );

    println!(
        "order_id          = {}",
        fill.order.id
    );

    println!(
        "symbol            = {}",
        fill.order.symbol
    );

    println!(
        "filled_qty        = {}",
        fill.order.filled_qty
    );

    println!(
        "filled_avg_price  = {:?}",
        fill.order.filled_avg_price
    );

    println!(
        "filled_at         = {:?}",
        fill.order.filled_at
    );

    println!(
        "position_intent   = {:?}",
        fill.order.position_intent
    );


    assert_eq!(
        fill.event,
        "fill"
    );


    assert!(
        fill.order
            .filled_avg_price
            .is_some()
    );


    Ok(())
}



// ============================================================
// CLOSE POSITION + WEBSOCKET FILL TEST
// ============================================================
//
// Tests the complete closing-order lifecycle.
//
// Flow:
//
// 1. Connect to Alpaca's trading WebSocket.
// 2. Authenticate.
// 3. Subscribe to `trade_updates`.
// 4. Submit a REST request to close the current AAPL position.
// 5. Read Alpaca's generated `client_order_id` from the REST response.
// 6. Wait for WebSocket updates matching that close order.
// 7. Stop when Alpaca reports `event = "fill"`.
// 8. Print the final:
//    - closed quantity
//    - average exit price
//    - fill timestamp
//    - position intent
//
// An AAPL position must already exist before running this test.
//
// Run:
//
// cargo test test_alpaca_close_with_trade_updates -- --ignored --nocapture
#[tokio::test]
#[ignore = "closes a real Alpaca position"]
async fn test_alpaca_close_with_trade_updates() -> Result<()> {
    dotenvy::dotenv().ok();


    let config =
        get_alpaca_test_config()?;


    println!(
        "\n========================================"
    );

    println!(
        "ALPACA CLOSE + WEBSOCKET TEST"
    );

    println!(
        "========================================"
    );

    println!(
        "MODE: {}",
        if LIVE_TRADING {
            "LIVE"
        } else {
            "PAPER"
        }
    );


    // ========================================================
    // CONNECT WEBSOCKET FIRST
    // ========================================================

    let (
        _write,
        mut read,
    ) =
        connect_trade_updates(
            &config
        )
        .await?;


    // ========================================================
    // CLOSE AAPL POSITION
    // ========================================================

    let symbol =
        "AAPL";


    let client =
        Client::new();


    let response =
        client
            .delete(
                format!(
                    "{}/v2/positions/{}",
                    config.rest_url,
                    symbol
                )
            )
            .header(
                "APCA-API-KEY-ID",
                &config.api_key,
            )
            .header(
                "APCA-API-SECRET-KEY",
                &config.api_secret,
            )
            .send()
            .await?;


    let status =
        response.status();


    let body =
        response
            .text()
            .await?;


    println!(
        "\n========================================"
    );

    println!(
        "IMMEDIATE REST CLOSE RESPONSE"
    );

    println!(
        "========================================"
    );

    println!(
        "HTTP STATUS: {}",
        status
    );

    println!(
        "{}",
        body
    );


    assert!(
        status.is_success(),
        "Alpaca close failed: {}",
        body
    );


    let initial_order:
        AlpacaOrderResponse =
        serde_json::from_str(
            &body
        )?;


    println!(
        "\nINITIAL CLOSE STATE"
    );

    println!(
        "order_id          = {}",
        initial_order.id
    );

    println!(
        "client_order_id   = {}",
        initial_order.client_order_id
    );

    println!(
        "symbol            = {}",
        initial_order.symbol
    );

    println!(
        "qty               = {:?}",
        initial_order.qty
    );

    println!(
        "filled_qty        = {}",
        initial_order.filled_qty
    );

    println!(
        "filled_avg_price  = {:?}",
        initial_order.filled_avg_price
    );

    println!(
        "status            = {}",
        initial_order.status
    );


    // The close endpoint generates its own client_order_id.
    //
    // Use that ID to match the later WebSocket fill.
    let client_order_id =
        initial_order
            .client_order_id
            .clone();


    println!(
        "\nWaiting for CLOSE fill over WebSocket..."
    );


    // ========================================================
    // WAIT FOR REAL CLOSE FILL
    // ========================================================

    let fill =
        timeout(
            Duration::from_secs(
                30
            ),

            wait_for_order_fill(
                &mut read,
                &client_order_id,
            ),
        )
        .await
        .map_err(
            |_| anyhow!(
                "Timed out waiting for CLOSE fill"
            )
        )??;


    println!(
        "\n========================================"
    );

    println!(
        "FINAL CLOSE FILL"
    );

    println!(
        "========================================"
    );

    println!(
        "order_id          = {}",
        fill.order.id
    );

    println!(
        "symbol            = {}",
        fill.order.symbol
    );

    println!(
        "side              = {}",
        fill.order.side
    );

    println!(
        "filled_qty        = {}",
        fill.order.filled_qty
    );

    println!(
        "filled_avg_price  = {:?}",
        fill.order.filled_avg_price
    );

    println!(
        "filled_at         = {:?}",
        fill.order.filled_at
    );

    println!(
        "position_intent   = {:?}",
        fill.order.position_intent
    );


    assert_eq!(
        fill.event,
        "fill"
    );


    assert!(
        fill.order
            .filled_avg_price
            .is_some()
    );


    Ok(())
}