use anyhow::Result;
use reqwest::Client;
use serde::{
    Deserialize,
    Serialize,
};


// ============================================================
// ALPACA ORDER TYPES
// ============================================================

#[derive(Debug, Serialize)]
struct AlpacaOrderRequest {
    // Stock ticker symbol to trade.
    symbol: String,

    // Dollar amount of the position to buy or sell.
    notional: String,

    // Order direction: "buy" or "sell".
    side: String,

    // Order type, such as "market" or "limit".
    #[serde(rename = "type")]
    order_type: String,

    // How long the order remains active, such as "day".
    time_in_force: String,
}


// ============================================================
// ALPACA ORDER RESPONSE
// ============================================================

#[derive(Debug, Deserialize)]
struct AlpacaOrderResponse {
    // Unique Alpaca identifier for the order.
    id: String,

    // Client-side identifier associated with the order.
    client_order_id: String,

    // Stock ticker symbol associated with the order.
    symbol: String,

    // Number of shares requested when the order uses quantity.
    qty: Option<String>,

    // Dollar amount requested when the order uses notional sizing.
    notional: Option<String>,

    // Number of shares that have actually been filled.
    filled_qty: String,

    // Average execution price of the filled shares.
    filled_avg_price: Option<String>,

    // Order direction: "buy" or "sell".
    side: String,

    // Order type, such as "market" or "limit".
    #[serde(rename = "type")]
    order_type: String,

    // How long the order remains active, such as "day".
    time_in_force: String,

    // Current order state, such as "new", "filled", or "canceled".
    status: String,
}


// ============================================================
// ALPACA TRADING MODE
// ============================================================

// false = Paper trading
// true  = Live trading
const LIVE_TRADING: bool = false;


// ============================================================
// OPEN ALPACA TRADE TEST
// ============================================================

// Run:
// cargo test test_open_alpaca_trade -- --ignored --nocapture

#[tokio::test]
#[ignore = "submits a real Alpaca order"]
async fn test_open_alpaca_trade() -> Result<()> {
    dotenvy::dotenv().ok();

    // Select credentials + API URL based on trading mode.
    let (api_key, api_secret, base_url) =
        if LIVE_TRADING {
            (
                std::env::var("ALPACA_API_KEY_ID")?,
                std::env::var("ALPACA_API_SECRET_KEY")?,
                "https://api.alpaca.markets",
            )
        } else {
            (
                std::env::var("ALPACA_PAPER_API_KEY_ID")?,
                std::env::var("ALPACA_PAPER_API_SECRET_KEY")?,
                "https://paper-api.alpaca.markets",
            )
        };

    println!(
        "\nALPACA MODE: {}",
        if LIVE_TRADING { "LIVE" } else { "PAPER" }
    );

    let client =
        Client::new();

    // Buy approximately $10 worth of AAPL.
    //
    // Using `notional` allows Alpaca to calculate the
    // fractional share quantity automatically.
    let order =
        AlpacaOrderRequest {
            symbol: "AAPL".to_string(),
            notional: "1500".to_string(),
            side: "buy".to_string(),
            order_type: "market".to_string(),
            time_in_force: "day".to_string(),
        };

    let response =
        client
            .post(
                format!(
                    "{}/v2/orders",
                    base_url
                )
            )
            .header(
                "APCA-API-KEY-ID",
                api_key,
            )
            .header(
                "APCA-API-SECRET-KEY",
                api_secret,
            )
            .json(&order)
            .send()
            .await?;

    let status =
        response.status();

    let body =
        response.text().await?;

    println!(
        "\nALPACA OPEN STATUS: {}",
        status
    );

    println!(
        "ALPACA OPEN RESPONSE:\n{}",
        body
    );

    assert!(
        status.is_success(),
        "Alpaca open order failed: {}",
        body
    );

    Ok(())
}


// ============================================================
// CLOSE ALPACA TRADE TEST
// ============================================================

// Run:
// cargo test test_close_alpaca_trade -- --ignored --nocapture

#[tokio::test]
#[ignore = "closes a real Alpaca position"]
async fn test_close_alpaca_trade() -> Result<()> {
    dotenvy::dotenv().ok();

    // Select credentials + API URL based on trading mode.
    let (api_key, api_secret, base_url) =
        if LIVE_TRADING {
            (
                std::env::var("ALPACA_API_KEY_ID")?,
                std::env::var("ALPACA_API_SECRET_KEY")?,
                "https://api.alpaca.markets",
            )
        } else {
            (
                std::env::var("ALPACA_PAPER_API_KEY_ID")?,
                std::env::var("ALPACA_PAPER_API_SECRET_KEY")?,
                "https://paper-api.alpaca.markets",
            )
        };

    println!(
        "\nALPACA MODE: {}",
        if LIVE_TRADING { "LIVE" } else { "PAPER" }
    );

    let client =
        Client::new();

    // Close the entire currently open AAPL position.
    //
    // Alpaca determines whether the position is long or short
    // and submits the appropriate liquidation order.
    let symbol =
        "AAPL";

    let response =
        client
            .delete(
                format!(
                    "{}/v2/positions/{}",
                    base_url,
                    symbol
                )
            )
            .header(
                "APCA-API-KEY-ID",
                api_key,
            )
            .header(
                "APCA-API-SECRET-KEY",
                api_secret,
            )
            .send()
            .await?;

    let status =
        response.status();

    let body =
        response.text().await?;

    println!(
        "\nALPACA CLOSE STATUS: {}",
        status
    );

    println!(
        "ALPACA CLOSE RESPONSE:\n{}",
        body
    );

    assert!(
        status.is_success(),
        "Alpaca close position failed: {}",
        body
    );

    Ok(())
}
