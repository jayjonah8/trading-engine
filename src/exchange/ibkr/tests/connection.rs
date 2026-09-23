#![allow(dead_code)]

use ibapi::{
    register_timezone_alias,
    Client,
};

// Tests that the bot can establish a connection to IBKR TWS / IB Gateway.
//
// This is intentionally a minimal connectivity test. It does not submit
// orders or start any execution streams.
//
// Run:
// cargo test test_ibkr_tws_connection -- --nocapture
#[tokio::test]
async fn test_ibkr_tws_connection() {
    let address = "127.0.0.1:7497";
    let client_id = 100;

    // TWS reports this Windows-style timezone name.
    // ibapi expects an IANA timezone alias.
    register_timezone_alias("Hawaii-Aleutian Standard Time", "Pacific/Honolulu");

    println!("\n----------------------------------------");
    println!("[IBKR TEST] TWS PAPER CONNECTION");
    println!("Address:   {address}");
    println!("Client ID: {client_id}");
    println!("----------------------------------------");

    let client = match Client::connect(address, client_id).await {
        Ok(client) => client,
        Err(error) => {
            println!("\n[IBKR CONNECTION ERROR]");
            println!("{error:?}");
            panic!("Failed to connect to IBKR TWS");
        }
    };

    println!("[IBKR TEST] CONNECTED SUCCESSFULLY");
    println!("Connected: {}", client.is_connected());

    assert!(client.is_connected(), "IBKR client should report connected");

    client.disconnect().await;

    println!("[IBKR TEST] DISCONNECTED");
    println!("----------------------------------------");
}