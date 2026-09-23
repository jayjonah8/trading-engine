#![allow(dead_code)]

use ibapi::{
    contracts::Contract,
    register_timezone_alias,
    Client,
};

// Tests IBKR contract resolution before attempting to place orders.
//
// Creates a basic stock contract using IBKR SMART routing and USD, sends it
// to TWS, and requests the full matching contract details.
//
// This verifies that the Rust bot can translate a normal ticker into IBKR's
// actual tradable instrument, including its unique contract ID (con_id),
// exchange information, trading class, and other contract metadata.
//
// Run:
// cargo test test_ibkr_aapl_contract -- --nocapture
#[tokio::test]
async fn test_ibkr_aapl_contract() {
    let address = "127.0.0.1:7497";
    let symbol = "AAPL";
    let client_id = 103;

    register_timezone_alias("Hawaii-Aleutian Standard Time", "Pacific/Honolulu");

    println!("\n----------------------------------------");
    println!("[IBKR TEST] CONTRACT RESOLUTION");
    println!("Symbol:    {symbol}");
    println!("Address:   {address}");
    println!("Client ID: {client_id}");
    println!("----------------------------------------");

    let client = match Client::connect(address, client_id).await {
        Ok(client) => client,
        Err(error) => {
            println!("\n[IBKR CONNECTION ERROR]");
            println!("{error:?}");
            return;
        }
    };

    println!("[IBKR TEST] CONNECTED");

    // Creates a stock contract using SMART routing and USD.
    let contract = Contract::stock(symbol).build();

    println!("[IBKR TEST] REQUESTING CONTRACT DETAILS | symbol={symbol}");

    let details = match client.contract_details(&contract).await {
        Ok(details) => details,
        Err(error) => {
            println!("\n[IBKR CONTRACT DETAILS ERROR]");
            println!("{error:?}");
            client.disconnect().await;
            println!("[IBKR TEST] DISCONNECTED");
            return;
        }
    };

    assert!(!details.is_empty(), "IBKR returned no contract details for {symbol}");

    println!("[IBKR TEST] CONTRACTS RETURNED: {}", details.len());

    for detail in &details {
        println!("\n[IBKR CONTRACT]");
        println!("Symbol:           {}", detail.contract.symbol);
        println!("Contract ID:      {}", detail.contract.contract_id);
        println!("Security Type:    {}", detail.contract.security_type);
        println!("Exchange:         {}", detail.contract.exchange);
        println!("Primary Exchange: {}", detail.contract.primary_exchange);
        println!("Currency:         {}", detail.contract.currency);
        println!("Local Symbol:     {}", detail.contract.local_symbol);
        println!("Trading Class:    {}", detail.contract.trading_class);
    }

    client.disconnect().await;

    println!("\n[IBKR TEST] DISCONNECTED");
    println!("----------------------------------------");
}