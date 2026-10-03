#![allow(dead_code)]

use futures::StreamExt;
use ibapi::{
    market_data::realtime::{generic_tick, TickTypes},
    prelude::*,
    register_timezone_alias,
    subscriptions::SubscriptionItem,
    Client,
};
use tokio::time::{timeout, Duration};

// Tests IBKR shortability market data for a stock.
//
// Requests generic tick 236 (SHORTABLE), which subscribes to:
// - Tick 46: Shortable indicator
// - Tick 89: Shortable Shares
//
// This test intentionally prints the complete market-data messages so we can
// verify exactly how the installed ibapi version exposes IBKR shortability
// data before mapping it into the trading engine.
//
// Run:
// cargo test test_ibkr_shortable -- --nocapture
//
// Error: "Requested market data is not subscribed. Delayed market data is not enabled."
//
// IBKR market data types control whether TWS / IB Gateway returns live or
// delayed market data for API requests. Type 1 = Live, Type 2 = Frozen,
// Type 3 = Delayed, and Type 4 = Delayed Frozen. Live data requires the
// appropriate market-data subscription, while Delayed data can be used when
// live data is unavailable and IBKR provides delayed data for that instrument.
// Requesting Type 3 does not force delayed data when live data is available;
// IBKR may return live data instead when the account has the required entitlement.
#[tokio::test]
async fn test_ibkr_shortable() {
    let address = "127.0.0.1:7497";
    let symbol = "AAPL";
    let client_id = 107;

    register_timezone_alias("Hawaii-Aleutian Standard Time", "Pacific/Honolulu");

    println!("\n========================================");
    println!("[IBKR TEST] SHORTABLE DATA");
    println!("========================================");
    println!("Symbol:    {symbol}");
    println!("Address:   {address}");
    println!("Client ID: {client_id}");
    println!("========================================");

    let client = match Client::connect(address, client_id).await {
        Ok(client) => client,
        Err(error) => {
            println!("\n[IBKR CONNECTION ERROR]");
            println!("{error:?}");
            return;
        }
    };

    println!("\n[IBKR TEST] CONNECTED");

    let contract = Contract::stock(symbol).build();

    println!("[IBKR TEST] REQUESTING SHORTABLE DATA | symbol={symbol} | generic_tick=236");

    let mut subscription = match client
        .market_data(&contract)
        .add_generic_tick(generic_tick::SHORTABLE)
        .subscribe()
        .await
    {
        Ok(subscription) => subscription,
        Err(error) => {
            println!("\n[IBKR MARKET DATA ERROR]");
            println!("{error:?}");
            client.disconnect().await;
            return;
        }
    };

    let result = timeout(Duration::from_secs(10), async {
        while let Some(item) = subscription.next().await {
            match item {
                Ok(SubscriptionItem::Data(tick)) => {
                    println!("\n[IBKR MARKET DATA]");
                    println!("{tick:#?}");

                    match tick {
                        TickTypes::Generic(data) => {
                            println!("[IBKR GENERIC TICK]");
                            println!("Tick Type: {:?}", data.tick_type);
                            println!("Value:     {}", data.value);
                        }

                        TickTypes::Size(data) => {
                            println!("[IBKR SIZE TICK]");
                            println!("{data:#?}");
                        }

                        _ => {}
                    }
                }

                Ok(SubscriptionItem::Notice(notice)) => {
                    println!("\n[IBKR MARKET DATA NOTICE]");
                    println!("Request ID: {:?}", notice.request_id);
                    println!("Code:       {}", notice.code);
                    println!("Message:    {}", notice.message);
                }

                Err(error) => {
                    println!("\n[IBKR MARKET DATA STREAM ERROR]");
                    println!("{error:?}");
                    break;
                }
            }
        }
    })
    .await;

    if result.is_err() {
        println!("\n[IBKR TEST] 10 SECOND TIMEOUT");
        println!("Finished collecting shortability market data.");
    }

    client.disconnect().await;

    println!("\n[IBKR TEST] DISCONNECTED");
    println!("========================================");
}