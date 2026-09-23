#![allow(dead_code)]

use futures::StreamExt;
use ibapi::{
    prelude::*,
    register_timezone_alias,
    subscriptions::SubscriptionItem,
    Client,
};

// Tests that Rust can request the current positions held in the IBKR PAPER
// account.
//
// The positions subscription returns the initial portfolio snapshot, including
// every currently held position, followed by PositionEnd when the initial
// snapshot is complete.
//
// The subscription also listens for IBKR notices/errors related to the
// positions request.
//
// An empty snapshot followed by PositionEnd is valid and means the account
// currently has no open positions.
//
// Run:
// cargo test test_ibkr_positions -- --nocapture
#[tokio::test]
async fn test_ibkr_positions() {
    let address = "127.0.0.1:7497";
    let client_id = 102;

    register_timezone_alias("Hawaii-Aleutian Standard Time", "Pacific/Honolulu");

    println!("\n----------------------------------------");
    println!("[IBKR TEST] POSITIONS");
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

    let mut subscription = match client.positions().await {
        Ok(subscription) => subscription,
        Err(error) => {
            println!("\n[IBKR POSITIONS REQUEST ERROR]");
            println!("{error:?}");
            client.disconnect().await;
            println!("[IBKR TEST] DISCONNECTED");
            return;
        }
    };

    while let Some(item) = subscription.next().await {
        match item {
            Ok(SubscriptionItem::Data(PositionUpdate::Position(position))) => {
                println!("\n[IBKR POSITION]");
                println!("{position:#?}");
            }

            Ok(SubscriptionItem::Data(PositionUpdate::PositionEnd)) => {
                println!("\n[IBKR TEST] INITIAL POSITION SNAPSHOT COMPLETE");
                break;
            }

            Ok(SubscriptionItem::Notice(notice)) => {
                println!("\n[IBKR POSITION NOTICE]");
                println!("Request ID: {:?}", notice.request_id);
                println!("Code:       {}", notice.code);
                println!("Message:    {}", notice.message);

                if notice.is_error() {
                    println!("[IBKR POSITION ERROR]");
                } else if notice.is_warning() {
                    println!("[IBKR POSITION WARNING]");
                } else if notice.is_system_message() {
                    println!("[IBKR POSITION SYSTEM MESSAGE]");
                } else {
                    println!("[IBKR POSITION INFO]");
                }
            }

            Err(error) => {
                println!("\n[IBKR POSITIONS STREAM ERROR]");
                println!("{error:?}");
                break;
            }
        }
    }

    client.disconnect().await;

    println!("\n[IBKR TEST] DISCONNECTED");
    println!("----------------------------------------");
}