#![allow(dead_code)]

use std::time::Duration;

use futures::StreamExt;

use ibapi::{
    prelude::*,
    register_timezone_alias,
    subscriptions::SubscriptionItem,
    Client,
};

use tokio::time::timeout;

// Run test:
// cargo test test_ibkr_account_balance -- --nocapture

/// Tests fetching account balances from the IBKR PAPER account.
///
/// Requests:
/// - NetLiquidation: total account equity
/// - TotalCashValue: total cash
/// - BuyingPower: available buying power
/// - GrossPositionValue: gross position value
///
/// NetLiquidation is the value used for Engine::General.starting_balance.
#[tokio::test]
async fn test_ibkr_account_balance() {
    let address = "127.0.0.1:7497";
    let client_id = 107;

    register_timezone_alias(
        "Hawaii-Aleutian Standard Time",
        "Pacific/Honolulu",
    );

    println!("\n----------------------------------------");
    println!("[IBKR TEST] ACCOUNT BALANCE");
    println!("Address:   {address}");
    println!("Client ID: {client_id}");
    println!("----------------------------------------");

    // ============================================================
    // CONNECT
    // ============================================================

    let client = match Client::connect(address, client_id).await {
        Ok(client) => client,
        Err(error) => {
            println!("\n[IBKR CONNECTION ERROR]");
            println!("{error:?}");
            return;
        }
    };

    println!("[IBKR TEST] CONNECTED");

    // ============================================================
    // REQUEST ACCOUNT SUMMARY
    // ============================================================

    let group = AccountGroup("All".to_string());

    let tags = [
        "NetLiquidation",
        "TotalCashValue",
        "BuyingPower",
        "GrossPositionValue",
    ];

    let mut subscription = match client.account_summary(&group, &tags).await {
        Ok(subscription) => subscription,
        Err(error) => {
            println!("\n[IBKR ACCOUNT SUMMARY REQUEST ERROR]");
            println!("{error:?}");

            client.disconnect().await;
            return;
        }
    };

    // ============================================================
    // RECEIVE ACCOUNT BALANCES
    // ============================================================

    let mut net_liquidation: Option<f64> = None;
    let mut snapshot_complete = false;

    let result = timeout(Duration::from_secs(30), async {
        while let Some(item) = subscription.next().await {
            match item {
                Ok(SubscriptionItem::Data(AccountSummaryResult::Summary(summary))) => {
                    println!("\n[IBKR ACCOUNT VALUE]");
                    println!("Account:  {}", summary.account);
                    println!("Tag:      {}", summary.tag);
                    println!("Value:    {}", summary.value);
                    println!("Currency: {}", summary.currency);

                    if summary.tag == "NetLiquidation" && summary.currency == "USD" {
                        net_liquidation = Some(
                            summary.value.parse::<f64>()
                                .expect("Invalid NetLiquidation value")
                        );
                    }
                }

                Ok(SubscriptionItem::Data(AccountSummaryResult::End)) => {
                    println!("\n[IBKR TEST] ACCOUNT SNAPSHOT COMPLETE");
                    snapshot_complete = true;
                    break;
                }

                Ok(SubscriptionItem::Notice(notice)) => {
                    println!("\n[IBKR ACCOUNT NOTICE]");
                    println!("Code:    {}", notice.code);
                    println!("Message: {}", notice.message);

                    if notice.is_error() {
                        println!("[IBKR ACCOUNT ERROR]");
                    }
                }

                Err(error) => {
                    println!("\n[IBKR ACCOUNT STREAM ERROR]");
                    println!("{error:?}");
                    break;
                }
            }
        }
    }).await;

    if result.is_err() {
        println!("\n[IBKR TEST] ACCOUNT REQUEST TIMED OUT");
    }

    // ============================================================
    // FINAL BALANCE
    // ============================================================

    println!("\n----------------------------------------");
    println!("[IBKR ACCOUNT BALANCE RESULT]");

    if let Some(balance) = net_liquidation {
        println!("Net Liquidation: ${balance:.2}");
    } else {
        println!("Net Liquidation: NOT RECEIVED");
    }

    // ============================================================
    // DISCONNECT
    // ============================================================

    drop(subscription);
    client.disconnect().await;

    println!("\n[IBKR TEST] DISCONNECTED");
    println!("----------------------------------------");

    assert!(snapshot_complete, "IBKR account snapshot did not complete");
    assert!(net_liquidation.is_some(), "IBKR NetLiquidation not received");
}

// Example Output: 
// ----------------------------------------
// [IBKR TEST] ACCOUNT BALANCE
// Address:   127.0.0.1:7497
// Client ID: 107
// ----------------------------------------
// [IBKR TEST] CONNECTED

// [IBKR ACCOUNT VALUE]
// Account:  DUT097247
// Tag:      BuyingPower
// Value:    4007634.61
// Currency: USD

// [IBKR ACCOUNT VALUE]
// Account:  DUT097247
// Tag:      GrossPositionValue
// Value:    527.81
// Currency: USD

// [IBKR ACCOUNT VALUE]
// Account:  DUT097247
// Tag:      NetLiquidation
// Value:    1002454.39
// Currency: USD

// [IBKR ACCOUNT VALUE]
// Account:  DUT097247
// Tag:      TotalCashValue
// Value:    1001555.02
// Currency: USD

// [IBKR TEST] ACCOUNT SNAPSHOT COMPLETE

// ----------------------------------------
// [IBKR ACCOUNT BALANCE RESULT]
// Net Liquidation: $1002454.39

// [IBKR TEST] DISCONNECTED
// ----------------------------------------