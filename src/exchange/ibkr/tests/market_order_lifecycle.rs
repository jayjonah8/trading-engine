#![allow(dead_code)]

use futures::StreamExt;
use ibapi::{
    prelude::*,
    register_timezone_alias,
    subscriptions::SubscriptionItem,
    Client,
};
use tokio::time::{timeout, Duration};

use crate::core::execution::Side;

// Tests a complete IBKR PAPER market-order lifecycle:
//
// 1. Submit a market entry order.
// 2. Wait until the entry is completely filled.
// 3. Hold the position for a configured number of seconds.
// 4. Submit the opposite market order.
// 5. Wait until the exit is completely filled.
//
// The same order update stream remains active for the entire lifecycle.
//
// IMPORTANT:
// This sends real orders to the configured IBKR PAPER account.
//
// Run:
// cargo test test_ibkr_market_order_lifecycle -- --nocapture
#[tokio::test]
async fn test_ibkr_market_order_lifecycle() {
    let address = "127.0.0.1:7497";
    let client_id = 109;
    let symbol = "AAPL";
    let quantity = 1;
    let entry_side = Side::Buy;
    let exit_side = entry_side.opposite();
    let hold_secs = 5;

    let entry_side_str = match entry_side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    };

    let exit_side_str = match exit_side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    };

    register_timezone_alias("Hawaii-Aleutian Standard Time", "Pacific/Honolulu");

    println!("\n============================================================");
    println!("[IBKR TEST] PAPER MARKET ORDER LIFECYCLE");
    println!("Symbol:     {symbol}");
    println!("Quantity:   {quantity}");
    println!("Entry:      MARKET {entry_side_str}");
    println!("Exit:       MARKET {exit_side_str}");
    println!("Hold Time:  {hold_secs}s");
    println!("Address:    {address}");
    println!("Client ID:  {client_id}");
    println!("============================================================");

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

    let contract = Contract::stock(symbol).build();

    // ============================================================
    // ORDER UPDATE STREAM
    // ============================================================

    let mut order_stream = match client.order_update_stream().await {
        Ok(stream) => stream,

        Err(error) => {
            println!("\n[IBKR ORDER STREAM START ERROR]");
            println!("{error:?}");
            client.disconnect().await;
            return;
        }
    };

    // ============================================================
    // GLOBAL NOTICE STREAM
    // ============================================================

    let mut notice_stream = match client.notice_stream() {
        Ok(stream) => stream,

        Err(error) => {
            println!("\n[IBKR GLOBAL NOTICE STREAM START ERROR]");
            println!("{error:?}");
            client.disconnect().await;
            return;
        }
    };

    println!("[IBKR TEST] ORDER UPDATE STREAM STARTED");
    println!("[IBKR TEST] GLOBAL NOTICE STREAM STARTED");

    tokio::time::sleep(Duration::from_millis(250)).await;

    // ============================================================
    // ENTRY
    // ============================================================

    println!("\n[IBKR TEST] SUBMITTING ENTRY | MARKET {entry_side_str} | symbol={symbol} | quantity={quantity}");

    let entry_order_id = match entry_side {
        Side::Buy => client.order(&contract).buy(quantity).market().submit().await,
        Side::Sell => client.order(&contract).sell(quantity).market().submit().await,
    };

    let entry_order_id = match entry_order_id {
        Ok(order_id) => order_id,

        Err(error) => {
            println!("\n[IBKR ENTRY SUBMIT ERROR]");
            println!("{error:?}");
            client.disconnect().await;
            return;
        }
    };

    let entry_order_id_raw = entry_order_id.0;

    println!("[IBKR TEST] ENTRY ORDER SENT | order_id={entry_order_id}");

    let entry_result = timeout(Duration::from_secs(30), async {
        loop {
            tokio::select! {
                item = order_stream.next() => {
                    match item {
                        Some(Ok(SubscriptionItem::Data(OrderUpdate::OpenOrder(data)))) => {
                            println!("\n[IBKR OPEN ORDER]");
                            println!("Phase:    ENTRY");
                            println!("Order ID: {}", data.order_id);
                            println!("Symbol:   {}", data.contract.symbol);
                            println!("State:    {}", data.order_state.status);

                            if data.order_id == entry_order_id_raw {
                                println!("[IBKR TEST] THIS IS A ENTRY {entry_side_str} ORDER");
                            }
                        }

                        Some(Ok(SubscriptionItem::Data(OrderUpdate::OrderStatus(status)))) => {
                            println!("\n[IBKR ORDER STATUS]");
                            println!("Phase:       ENTRY");
                            println!("Order ID:    {}", status.order_id);
                            println!("Status:      {}", status.status);
                            println!("Filled:      {}", status.filled);
                            println!("Remaining:   {}", status.remaining);
                            println!("Avg Fill Px: {:?}", status.average_fill_price);
                            println!("Why Held:    {}", status.why_held);

                            if status.order_id == entry_order_id_raw {
                                println!("[IBKR TEST] THIS IS A ENTRY {entry_side_str} ORDER");

                                if status.status.is_terminal() {
                                    if status.filled >= quantity as f64 && status.remaining <= 0.0 {
                                        println!("[IBKR TEST] ENTRY COMPLETELY FILLED");
                                        break true;
                                    }

                                    println!("[IBKR TEST] ENTRY TERMINAL WITHOUT COMPLETE FILL");
                                    break false;
                                }
                            }
                        }

                        Some(Ok(SubscriptionItem::Data(OrderUpdate::ExecutionData(data)))) => {
                            println!("\n[IBKR EXECUTION]");
                            println!("Phase:        ENTRY");
                            println!("Order ID:     {}", data.execution.order_id);
                            println!("Execution ID: {}", data.execution.execution_id);
                            println!("Shares:       {}", data.execution.shares);
                            println!("Price:        {}", data.execution.price);
                            println!("Exchange:     {}", data.execution.exchange);

                            if data.execution.order_id == entry_order_id_raw {
                                println!("[IBKR TEST] THIS EXECUTION IS FOR A ENTRY {entry_side_str} ORDER");
                            }
                        }

                        Some(Ok(SubscriptionItem::Data(OrderUpdate::CommissionReport(report)))) => {
                            println!("\n[IBKR COMMISSION]");
                            println!("{report:#?}");
                        }

                        Some(Ok(SubscriptionItem::Notice(notice))) => {
                            println!("\n[IBKR ORDER NOTICE]");
                            println!("Request ID: {:?}", notice.request_id);
                            println!("Code:       {}", notice.code);
                            println!("Message:    {}", notice.message);

                            if notice.is_error() && notice.request_id == Some(entry_order_id_raw) {
                                println!("[IBKR TEST] ENTRY {entry_side_str} ORDER REJECTED");
                                break false;
                            } else if notice.is_warning() {
                                println!("[IBKR ORDER WARNING]");
                            } else if notice.is_system_message() {
                                println!("[IBKR ORDER SYSTEM MESSAGE]");
                            }
                        }

                        Some(Err(error)) => {
                            println!("\n[IBKR ORDER STREAM ERROR]");
                            println!("{error:?}");
                            break false;
                        }

                        None => {
                            println!("\n[IBKR TEST] ORDER STREAM CLOSED");
                            break false;
                        }
                    }
                }

                notice = notice_stream.next() => {
                    match notice {
                        Some(notice) => {
                            println!("\n[IBKR GLOBAL NOTICE]");
                            println!("Request ID: {:?}", notice.request_id);
                            println!("Code:       {}", notice.code);
                            println!("Message:    {}", notice.message);

                            if notice.is_system_message() {
                                println!("[IBKR SYSTEM MESSAGE]");
                            } else if notice.is_error() {
                                println!("[IBKR GLOBAL ERROR]");
                            } else if notice.is_warning() {
                                println!("[IBKR GLOBAL WARNING]");
                            } else {
                                println!("[IBKR GLOBAL INFO]");
                            }
                        }

                        None => {
                            println!("\n[IBKR TEST] GLOBAL NOTICE STREAM CLOSED");
                        }
                    }
                }
            }
        }
    })
    .await;

    let entry_filled = match entry_result {
        Ok(filled) => filled,

        Err(_) => {
            println!("\n[IBKR TEST] ENTRY TIMEOUT");
            println!("Entry did not reach a terminal status within 30 seconds.");
            false
        }
    };

    if !entry_filled {
        println!("\n[IBKR TEST] ENTRY DID NOT COMPLETE — EXIT WILL NOT BE SUBMITTED");
        client.disconnect().await;
        println!("[IBKR TEST] DISCONNECTED");
        return;
    }

    // ============================================================
    // HOLD
    // ============================================================

    println!("\n[IBKR TEST] ENTRY FILLED");
    println!("[IBKR TEST] HOLDING POSITION FOR {hold_secs} SECONDS");

    tokio::time::sleep(Duration::from_secs(hold_secs)).await;

    // ============================================================
    // EXIT
    // ============================================================

    println!("\n[IBKR TEST] SUBMITTING EXIT | MARKET {exit_side_str} | symbol={symbol} | quantity={quantity}");

    let exit_order_id = match exit_side {
        Side::Buy => client.order(&contract).buy(quantity).market().submit().await,
        Side::Sell => client.order(&contract).sell(quantity).market().submit().await,
    };

    let exit_order_id = match exit_order_id {
        Ok(order_id) => order_id,

        Err(error) => {
            println!("\n[IBKR EXIT SUBMIT ERROR]");
            println!("{error:?}");
            client.disconnect().await;
            return;
        }
    };

    let exit_order_id_raw = exit_order_id.0;

    println!("[IBKR TEST] EXIT ORDER SENT | order_id={exit_order_id}");

    let exit_result = timeout(Duration::from_secs(30), async {
        loop {
            tokio::select! {
                item = order_stream.next() => {
                    match item {
                        Some(Ok(SubscriptionItem::Data(OrderUpdate::OpenOrder(data)))) => {
                            println!("\n[IBKR OPEN ORDER]");
                            println!("Phase:    EXIT");
                            println!("Order ID: {}", data.order_id);
                            println!("Symbol:   {}", data.contract.symbol);
                            println!("State:    {}", data.order_state.status);

                            if data.order_id == exit_order_id_raw {
                                println!("[IBKR TEST] THIS IS A EXIT {exit_side_str} ORDER");
                            }
                        }

                        Some(Ok(SubscriptionItem::Data(OrderUpdate::OrderStatus(status)))) => {
                            println!("\n[IBKR ORDER STATUS]");
                            println!("Phase:       EXIT");
                            println!("Order ID:    {}", status.order_id);
                            println!("Status:      {}", status.status);
                            println!("Filled:      {}", status.filled);
                            println!("Remaining:   {}", status.remaining);
                            println!("Avg Fill Px: {:?}", status.average_fill_price);
                            println!("Why Held:    {}", status.why_held);

                            if status.order_id == exit_order_id_raw {
                                println!("[IBKR TEST] THIS IS A EXIT {exit_side_str} ORDER");

                                if status.status.is_terminal() {
                                    if status.filled >= quantity as f64 && status.remaining <= 0.0 {
                                        println!("[IBKR TEST] EXIT COMPLETELY FILLED");
                                        break true;
                                    }

                                    println!("[IBKR TEST] EXIT TERMINAL WITHOUT COMPLETE FILL");
                                    break false;
                                }
                            }
                        }

                        Some(Ok(SubscriptionItem::Data(OrderUpdate::ExecutionData(data)))) => {
                            println!("\n[IBKR EXECUTION]");
                            println!("Phase:        EXIT");
                            println!("Order ID:     {}", data.execution.order_id);
                            println!("Execution ID: {}", data.execution.execution_id);
                            println!("Shares:       {}", data.execution.shares);
                            println!("Price:        {}", data.execution.price);
                            println!("Exchange:     {}", data.execution.exchange);

                            if data.execution.order_id == exit_order_id_raw {
                                println!("[IBKR TEST] THIS EXECUTION IS FOR A EXIT {exit_side_str} ORDER");
                            }
                        }

                        Some(Ok(SubscriptionItem::Data(OrderUpdate::CommissionReport(report)))) => {
                            println!("\n[IBKR COMMISSION]");
                            println!("{report:#?}");
                        }

                        Some(Ok(SubscriptionItem::Notice(notice))) => {
                            println!("\n[IBKR ORDER NOTICE]");
                            println!("Request ID: {:?}", notice.request_id);
                            println!("Code:       {}", notice.code);
                            println!("Message:    {}", notice.message);

                            if notice.is_error() && notice.request_id == Some(exit_order_id_raw) {
                                println!("[IBKR TEST] EXIT {exit_side_str} ORDER REJECTED");
                                break false;
                            } else if notice.is_warning() {
                                println!("[IBKR ORDER WARNING]");
                            } else if notice.is_system_message() {
                                println!("[IBKR ORDER SYSTEM MESSAGE]");
                            }
                        }

                        Some(Err(error)) => {
                            println!("\n[IBKR ORDER STREAM ERROR]");
                            println!("{error:?}");
                            break false;
                        }

                        None => {
                            println!("\n[IBKR TEST] ORDER STREAM CLOSED");
                            break false;
                        }
                    }
                }

                notice = notice_stream.next() => {
                    match notice {
                        Some(notice) => {
                            println!("\n[IBKR GLOBAL NOTICE]");
                            println!("Request ID: {:?}", notice.request_id);
                            println!("Code:       {}", notice.code);
                            println!("Message:    {}", notice.message);

                            if notice.is_system_message() {
                                println!("[IBKR SYSTEM MESSAGE]");
                            } else if notice.is_error() {
                                println!("[IBKR GLOBAL ERROR]");
                            } else if notice.is_warning() {
                                println!("[IBKR GLOBAL WARNING]");
                            } else {
                                println!("[IBKR GLOBAL INFO]");
                            }
                        }

                        None => {
                            println!("\n[IBKR TEST] GLOBAL NOTICE STREAM CLOSED");
                        }
                    }
                }
            }
        }
    })
    .await;

    let exit_filled = match exit_result {
        Ok(filled) => filled,

        Err(_) => {
            println!("\n[IBKR TEST] EXIT TIMEOUT");
            println!("Exit did not reach a terminal status within 30 seconds.");
            false
        }
    };

    // ============================================================
    // RESULT
    // ============================================================

    if exit_filled {
        println!("\n============================================================");
        println!("[IBKR TEST] MARKET ORDER LIFECYCLE COMPLETE");
        println!("Entry: MARKET {entry_side_str} FILLED");
        println!("Hold:  {hold_secs}s");
        println!("Exit:  MARKET {exit_side_str} FILLED");
        println!("============================================================");
    } else {
        println!("\n[IBKR TEST] EXIT DID NOT COMPLETE");
    }

    client.disconnect().await;

    println!("[IBKR TEST] DISCONNECTED");
}