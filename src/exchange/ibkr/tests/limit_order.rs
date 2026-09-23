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

// Runs a generic IBKR PAPER limit-order test.
//
// This test:
// - connects to TWS / IB Gateway
// - starts the complete order update stream before submission
// - starts the global notice stream
// - submits a limit BUY or SELL
// - prints OpenOrder, OrderStatus, ExecutionData and CommissionReport
// - prints order-specific and global IBKR notices
// - stops when this order becomes terminal, is rejected, or times out
//
// IMPORTANT:
// submit() returning an OrderId only proves the order was sent to TWS.
// Acceptance, routing and fills are determined from subsequent IBKR updates.
//
// A limit price away from the market can be used to intentionally leave the
// order resting so its working state can be observed.
//
// IMPORTANT:
// This sends a real order to the configured IBKR PAPER account.
async fn run_limit_order_test(
    symbol: &str,
    quantity: u32,
    limit_price: f64,
    side: Side,
    client_id: i32,
) {
    let address = "127.0.0.1:7497";

    register_timezone_alias("Hawaii-Aleutian Standard Time", "Pacific/Honolulu");

    let side_str = match side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    };

    println!("\n----------------------------------------");
    println!("[IBKR TEST] PAPER LIMIT {side_str}");
    println!("Symbol:      {symbol}");
    println!("Quantity:    {quantity}");
    println!("Limit Price: ${limit_price:.2}");
    println!("Address:     {address}");
    println!("Client ID:   {client_id}");
    println!("----------------------------------------");

    // ------------------------------------------------------------
    // CONNECT
    // ------------------------------------------------------------

    let client = match Client::connect(address, client_id).await {
        Ok(client) => client,
        Err(error) => {
            println!("\n[IBKR CONNECTION ERROR]");
            println!("{error:?}");
            println!("----------------------------------------");
            return;
        }
    };

    println!("[IBKR TEST] CONNECTED");

    let contract = Contract::stock(symbol).build();

    // ------------------------------------------------------------
    // ORDER UPDATE STREAM
    //
    // Do not use filter_data().
    // Order warnings/rejections may arrive through
    // SubscriptionItem::Notice.
    // ------------------------------------------------------------

    let mut order_stream = match client.order_update_stream().await {
        Ok(stream) => stream,
        Err(error) => {
            println!("\n[IBKR ORDER STREAM START ERROR]");
            println!("{error:?}");
            client.disconnect().await;
            println!("[IBKR TEST] DISCONNECTED");
            println!("----------------------------------------");
            return;
        }
    };

    // ------------------------------------------------------------
    // GLOBAL NOTICE STREAM
    // ------------------------------------------------------------

    let mut notice_stream = match client.notice_stream() {
        Ok(stream) => stream,
        Err(error) => {
            println!("\n[IBKR GLOBAL NOTICE STREAM START ERROR]");
            println!("{error:?}");
            client.disconnect().await;
            println!("[IBKR TEST] DISCONNECTED");
            println!("----------------------------------------");
            return;
        }
    };

    println!("[IBKR TEST] ORDER UPDATE STREAM STARTED");
    println!("[IBKR TEST] GLOBAL NOTICE STREAM STARTED");

    tokio::time::sleep(Duration::from_millis(250)).await;

    // ------------------------------------------------------------
    // SUBMIT LIMIT ORDER
    // ------------------------------------------------------------

    println!(
        "\n[IBKR TEST] SUBMITTING LIMIT {side_str} | symbol={symbol} | quantity={quantity} | limit=${limit_price:.2}"
    );

    let order_result = match side {
        Side::Buy => client.order(&contract).buy(quantity).limit(limit_price).submit().await,
        Side::Sell => client.order(&contract).sell(quantity).limit(limit_price).submit().await,
    };

    let order_id = match order_result {
        Ok(order_id) => order_id,
        Err(error) => {
            println!("\n[IBKR ORDER SUBMIT ERROR]");
            println!("{error:?}");
            client.disconnect().await;
            println!("[IBKR TEST] DISCONNECTED");
            println!("----------------------------------------");
            return;
        }
    };

    let order_id_raw = order_id.0;

    println!("[IBKR TEST] ORDER SENT | order_id={order_id}");

    // ------------------------------------------------------------
    // LISTEN FOR ORDER LIFECYCLE
    // ------------------------------------------------------------

    let wait_result = timeout(Duration::from_secs(15), async {
        loop {
            tokio::select! {
                item = order_stream.next() => {
                    match item {
                        Some(Ok(SubscriptionItem::Data(OrderUpdate::OpenOrder(data)))) => {
                            println!("\n[IBKR OPEN ORDER]");
                            println!("Order ID: {}", data.order_id);
                            println!("Symbol:   {}", data.contract.symbol);
                            println!("State:    {}", data.order_state.status);
                            println!("{data:#?}");

                            if data.order_id == order_id_raw {
                                println!("[IBKR TEST] THIS IS A LIMIT {side_str} ORDER");
                            }
                        }

                        Some(Ok(SubscriptionItem::Data(OrderUpdate::OrderStatus(status)))) => {
                            println!("\n[IBKR ORDER STATUS]");
                            println!("Order ID:    {}", status.order_id);
                            println!("Status:      {}", status.status);
                            println!("Filled:      {}", status.filled);
                            println!("Remaining:   {}", status.remaining);
                            println!("Avg Fill Px: {:?}", status.average_fill_price);
                            println!("Why Held:    {}", status.why_held);

                            if status.order_id == order_id_raw {
                                println!("[IBKR TEST] THIS IS A LIMIT {side_str} ORDER");

                                if status.status.is_terminal() {
                                    println!("[IBKR TEST] TERMINAL STATUS: {}", status.status);
                                    break;
                                }
                            }
                        }

                        Some(Ok(SubscriptionItem::Data(OrderUpdate::ExecutionData(data)))) => {
                            println!("\n[IBKR EXECUTION]");
                            println!("Order ID:     {}", data.execution.order_id);
                            println!("Execution ID: {}", data.execution.execution_id);
                            println!("Side:         {}", data.execution.side);
                            println!("Shares:       {}", data.execution.shares);
                            println!("Price:        {}", data.execution.price);
                            println!("Exchange:     {}", data.execution.exchange);
                            println!("Time:         {}", data.execution.time);

                            if data.execution.order_id == order_id_raw {
                                println!("[IBKR TEST] THIS EXECUTION IS FOR A LIMIT {side_str} ORDER");
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

                            if notice.is_error() {
                                println!("[IBKR ORDER ERROR / REJECTION]");

                                if notice.request_id == Some(order_id_raw) {
                                    println!("[IBKR TEST] THIS LIMIT {side_str} ORDER WAS REJECTED");
                                    break;
                                }
                            } else if notice.is_warning() {
                                println!("[IBKR ORDER WARNING]");
                            } else if notice.is_system_message() {
                                println!("[IBKR ORDER SYSTEM MESSAGE]");
                            } else {
                                println!("[IBKR ORDER INFO]");
                            }
                        }

                        Some(Err(error)) => {
                            println!("\n[IBKR ORDER STREAM ERROR]");
                            println!("{error:?}");
                            break;
                        }

                        None => {
                            println!("\n[IBKR TEST] ORDER STREAM CLOSED");
                            break;
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

    if wait_result.is_err() {
        println!("\n[IBKR TEST] 15 SECOND TIMEOUT");
        println!("No terminal status or rejection received within 15 seconds.");
        println!("The limit order may still be active/resting in IBKR.");
    }

    client.disconnect().await;

    println!("\n[IBKR TEST] DISCONNECTED");
    println!("----------------------------------------");
}

// Run:
// cargo test test_ibkr_limit_buy -- --nocapture
//
// $100 is intentionally far below AAPL's current market price so the order
// should normally remain resting rather than execute immediately.
//
// IMPORTANT:
// This sends a real BUY order to the configured IBKR PAPER account.
#[tokio::test]
async fn test_ibkr_limit_buy() {
    run_limit_order_test("AAPL", 1, 100.00, Side::Buy, 107).await;
}

// Run:
// cargo test test_ibkr_limit_sell -- --nocapture
//
// $500 is intentionally far above AAPL's current market price so the order
// should normally remain resting rather than execute immediately.
//
// Depending on account state, a SELL may reduce a long position or establish
// short exposure if the order eventually executes.
//
// IMPORTANT:
// This sends a real SELL order to the configured IBKR PAPER account.
#[tokio::test]
async fn test_ibkr_limit_sell() {
    run_limit_order_test("AAPL", 1, 500.00, Side::Sell, 108).await;
}