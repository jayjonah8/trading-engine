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

// Runs an IBKR PAPER limit-order cancellation test.
//
// Flow:
//
// submit non-marketable limit order
//              ↓
// wait briefly for IBKR to receive/work the order
//              ↓
// cancel the exact order using its IBKR order ID
//              ↓
// observe cancellation updates/notices
//
// The limit price should intentionally be far enough away from the market that
// the order is unlikely to fill before the cancellation request is sent.
//
// IMPORTANT:
// The same IBKR client connection that submitted the order is used to cancel it.
//
// IMPORTANT:
// This sends and cancels a real order on the configured IBKR PAPER account.
async fn run_limit_order_cancel_test(
    symbol: &str,
    quantity: u32,
    limit_price: f64,
    side: Side,
    client_id: i32,
) {
    let address = "127.0.0.1:7497";
    let cancel_delay_secs = 5;

    register_timezone_alias("Hawaii-Aleutian Standard Time", "Pacific/Honolulu");

    let side_str = match side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    };

    println!("\n----------------------------------------");
    println!("[IBKR TEST] PAPER LIMIT {side_str} CANCEL");
    println!("Symbol:       {symbol}");
    println!("Quantity:     {quantity}");
    println!("Limit Price:  ${limit_price:.2}");
    println!("Cancel Delay: {cancel_delay_secs}s");
    println!("Address:      {address}");
    println!("Client ID:    {client_id}");
    println!("----------------------------------------");

    // ------------------------------------------------------------
    // CONNECT
    // ------------------------------------------------------------

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

    // ------------------------------------------------------------
    // ORDER UPDATE STREAM
    // ------------------------------------------------------------

    let mut order_stream = match client.order_update_stream().await {
        Ok(stream) => stream,
        Err(error) => {
            println!("\n[IBKR ORDER STREAM START ERROR]");
            println!("{error:?}");
            client.disconnect().await;
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
            return;
        }
    };

    let order_id_raw = order_id.0;

    println!("[IBKR TEST] ORDER SENT | order_id={order_id}");

    // Give IBKR time to receive/work the limit order before cancelling.
    println!("[IBKR TEST] WAITING {cancel_delay_secs} SECONDS BEFORE CANCELLATION");

    tokio::time::sleep(Duration::from_secs(cancel_delay_secs)).await;

    // ------------------------------------------------------------
    // CANCEL ORDER
    //
    // Empty manual cancellation time tells IBKR to use the current time.
    // ------------------------------------------------------------

    println!("\n[IBKR TEST] CANCELLING ORDER | order_id={order_id}");

    let mut cancel_stream = match client.cancel_order(order_id_raw, "").await {
        Ok(stream) => stream,
        Err(error) => {
            println!("\n[IBKR CANCEL REQUEST ERROR]");
            println!("{error:?}");
            client.disconnect().await;
            return;
        }
    };

    println!("[IBKR TEST] CANCEL REQUEST SENT | order_id={order_id}");

    // ------------------------------------------------------------
    // OBSERVE CANCELLATION
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

                            if data.order_id == order_id_raw {
                                println!("[IBKR TEST] THIS IS THE LIMIT {side_str} ORDER");
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
                                println!("[IBKR TEST] THIS IS THE LIMIT {side_str} ORDER");

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
                                println!("[IBKR TEST] THIS EXECUTION IS FOR THE LIMIT {side_str} ORDER");
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
                                println!("[IBKR ORDER ERROR]");
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

                item = cancel_stream.next() => {
                    match item {
                        Some(Ok(SubscriptionItem::Data(data))) => {
                            println!("\n[IBKR CANCEL DATA]");
                            println!("{data:#?}");
                        }

                        Some(Ok(SubscriptionItem::Notice(notice))) => {
                            println!("\n[IBKR CANCEL NOTICE]");
                            println!("Request ID: {:?}", notice.request_id);
                            println!("Code:       {}", notice.code);
                            println!("Message:    {}", notice.message);

                            if notice.is_cancellation() {
                                println!("[IBKR TEST] CANCELLATION CONFIRMED");
                                break;
                            } else if notice.is_error() {
                                println!("[IBKR CANCEL ERROR]");
                            } else if notice.is_warning() {
                                println!("[IBKR CANCEL WARNING]");
                            } else {
                                println!("[IBKR CANCEL INFO]");
                            }
                        }

                        Some(Err(error)) => {
                            println!("\n[IBKR CANCEL STREAM ERROR]");
                            println!("{error:?}");
                            break;
                        }

                        None => {
                            println!("\n[IBKR TEST] CANCEL STREAM CLOSED");
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
        println!("\n[IBKR TEST] 15 SECOND CANCEL TIMEOUT");
        println!("No terminal order status or cancellation confirmation was received.");
    }

    client.disconnect().await;

    println!("\n[IBKR TEST] DISCONNECTED");
    println!("----------------------------------------");
}

// Run:
// cargo test test_cancel_ibkr_limit_buy -- --nocapture
//
// $100 is intentionally far below the AAPL market so the order should
// normally rest long enough for us to cancel it.
#[tokio::test]
async fn test_cancel_ibkr_limit_buy() {
    run_limit_order_cancel_test("AAPL", 1, 100.00, Side::Buy, 110).await;
}

// Run:
// cargo test test_cancel_ibkr_limit_sell -- --nocapture
//
// $500 is intentionally far above the AAPL market so the order should
// normally rest long enough for us to cancel it.
#[tokio::test]
async fn test_cancel_ibkr_limit_sell() {
    run_limit_order_cancel_test("AAPL", 1, 500.00, Side::Sell, 111).await;
}