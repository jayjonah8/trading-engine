use futures::StreamExt;
use ibapi::{
    accounts::{
        types::AccountGroup,
        AccountSummaryTags,
    },
    register_timezone_alias,
    Client,
};

use ibapi::{
    accounts::PositionUpdate,
    subscriptions::SubscriptionItem,
};


// ============================================================
// 
// ============================================================


// Tests that the Rust bot can submit a normal 1-share AAPL MARKET BUY to the
// IBKR PAPER account while listening for BOTH complete order lifecycle updates
// and IBKR notices/errors.
//
// IMPORTANT BEHAVIOR:
// `submit()` returning an OrderId only means the order was sent to TWS.
// It does NOT prove IBKR accepted, routed, or filled the order.
//
// The order update stream is started BEFORE submission so the bot can receive:
// - OpenOrder
// - OrderStatus
// - ExecutionData
// - CommissionReport
// - order-specific warnings
// - order-specific rejection/error messages
//
// A separate global notice stream is also started so the bot can receive
// connectivity/system-level IBKR messages that are not tied to one order.
//
// This test intentionally prints ALL order and notice messages so we can see
// exactly what IBKR sends. Only messages belonging to this exact order are
// treated as authoritative for ending the test.
//
// Because the market may be closed, a valid order may remain PreSubmitted
// instead of filling. The test waits up to 15 seconds unless the order reaches
// a terminal status or IBKR rejects this exact order first.
//
// IMPORTANT: This sends an actual order to the IBKR PAPER account.
//
// Run:
// cargo test test_ibkr_aapl_market_buy -- --nocapture
#[tokio::test]
async fn test_ibkr_aapl_market_buy() {
    use ibapi::prelude::*;
    use tokio::time::{timeout, Duration};

    let address = "127.0.0.1:7497";

    register_timezone_alias(
        "Hawaii-Aleutian Standard Time",
        "Pacific/Honolulu",
    );

    println!("\n----------------------------------------");
    println!("[IBKR TEST] AAPL Paper Market Buy");
    println!("Address: {address}");
    println!("Quantity: 4 share");
    println!("----------------------------------------");

    let client = Client::connect(address, 104)
        .await
        .expect("Failed to connect to IBKR TWS");

    println!("[IBKR TEST] CONNECTED");

    let contract = Contract::stock("AAPL").build();

    // ------------------------------------------------------------
    // ORDER UPDATE STREAM
    //
    // Do NOT use filter_data().
    //
    // We want the complete SubscriptionItem because IBKR can deliver
    // order warnings/rejections as SubscriptionItem::Notice.
    // ------------------------------------------------------------
    let mut order_stream = client
        .order_update_stream()
        .await
        .expect("Failed to create IBKR order update stream");

    // ------------------------------------------------------------
    // GLOBAL NOTICE STREAM
    //
    // Receives IBKR notices not tied to one specific order,
    // such as market-data connectivity and connection status.
    // ------------------------------------------------------------
    let mut notice_stream = client
        .notice_stream()
        .expect("Failed to create IBKR global notice stream");

    println!("[IBKR TEST] ORDER UPDATE STREAM STARTED");
    println!("[IBKR TEST] GLOBAL NOTICE STREAM STARTED");

    tokio::time::sleep(Duration::from_millis(250)).await;

    println!("[IBKR TEST] Submitting MARKET BUY for 1 AAPL...");

    let order_id = client
        .order(&contract)
        .buy(4)
        .market()
        .submit()
        .await
        .expect("Failed to submit AAPL market buy");

    let order_id_raw = order_id.0;

    println!("[IBKR TEST] ORDER SENT");
    println!("Order ID: {order_id}");

    let wait_result = timeout(Duration::from_secs(15), async {
        loop {
            tokio::select! {

                // ============================================================
                // ORDER EVENTS + ORDER-SPECIFIC ERRORS
                // ============================================================
                item = order_stream.next() => {
                    match item {
                        Some(Ok(SubscriptionItem::Data(
                            OrderUpdate::OpenOrder(data)
                        ))) => {
                            println!("\n[IBKR OPEN ORDER]");
                            println!("Order ID: {}", data.order_id);
                            println!("Symbol:   {}", data.contract.symbol);
                            println!("State:    {}", data.order_state.status);

                            // Print the complete payload for diagnostics.
                            println!("{data:#?}");

                            if data.order_id == order_id_raw {
                                println!("[IBKR TEST] THIS IS OUR ORDER");
                            }
                        }

                        Some(Ok(SubscriptionItem::Data(
                            OrderUpdate::OrderStatus(status)
                        ))) => {
                            println!("\n[IBKR ORDER STATUS]");
                            println!("Order ID:    {}", status.order_id);
                            println!("Status:      {}", status.status);
                            println!("Filled:      {}", status.filled);
                            println!("Remaining:   {}", status.remaining);
                            println!("Avg Fill Px: {:?}", status.average_fill_price);
                            println!("Why Held:    {}", status.why_held);

                            if status.order_id == order_id_raw {
                                println!("[IBKR TEST] THIS IS OUR ORDER");

                                if status.status.is_terminal() {
                                    println!(
                                        "[IBKR TEST] TERMINAL STATUS: {}",
                                        status.status
                                    );

                                    break;
                                }
                            }
                        }

                        Some(Ok(SubscriptionItem::Data(
                            OrderUpdate::ExecutionData(data)
                        ))) => {
                            println!("\n[IBKR EXECUTION]");
                            println!("Order ID:     {}", data.execution.order_id);
                            println!("Execution ID: {}", data.execution.execution_id);
                            println!("Shares:       {}", data.execution.shares);
                            println!("Price:        {}", data.execution.price);
                            println!("Exchange:     {}", data.execution.exchange);

                            if data.execution.order_id == order_id_raw {
                                println!("[IBKR TEST] THIS EXECUTION IS OUR ORDER");
                            }
                        }

                        Some(Ok(SubscriptionItem::Data(
                            OrderUpdate::CommissionReport(report)
                        ))) => {
                            println!("\n[IBKR COMMISSION]");
                            println!("{report:#?}");
                        }

                        // ----------------------------------------------------
                        // ORDER-SPECIFIC WARNING / ERROR / REJECTION
                        // ----------------------------------------------------
                        Some(Ok(SubscriptionItem::Notice(notice))) => {
                            println!("\n[IBKR ORDER NOTICE]");
                            println!("Request ID: {:?}", notice.request_id);
                            println!("Code:       {}", notice.code);
                            println!("Message:    {}", notice.message);

                            if notice.is_error() {
                                println!("[IBKR ORDER ERROR / REJECTION]");

                                // If this error belongs to our order,
                                // stop immediately.
                                if notice.request_id == Some(order_id_raw) {
                                    println!("[IBKR TEST] OUR ORDER WAS REJECTED");
                                    break;
                                }
                            } else if notice.is_warning() {
                                println!("[IBKR ORDER WARNING]");
                            } else if notice.is_system_message() {
                                println!("[IBKR ORDER SYSTEM MESSAGE]");
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

                // ============================================================
                // GLOBAL IBKR NOTICES
                // ============================================================
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
        println!("The order may still be active or waiting for the market to open.");
    }

    client.disconnect().await;

    println!("\n[IBKR TEST] DISCONNECTED");
    println!("----------------------------------------");
}


// Tests that the Rust bot can submit a normal 1-share AAPL MARKET SELL to the
// IBKR PAPER account while listening for BOTH complete order lifecycle updates
// and IBKR notices/errors.
//
// This is the close-side equivalent of test_ibkr_aapl_market_buy().
//
// IMPORTANT BEHAVIOR:
// `submit()` returning an OrderId only means the order was sent to TWS.
// It does NOT prove IBKR accepted, routed, or filled the order.
//
// The order update stream is started BEFORE submission so the bot can receive:
// - OpenOrder
// - OrderStatus
// - ExecutionData
// - CommissionReport
// - order-specific warnings
// - order-specific rejection/error messages
//
// A separate global notice stream is also started so the bot can receive
// connectivity/system-level IBKR messages that are not tied to one order.
//
// This test intentionally prints ALL order and notice messages so we can see
// exactly what IBKR sends. Only messages belonging to this exact order are
// used to determine whether this close order completed or was rejected.
//
// NOTE:
// This test assumes the PAPER account currently owns at least 1 available
// share of AAPL. If it does not, IBKR should reject the SELL and the rejection
// will be printed through the order notice stream.
//
// IMPORTANT: This sends an actual SELL order to the IBKR PAPER account.
//
// Run:
// cargo test test_ibkr_aapl_market_sell -- --nocapture
#[tokio::test]
async fn test_ibkr_aapl_market_sell() {
    use ibapi::prelude::*;
    use tokio::time::{timeout, Duration};

    let address = "127.0.0.1:7497";

    register_timezone_alias(
        "Hawaii-Aleutian Standard Time",
        "Pacific/Honolulu",
    );

    println!("\n----------------------------------------");
    println!("[IBKR TEST] AAPL Paper Market Sell");
    println!("Address: {address}");
    println!("Quantity: 1 share");
    println!("----------------------------------------");

    let client = Client::connect(address, 106)
        .await
        .expect("Failed to connect to IBKR TWS");

    println!("[IBKR TEST] CONNECTED");

    let contract = Contract::stock("AAPL").build();

    // ------------------------------------------------------------
    // ORDER UPDATE STREAM
    //
    // Do NOT use filter_data().
    //
    // We need the complete SubscriptionItem because IBKR can deliver
    // order warnings/rejections as SubscriptionItem::Notice.
    // ------------------------------------------------------------
    let mut order_stream = client
        .order_update_stream()
        .await
        .expect("Failed to create IBKR order update stream");

    // ------------------------------------------------------------
    // GLOBAL NOTICE STREAM
    //
    // Receives IBKR notices not tied to one specific order,
    // such as connectivity and market-data status messages.
    // ------------------------------------------------------------
    let mut notice_stream = client
        .notice_stream()
        .expect("Failed to create IBKR global notice stream");

    println!("[IBKR TEST] ORDER UPDATE STREAM STARTED");
    println!("[IBKR TEST] GLOBAL NOTICE STREAM STARTED");

    // Make sure both listeners are active before submitting.
    tokio::time::sleep(Duration::from_millis(250)).await;

    println!("[IBKR TEST] Submitting MARKET SELL for 1 AAPL...");

    let order_id = client
        .order(&contract)
        .sell(1)
        .market()
        .submit()
        .await
        .expect("Failed to submit AAPL market sell");

    let order_id_raw = order_id.0;

    println!("[IBKR TEST] ORDER SENT");
    println!("Order ID: {order_id}");

    let wait_result = timeout(Duration::from_secs(15), async {
        loop {
            tokio::select! {

                // ============================================================
                // ORDER EVENTS + ORDER-SPECIFIC ERRORS
                // ============================================================
                item = order_stream.next() => {
                    match item {
                        Some(Ok(SubscriptionItem::Data(
                            OrderUpdate::OpenOrder(data)
                        ))) => {
                            println!("\n[IBKR OPEN ORDER]");
                            println!("Order ID: {}", data.order_id);
                            println!("Symbol:   {}", data.contract.symbol);
                            println!("State:    {}", data.order_state.status);

                            println!("{data:#?}");

                            if data.order_id == order_id_raw {
                                println!("[IBKR TEST] THIS IS OUR CLOSE ORDER");
                            }
                        }

                        Some(Ok(SubscriptionItem::Data(
                            OrderUpdate::OrderStatus(status)
                        ))) => {
                            println!("\n[IBKR ORDER STATUS]");
                            println!("Order ID:    {}", status.order_id);
                            println!("Status:      {}", status.status);
                            println!("Filled:      {}", status.filled);
                            println!("Remaining:   {}", status.remaining);
                            println!("Avg Fill Px: {:?}", status.average_fill_price);
                            println!("Why Held:    {}", status.why_held);

                            if status.order_id == order_id_raw {
                                println!("[IBKR TEST] THIS IS OUR CLOSE ORDER");

                                if status.status.is_terminal() {
                                    println!(
                                        "[IBKR TEST] TERMINAL STATUS: {}",
                                        status.status
                                    );

                                    break;
                                }
                            }
                        }

                        Some(Ok(SubscriptionItem::Data(
                            OrderUpdate::ExecutionData(data)
                        ))) => {
                            println!("\n[IBKR EXECUTION]");
                            println!("Order ID:     {}", data.execution.order_id);
                            println!("Execution ID: {}", data.execution.execution_id);
                            println!("Shares:       {}", data.execution.shares);
                            println!("Price:        {}", data.execution.price);
                            println!("Exchange:     {}", data.execution.exchange);

                            if data.execution.order_id == order_id_raw {
                                println!(
                                    "[IBKR TEST] THIS EXECUTION IS OUR CLOSE ORDER"
                                );
                            }
                        }

                        Some(Ok(SubscriptionItem::Data(
                            OrderUpdate::CommissionReport(report)
                        ))) => {
                            println!("\n[IBKR COMMISSION]");
                            println!("{report:#?}");
                        }

                        // ----------------------------------------------------
                        // ORDER-SPECIFIC WARNING / ERROR / REJECTION
                        // ----------------------------------------------------
                        Some(Ok(SubscriptionItem::Notice(notice))) => {
                            println!("\n[IBKR ORDER NOTICE]");
                            println!("Request ID: {:?}", notice.request_id);
                            println!("Code:       {}", notice.code);
                            println!("Message:    {}", notice.message);

                            if notice.is_error() {
                                println!("[IBKR ORDER ERROR / REJECTION]");

                                if notice.request_id == Some(order_id_raw) {
                                    println!(
                                        "[IBKR TEST] OUR CLOSE ORDER WAS REJECTED"
                                    );

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

                // ============================================================
                // GLOBAL IBKR NOTICES
                // ============================================================
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
        println!("The close order may still be active or waiting for the market to open.");
    }

    client.disconnect().await;

    println!("\n[IBKR TEST] DISCONNECTED");
    println!("----------------------------------------");
}


// ============================================================
// 
// ============================================================


// Tests that the Rust bot can submit a 1-share AAPL BUY LIMIT order to the
// IBKR PAPER account while listening for BOTH complete order lifecycle updates
// and IBKR notices/errors.
//
// This test follows the same failure-handling style that the real live adapter
// should use:
// - no panic/expect for broker runtime failures
// - print/log the error
// - stop this operation cleanly
// - do not crash the trading engine
//
// IMPORTANT:
// `submit()` returning an OrderId only means the order was sent to TWS.
// It does NOT prove IBKR accepted, routed, or filled the order.
//
// The order update stream is started BEFORE submission so the bot can receive:
// - OpenOrder
// - OrderStatus
// - ExecutionData
// - CommissionReport
// - order-specific warnings
// - order-specific rejection/error messages
//
// A separate global notice stream receives IBKR connectivity/system messages.
//
// This test intentionally uses a low $100.00 limit price so the order is
// unlikely to execute.
//
// IMPORTANT: This sends an actual order to the IBKR PAPER account.
//
// Run:
// cargo test test_ibkr_aapl_limit_buy -- --nocapture
#[tokio::test]
async fn test_ibkr_aapl_limit_buy() {
    use ibapi::prelude::*;
    use tokio::time::{timeout, Duration};

    let address = "127.0.0.1:7497";
    let quantity = 1;
    let limit_price = 100.00;

    register_timezone_alias(
        "Hawaii-Aleutian Standard Time",
        "Pacific/Honolulu",
    );

    println!("\n----------------------------------------");
    println!("[IBKR TEST] AAPL Paper Limit Buy");
    println!("Address:     {address}");
    println!("Quantity:    {quantity} share");
    println!("Limit Price: ${limit_price:.2}");
    println!("----------------------------------------");

    // ------------------------------------------------------------
    // CONNECT
    // ------------------------------------------------------------
    let client = match Client::connect(address, 107).await {
        Ok(client) => client,

        Err(error) => {
            println!("\n[IBKR CONNECTION ERROR]");
            println!("{error:?}");
            println!("----------------------------------------");
            return;
        }
    };

    println!("[IBKR TEST] CONNECTED");

    let contract = Contract::stock("AAPL").build();

    // ------------------------------------------------------------
    // ORDER UPDATE STREAM
    //
    // Do NOT use filter_data().
    // We need SubscriptionItem::Notice as well as OrderUpdate data.
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

    println!(
        "[IBKR TEST] Submitting LIMIT BUY for {quantity} AAPL @ ${limit_price:.2}..."
    );

    // ------------------------------------------------------------
    // SUBMIT ORDER
    //
    // A submit error here is a client/API-level failure.
    // Even after successful submission, broker-side rejection can still
    // arrive later through the order stream as SubscriptionItem::Notice.
    // ------------------------------------------------------------
    let order_id = match client
        .order(&contract)
        .buy(quantity)
        .limit(limit_price)
        .submit()
        .await
    {
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

    println!("[IBKR TEST] ORDER SENT");
    println!("Order ID: {order_id}");

    let wait_result = timeout(Duration::from_secs(15), async {
        loop {
            tokio::select! {

                // ============================================================
                // ORDER EVENTS + ORDER-SPECIFIC ERRORS / WARNINGS
                // ============================================================
                item = order_stream.next() => {
                    match item {

                        Some(Ok(SubscriptionItem::Data(
                            OrderUpdate::OpenOrder(data)
                        ))) => {
                            println!("\n[IBKR OPEN ORDER]");
                            println!("Order ID: {}", data.order_id);
                            println!("Symbol:   {}", data.contract.symbol);
                            println!("State:    {}", data.order_state.status);

                            println!("{data:#?}");

                            if data.order_id == order_id_raw {
                                println!("[IBKR TEST] THIS IS OUR LIMIT ORDER");
                            }
                        }

                        Some(Ok(SubscriptionItem::Data(
                            OrderUpdate::OrderStatus(status)
                        ))) => {
                            println!("\n[IBKR ORDER STATUS]");
                            println!("Order ID:    {}", status.order_id);
                            println!("Status:      {}", status.status);
                            println!("Filled:      {}", status.filled);
                            println!("Remaining:   {}", status.remaining);
                            println!("Avg Fill Px: {:?}", status.average_fill_price);
                            println!("Why Held:    {}", status.why_held);

                            if status.order_id == order_id_raw {
                                println!("[IBKR TEST] THIS IS OUR LIMIT ORDER");

                                if status.status.is_terminal() {
                                    println!(
                                        "[IBKR TEST] TERMINAL STATUS: {}",
                                        status.status
                                    );

                                    break;
                                }
                            }
                        }

                        Some(Ok(SubscriptionItem::Data(
                            OrderUpdate::ExecutionData(data)
                        ))) => {
                            println!("\n[IBKR EXECUTION]");
                            println!("Order ID:     {}", data.execution.order_id);
                            println!("Execution ID: {}", data.execution.execution_id);
                            println!("Shares:       {}", data.execution.shares);
                            println!("Price:        {}", data.execution.price);
                            println!("Exchange:     {}", data.execution.exchange);

                            if data.execution.order_id == order_id_raw {
                                println!(
                                    "[IBKR TEST] THIS EXECUTION IS OUR LIMIT ORDER"
                                );
                            }
                        }

                        Some(Ok(SubscriptionItem::Data(
                            OrderUpdate::CommissionReport(report)
                        ))) => {
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
                                    println!(
                                        "[IBKR TEST] OUR LIMIT ORDER WAS REJECTED"
                                    );

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

                            // Real adapter:
                            // convert/log this as an adapter failure and keep
                            // the engine alive.
                            break;
                        }

                        None => {
                            println!("\n[IBKR TEST] ORDER STREAM CLOSED");
                            break;
                        }
                    }
                }

                // ============================================================
                // GLOBAL IBKR NOTICES
                // ============================================================
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

                            // Do not break the order listener just because
                            // the global notice stream ended.
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


// Tests that the Rust bot can submit a 1-share AAPL SELL LIMIT order to the
// IBKR PAPER account while listening for BOTH complete order lifecycle updates
// and IBKR notices/errors.
//
// This test follows the same failure-handling style that the real live adapter
// should use:
// - no panic/expect for broker runtime failures
// - print/log errors
// - stop the failed operation cleanly
// - do not crash the trading engine
//
// IMPORTANT:
// `submit()` returning an OrderId only means the order was sent to TWS.
// It does NOT prove IBKR accepted, routed, or filled the order.
//
// The order update stream is started BEFORE submission so the bot can receive:
// - OpenOrder
// - OrderStatus
// - ExecutionData
// - CommissionReport
// - order-specific warnings
// - order-specific rejection/error messages
//
// A separate global notice stream receives IBKR connectivity/system messages.
//
// This test intentionally uses a high $500.00 limit price so the order is
// unlikely to execute.
//
// NOTE:
// If there is no existing AAPL long position, this SELL order can potentially
// open a SHORT position if it executes.
//
// IMPORTANT: This sends an actual order to the IBKR PAPER account.
//
// Run:
// cargo test test_ibkr_aapl_limit_sell -- --nocapture
#[tokio::test]
async fn test_ibkr_aapl_limit_sell() {
    use ibapi::prelude::*;
    use tokio::time::{timeout, Duration};

    let address = "127.0.0.1:7497";
    let quantity = 1;
    let limit_price = 500.00;

    register_timezone_alias(
        "Hawaii-Aleutian Standard Time",
        "Pacific/Honolulu",
    );

    println!("\n----------------------------------------");
    println!("[IBKR TEST] AAPL Paper Limit Sell");
    println!("Address:     {address}");
    println!("Quantity:    {quantity} share");
    println!("Limit Price: ${limit_price:.2}");
    println!("----------------------------------------");

    // ------------------------------------------------------------
    // CONNECT
    // ------------------------------------------------------------
    let client = match Client::connect(address, 108).await {
        Ok(client) => client,

        Err(error) => {
            println!("\n[IBKR CONNECTION ERROR]");
            println!("{error:?}");
            println!("----------------------------------------");
            return;
        }
    };

    println!("[IBKR TEST] CONNECTED");

    let contract = Contract::stock("AAPL").build();

    // ------------------------------------------------------------
    // ORDER UPDATE STREAM
    //
    // Do NOT use filter_data().
    //
    // We need the complete SubscriptionItem because IBKR can deliver
    // order warnings/rejections as SubscriptionItem::Notice.
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

    // Give both listeners time to become active before submitting.
    tokio::time::sleep(Duration::from_millis(250)).await;

    println!(
        "[IBKR TEST] Submitting LIMIT SELL for {quantity} AAPL @ ${limit_price:.2}..."
    );

    // ------------------------------------------------------------
    // SUBMIT SELL LIMIT ORDER
    //
    // A successful submit only means the order was sent to TWS.
    // Broker acceptance/rejection is determined from subsequent events.
    // ------------------------------------------------------------
    let order_id = match client
        .order(&contract)
        .sell(quantity)
        .limit(limit_price)
        .submit()
        .await
    {
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

    println!("[IBKR TEST] ORDER SENT");
    println!("Order ID: {order_id}");

    let wait_result = timeout(Duration::from_secs(15), async {
        loop {
            tokio::select! {

                // ============================================================
                // ORDER EVENTS + ORDER-SPECIFIC ERRORS / WARNINGS
                // ============================================================
                item = order_stream.next() => {
                    match item {

                        // ----------------------------------------------------
                        // OPEN ORDER
                        // ----------------------------------------------------
                        Some(Ok(SubscriptionItem::Data(
                            OrderUpdate::OpenOrder(data)
                        ))) => {
                            println!("\n[IBKR OPEN ORDER]");
                            println!("Order ID: {}", data.order_id);
                            println!("Symbol:   {}", data.contract.symbol);
                            println!("State:    {}", data.order_state.status);

                            // Full payload for development diagnostics.
                            println!("{data:#?}");

                            if data.order_id == order_id_raw {
                                println!("[IBKR TEST] THIS IS OUR LIMIT SELL ORDER");
                            }
                        }

                        // ----------------------------------------------------
                        // ORDER STATUS
                        // ----------------------------------------------------
                        Some(Ok(SubscriptionItem::Data(
                            OrderUpdate::OrderStatus(status)
                        ))) => {
                            println!("\n[IBKR ORDER STATUS]");
                            println!("Order ID:    {}", status.order_id);
                            println!("Status:      {}", status.status);
                            println!("Filled:      {}", status.filled);
                            println!("Remaining:   {}", status.remaining);
                            println!("Avg Fill Px: {:?}", status.average_fill_price);
                            println!("Why Held:    {}", status.why_held);

                            if status.order_id == order_id_raw {
                                println!("[IBKR TEST] THIS IS OUR LIMIT SELL ORDER");

                                if status.status.is_terminal() {
                                    println!(
                                        "[IBKR TEST] TERMINAL STATUS: {}",
                                        status.status
                                    );

                                    break;
                                }
                            }
                        }

                        // ----------------------------------------------------
                        // EXECUTION / FILL
                        // ----------------------------------------------------
                        Some(Ok(SubscriptionItem::Data(
                            OrderUpdate::ExecutionData(data)
                        ))) => {
                            println!("\n[IBKR EXECUTION]");
                            println!("Order ID:     {}", data.execution.order_id);
                            println!("Execution ID: {}", data.execution.execution_id);
                            println!("Shares:       {}", data.execution.shares);
                            println!("Price:        {}", data.execution.price);
                            println!("Exchange:     {}", data.execution.exchange);

                            if data.execution.order_id == order_id_raw {
                                println!(
                                    "[IBKR TEST] THIS EXECUTION IS OUR LIMIT SELL ORDER"
                                );
                            }
                        }

                        // ----------------------------------------------------
                        // COMMISSION
                        // ----------------------------------------------------
                        Some(Ok(SubscriptionItem::Data(
                            OrderUpdate::CommissionReport(report)
                        ))) => {
                            println!("\n[IBKR COMMISSION]");
                            println!("{report:#?}");
                        }

                        // ----------------------------------------------------
                        // ORDER NOTICE / WARNING / ERROR / REJECTION
                        // ----------------------------------------------------
                        Some(Ok(SubscriptionItem::Notice(notice))) => {
                            println!("\n[IBKR ORDER NOTICE]");
                            println!("Request ID: {:?}", notice.request_id);
                            println!("Code:       {}", notice.code);
                            println!("Message:    {}", notice.message);

                            if notice.is_error() {
                                println!("[IBKR ORDER ERROR / REJECTION]");

                                if notice.request_id == Some(order_id_raw) {
                                    println!(
                                        "[IBKR TEST] OUR LIMIT SELL ORDER WAS REJECTED"
                                    );

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

                        // ----------------------------------------------------
                        // ORDER STREAM FAILURE
                        // ----------------------------------------------------
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

                // ============================================================
                // GLOBAL IBKR NOTICES
                // ============================================================
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
        println!("The limit sell order may still be active/resting in IBKR.");
    }

    client.disconnect().await;

    println!("\n[IBKR TEST] DISCONNECTED");
    println!("----------------------------------------");
}


// ============================================================
// 
// ============================================================


// Tests that the Rust bot can submit a FRACTIONAL AAPL market order to the
// IBKR PAPER account while listening for BOTH order lifecycle updates and
// IBKR notices/errors.
//
// IMPORTANT BEHAVIOR:
// `submit()` returning an OrderId only means the order was sent to TWS.
// It does NOT prove IBKR accepted the order.
//
// The order update stream is started BEFORE submission so the bot can receive:
// - OpenOrder
// - OrderStatus
// - ExecutionData
// - CommissionReport
// - order-specific warnings
// - order-specific rejection/error messages
//
// A separate global notice stream is also started so the bot can receive
// IBKR connectivity/system messages that are not tied to one specific order.
//
// This closely matches how the live IBKR adapter should work:
//     order updates + order errors + global IBKR notices
//
// The test submits a MARKET BUY for 0.25 shares of AAPL.
// If IBKR rejects this exact order, the test immediately stops listening
// instead of waiting for the 15-second timeout.
//
// Current expected IBKR behavior for this test:
// fractional API orders are rejected with error code 10243.
//
// IMPORTANT: This sends an actual order request to the IBKR PAPER account.
//
// Run:
// cargo test test_ibkr_aapl_fractional_market_buy -- --nocapture
#[tokio::test]
async fn test_ibkr_aapl_fractional_market_buy() {
    use ibapi::prelude::*;
    use tokio::time::{timeout, Duration};

    let address = "127.0.0.1:7497";

    register_timezone_alias(
        "Hawaii-Aleutian Standard Time",
        "Pacific/Honolulu",
    );

    println!("\n----------------------------------------");
    println!("[IBKR TEST] AAPL Fractional Paper Market Buy");
    println!("Address: {address}");
    println!("Quantity: 0.25 shares");
    println!("----------------------------------------");

    let client = Client::connect(address, 105)
        .await
        .expect("Failed to connect to IBKR TWS");

    println!("[IBKR TEST] CONNECTED");

    let contract = Contract::stock("AAPL").build();

    // ------------------------------------------------------------
    // ORDER UPDATE STREAM
    //
    // Do NOT use filter_data().
    //
    // We need the full SubscriptionItem because IBKR can deliver
    // order warnings and rejections as SubscriptionItem::Notice.
    // ------------------------------------------------------------
    let mut order_stream = client
        .order_update_stream()
        .await
        .expect("Failed to create IBKR order update stream");

    // ------------------------------------------------------------
    // GLOBAL NOTICE STREAM
    //
    // Receives IBKR notices not tied to one specific order,
    // such as market-data connectivity and connection status.
    // ------------------------------------------------------------
    let mut notice_stream = client
        .notice_stream()
        .expect("Failed to create IBKR global notice stream");

    println!("[IBKR TEST] ORDER UPDATE STREAM STARTED");
    println!("[IBKR TEST] GLOBAL NOTICE STREAM STARTED");

    // Small delay so both listeners are active before submission.
    tokio::time::sleep(Duration::from_millis(250)).await;

    println!("[IBKR TEST] Submitting MARKET BUY for 0.25 AAPL...");

    let order_id = client
        .order(&contract)
        .buy(0.25)
        .market()
        .submit()
        .await
        .expect("Failed to send fractional AAPL market buy");

    let order_id_raw = order_id.0;

    println!("[IBKR TEST] ORDER SENT");
    println!("Order ID: {order_id}");

    let wait_result = timeout(Duration::from_secs(15), async {
        loop {
            tokio::select! {

                // ============================================================
                // ORDER EVENTS + ORDER-SPECIFIC ERRORS
                // ============================================================
                item = order_stream.next() => {
                    match item {
                        Some(Ok(SubscriptionItem::Data(
                            OrderUpdate::OpenOrder(data)
                        ))) => {
                            if data.order_id != order_id_raw {
                                continue;
                            }

                            println!("\n[IBKR OPEN ORDER]");
                            println!("Order ID: {}", data.order_id);
                            println!("Symbol:   {}", data.contract.symbol);
                            println!("State:    {}", data.order_state.status);
                        }

                        Some(Ok(SubscriptionItem::Data(
                            OrderUpdate::OrderStatus(status)
                        ))) => {
                            if status.order_id != order_id_raw {
                                continue;
                            }

                            println!("\n[IBKR ORDER STATUS]");
                            println!("Order ID:    {}", status.order_id);
                            println!("Status:      {}", status.status);
                            println!("Filled:      {}", status.filled);
                            println!("Remaining:   {}", status.remaining);
                            println!("Avg Fill Px: {:?}", status.average_fill_price);
                            println!("Why Held:    {}", status.why_held);

                            if status.status.is_terminal() {
                                println!(
                                    "\n[IBKR TEST] TERMINAL STATUS: {}",
                                    status.status
                                );

                                break;
                            }
                        }

                        Some(Ok(SubscriptionItem::Data(
                            OrderUpdate::ExecutionData(data)
                        ))) => {
                            if data.execution.order_id != order_id_raw {
                                continue;
                            }

                            println!("\n[IBKR EXECUTION]");
                            println!("Order ID:     {}", data.execution.order_id);
                            println!("Execution ID: {}", data.execution.execution_id);
                            println!("Shares:       {}", data.execution.shares);
                            println!("Price:        {}", data.execution.price);
                            println!("Exchange:     {}", data.execution.exchange);
                        }

                        Some(Ok(SubscriptionItem::Data(
                            OrderUpdate::CommissionReport(report)
                        ))) => {
                            println!("\n[IBKR COMMISSION]");
                            println!("{report:#?}");
                        }

                        // ----------------------------------------------------
                        // ORDER-SPECIFIC WARNING / ERROR / REJECTION
                        // ----------------------------------------------------
                        Some(Ok(SubscriptionItem::Notice(notice))) => {
                            println!("\n[IBKR ORDER NOTICE]");
                            println!("Request ID: {:?}", notice.request_id);
                            println!("Code:       {}", notice.code);
                            println!("Message:    {}", notice.message);

                            if notice.is_error() {
                                println!("[IBKR ORDER ERROR / REJECTION]");

                                // If the rejection belongs to THIS order,
                                // stop immediately. No reason to wait 15 seconds.
                                if notice.request_id == Some(order_id_raw) {
                                    println!("[IBKR TEST] OUR ORDER WAS REJECTED");
                                    break;
                                }
                            } else if notice.is_warning() {
                                println!("[IBKR ORDER WARNING]");
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

                // ============================================================
                // GLOBAL IBKR NOTICES
                // ============================================================
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
    }

    client.disconnect().await;

    println!("\n[IBKR TEST] DISCONNECTED");
    println!("----------------------------------------");
}


// ============================================================
// 
// ============================================================


// Run:
// cargo test test_ibkr_positions -- --nocapture
//
// Tests that Rust can request the current positions held in the IBKR PAPER account.
//
// The positions subscription returns the initial portfolio snapshot, including
// every currently held position, followed by PositionEnd when the initial
// snapshot is complete.
//
// The subscription also listens for IBKR notices/errors related to the positions
// request and prints/classifies them so failures are not silently ignored.
//
// An empty snapshot followed by PositionEnd is valid and means the account
// currently has no open positions.
#[tokio::test]
async fn test_ibkr_positions() {
    let address = "127.0.0.1:7497";

    register_timezone_alias(
        "Hawaii-Aleutian Standard Time",
        "Pacific/Honolulu",
    );

    println!("\n----------------------------------------");
    println!("[IBKR TEST] Positions");
    println!("Address: {address}");
    println!("----------------------------------------");

    let client = Client::connect(address, 102)
        .await
        .expect("Failed to connect to IBKR TWS");

    println!("[IBKR TEST] CONNECTED");

    let mut subscription = client
        .positions()
        .await
        .expect("Failed to request IBKR positions");

    while let Some(item) = subscription.next().await {
        match item {
            // ------------------------------------------------------------
            // POSITION
            // ------------------------------------------------------------
            Ok(SubscriptionItem::Data(
                PositionUpdate::Position(position)
            )) => {
                println!("\n[IBKR POSITION]");
                println!("{position:#?}");
            }

            // ------------------------------------------------------------
            // INITIAL SNAPSHOT COMPLETE
            // ------------------------------------------------------------
            Ok(SubscriptionItem::Data(
                PositionUpdate::PositionEnd
            )) => {
                println!(
                    "\n[IBKR TEST] INITIAL POSITION SNAPSHOT COMPLETE"
                );

                break;
            }

            // ------------------------------------------------------------
            // POSITION REQUEST NOTICE / WARNING / ERROR
            // ------------------------------------------------------------
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

            // ------------------------------------------------------------
            // STREAM FAILURE
            // ------------------------------------------------------------
            Err(error) => {
                panic!("IBKR positions stream error: {error:?}");
            }
        }
    }

    client.disconnect().await;

    println!("\n[IBKR TEST] DISCONNECTED");
    println!("----------------------------------------");
}

// Example output when there are no positions:
//
// ----------------------------------------
// [IBKR TEST] Positions
// Address: 127.0.0.1:7497
// ----------------------------------------
// [IBKR TEST] CONNECTED
//
// [IBKR TEST] INITIAL POSITION SNAPSHOT COMPLETE
//
// [IBKR TEST] DISCONNECTED
// ----------------------------------------


// ============================================================
// 
// ============================================================


// Tests IBKR contract resolution for AAPL before we attempt to place any orders.
//
// Creates a basic AAPL stock contract using IBKR SMART routing and USD,
// sends it to TWS, and requests the full matching contract details.
//
// This verifies that the Rust bot can translate a normal ticker like "AAPL"
// into IBKR's actual tradable instrument, including its unique contract ID
// (con_id), exchange information, trading class, and other contract metadata.
//
// Run:
// cargo test test_ibkr_aapl_contract -- --nocapture
#[tokio::test]
async fn test_ibkr_aapl_contract() {
    use ibapi::{
        contracts::Contract,
        register_timezone_alias,
        Client,
    };

    let address = "127.0.0.1:7497";

    register_timezone_alias(
        "Hawaii-Aleutian Standard Time",
        "Pacific/Honolulu",
    );

    println!("\n----------------------------------------");
    println!("[IBKR TEST] AAPL Contract Resolution");
    println!("Address: {address}");
    println!("----------------------------------------");

    let client = Client::connect(address, 103)
        .await
        .expect("Failed to connect to IBKR TWS");

    println!("[IBKR TEST] CONNECTED");

    // Creates:
    // Symbol:   AAPL
    // Type:     Stock
    // Exchange: SMART
    // Currency: USD
    let contract = Contract::stock("AAPL").build();

    println!("[IBKR TEST] Requesting AAPL contract details...");

    let details = client
        .contract_details(&contract)
        .await
        .expect("Failed to request AAPL contract details");

    assert!(
        !details.is_empty(),
        "IBKR returned no contract details for AAPL"
    );

    println!("[IBKR TEST] Contracts returned: {}", details.len());

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

// Output:
// ----------------------------------------
// [IBKR TEST] AAPL Contract Resolution
// Address: 127.0.0.1:7497
// ----------------------------------------
// [IBKR TEST] CONNECTED
// [IBKR TEST] Requesting AAPL contract details...
// [IBKR TEST] Contracts returned: 1

// [IBKR CONTRACT]
// Symbol:           AAPL
// Contract ID:      265598
// Security Type:    STK
// Exchange:         SMART
// Primary Exchange: NASDAQ
// Currency:         USD
// Local Symbol:     AAPL
// Trading Class:    NMS

// [IBKR TEST] DISCONNECTED
// ----------------------------------------


// ============================================================
// 
// ============================================================


// cargo test test_ibkr_tws_connection -- --nocapture
#[tokio::test]
async fn test_ibkr_tws_connection() {
    let address = "127.0.0.1:7497";

    // TWS reports this Windows-style timezone name.
    // ibapi expects an IANA timezone alias.
    register_timezone_alias(
        "Hawaii-Aleutian Standard Time",
        "Pacific/Honolulu",
    );

    println!("\n----------------------------------------");
    println!("[IBKR TEST] Connecting to TWS Paper");
    println!("Address: {address}");
    println!("----------------------------------------");

    let client = Client::connect(address, 100)
        .await
        .expect("Failed to connect to IBKR TWS");

    println!("[IBKR TEST] CONNECTED SUCCESSFULLY");
    println!("Connected: {}", client.is_connected());

    client.disconnect().await;
}


// ============================================================
// 
// ============================================================


// Run:
// cargo test test_ibkr_account_summary -- --nocapture
#[tokio::test]
async fn test_ibkr_account_summary() {
    use futures::StreamExt;
    use ibapi::{
        accounts::{
            types::AccountGroup,
            AccountSummaryTags,
        },
        register_timezone_alias,
        Client,
    };

    let address = "127.0.0.1:7497";

    register_timezone_alias(
        "Hawaii-Aleutian Standard Time",
        "Pacific/Honolulu",
    );

    println!("\n----------------------------------------");
    println!("[IBKR TEST] Account Summary");
    println!("Address: {address}");
    println!("----------------------------------------");

    let client = Client::connect(address, 101)
        .await
        .expect("Failed to connect to IBKR TWS");

    println!("[IBKR TEST] CONNECTED");

    let group = AccountGroup("All".to_string());

    let tags = [
        AccountSummaryTags::NET_LIQUIDATION,
        AccountSummaryTags::TOTAL_CASH_VALUE,
        AccountSummaryTags::BUYING_POWER,
        AccountSummaryTags::AVAILABLE_FUNDS,
    ];

    let mut subscription = client
        .account_summary(&group, &tags)
        .await
        .expect("Failed to request IBKR account summary");

    let mut received = 0;

    while let Some(summary) = subscription.next().await {
        println!("[IBKR ACCOUNT] {summary:?}");

        received += 1;

        // 4 account values + End event
        if received >= 5 {
            break;
        }
    }

    client.disconnect().await;

    println!("[IBKR TEST] DISCONNECTED");
}


// Output:
// ----------------------------------------
// [IBKR TEST] Account Summary
// Address: 127.0.0.1:7497
// ----------------------------------------
// [IBKR TEST] CONNECTED
// [IBKR ACCOUNT] Ok(Data(Summary(AccountSummary { account: "DUT097247", tag: "AvailableFunds", value: "1000000.00", currency: "USD" })))
// [IBKR ACCOUNT] Ok(Data(Summary(AccountSummary { account: "DUT097247", tag: "BuyingPower", value: "4000000.00", currency: "USD" })))
// [IBKR ACCOUNT] Ok(Data(Summary(AccountSummary { account: "DUT097247", tag: "NetLiquidation", value: "1000000.00", currency: "USD" })))
// [IBKR ACCOUNT] Ok(Data(Summary(AccountSummary { account: "DUT097247", tag: "TotalCashValue", value: "1000000.00", currency: "USD" })))
// [IBKR ACCOUNT] Ok(Data(End))