#![allow(dead_code)]

use std::time::Duration;

use dotenvy::dotenv;

use tokio::{
    sync::mpsc,
    time::timeout,
};

use crate::{
    core::{
        config::Config,

        execution::{
            ClientOrderId,
            ExecCommand,
            ExecEvent,
            Order,
            OrderId,
            OrderPurpose,
            OrderState,
            OrderType,
            Side,
            TimeInForce,
            Venue,
        },
    },

    exchange::{
        hyperliquid::adapter::run_hyperliquid_adapter,

        router::{
            run_execution_router,
            ExecutionRoutes,
        },
    },
};

use super::shared::fetch_test_meta_cache;

use std::sync::Arc;

use hyperliquid_rust_sdk::Meta;

use tokio::{
    sync::RwLock,
    task::JoinHandle,
};

use crate::exchange::hyperliquid::{
    meta::fetch_meta,
};


// ============================================================
// HYPERLIQUID MARKET ORDER PRODUCTION-PIPELINE INTEGRATION TEST
// ============================================================
//
// Run:
//
// cargo test test_hyperliquid_market_order_production_pipeline -- --nocapture
//
// WARNING:
//
// This test uses Hyperliquid MAINNET and places REAL market orders.
//
// It opens a small BTC long position and then closes the SAME quantity.
//
// ============================================================
// TEST BOUNDARY
// ============================================================
//
// This test exercises the production execution pipeline, but intentionally
// does NOT test the Engine's authoritative state handlers yet.
//
// That separation is important.
//
// This test verifies:
//
// Test
//   ↓
// ExecCommand::SubmitOrder
//   ↓
// Execution Router
//   ↓
// Hyperliquid Adapter
//   ├── starts OrderUpdates WS
//   ├── starts UserEvents WS
//   ├── waits until both are ready
//   └── registers CLOID correlation
//   ↓
// market_order.rs
//   ↓
// REAL Hyperliquid MAINNET
//   ↓
// OrderAccepted
//   +
// OrderUpdates
//   +
// UserEvents::Fills
//   ↓
// ExecEvent channel
//   ↓
// TEST ASSERTIONS
//
// It therefore validates the execution infrastructure independently from:
//
// - Engine order-state mutation
// - Trade creation
// - Trade closing
// - Position accounting
// - PnL accounting
// - Risk state
//
// Those belong in a later Engine integration test.
//
// ============================================================
// EXPECTED ENTRY FLOW
// ============================================================
//
// Entry Order
//     purpose = Entry
//     side = Buy
//     quantity = BTC_QUANTITY
//     reduce_only = false
//
//         ↓
//
// Hyperliquid market_open()
//
//         ↓
//
// OrderAccepted
//
//         ↓
//
// UserEvents::Fills
//
//         ↓
//
// ExecEvent::Fill
//
// ============================================================
// EXPECTED EXIT FLOW
// ============================================================
//
// Exit Order
//     purpose = Exit
//     side = Sell
//     quantity = BTC_QUANTITY
//     reduce_only = true
//
//         ↓
//
// Hyperliquid market_close(
//     sz = Some(order.quantity)
// )
//
//         ↓
//
// OrderAccepted
//
//         ↓
//
// UserEvents::Fills
//
//         ↓
//
// ExecEvent::Fill
//
// IMPORTANT:
//
// The exit uses the explicit canonical Order quantity.
//
// It does NOT use:
//
//     sz = None
//
// because `None` means close the entire Hyperliquid position for the symbol.
// That would become unsafe if multiple logical Engine trades eventually share
// the same venue position.
//
// ============================================================


#[tokio::test]
async fn test_hyperliquid_market_order_production_pipeline() {
    // ============================================================
    // ENV
    // ============================================================

    dotenv().ok();

    std::env::var(
        "HYPERLIQUID_PRIVATE_KEY"
    )
    .expect(
        "HYPERLIQUID_PRIVATE_KEY env var missing"
    );


    // ============================================================
    // TEST SETTINGS
    // ============================================================

    const SYMBOL: &str =
        "BTC";

    const BTC_QUANTITY: f64 =
        0.00013;

    const EVENT_TIMEOUT_SECS: u64 =
        120;


    println!();
    println!("============================================================");
    println!("[TEST] HYPERLIQUID PRODUCTION EXECUTION PIPELINE");
    println!("============================================================");
    println!("[TEST] MAINNET REAL ORDER TEST");
    println!("[TEST] symbol={}", SYMBOL);
    println!("[TEST] quantity={}", BTC_QUANTITY);


    // ============================================================
    // CONFIG
    // ============================================================

    let cfg =
        Config {
            execution_venue: Venue::Hyperliquid,

            symbols: vec![
                SYMBOL.to_string(),
            ],

            max_active_trades: Some(1),
            max_active_assets: Some(1),
            max_active_trades_per_symbol: Some(1),
            max_active_trades_per_strategy: Some(1),

            max_order_notional: Some(20.0),
            max_gross_notional: Some(20.0),
            max_symbol_notional: Some(20.0),

            min_trade_age_before_exit_secs: 1,

            enable_simulation_mode: false,
            enable_backtest_mode: false,

            backtest_starting_balance: 100_000.0,

            enable_discord_notifications: false,
            enable_slack_notifications: false,
        };


    // ============================================================
    // ENGINE -> EXECUTION ROUTER
    // ============================================================

    let (
        tx_exec,
        rx_exec_router,
    ) =
        mpsc::unbounded_channel::<ExecCommand>();


    // ============================================================
    // EXECUTION ROUTER -> HYPERLIQUID ADAPTER
    // ============================================================

    let (
        tx_hyperliquid,
        rx_hyperliquid,
    ) =
        mpsc::unbounded_channel::<ExecCommand>();


    // ============================================================
    // HYPERLIQUID ADAPTER -> ENGINE/TEST
    // ============================================================

    let (
        tx_evt,
        mut rx_evt,
    ) =
        mpsc::unbounded_channel::<ExecEvent>();


    // ============================================================
    // EXECUTION ROUTER
    // ============================================================

    let mut routes =
        ExecutionRoutes::new();

    routes.register(
        Venue::Hyperliquid,
        tx_hyperliquid,
    );


    let router_task =
        tokio::spawn(
            run_execution_router(
                rx_exec_router,
                routes,
                true,
            )
        );


    // ============================================================
    // HYPERLIQUID ADAPTER
    // ============================================================

    let meta =
    fetch_meta()
        .await
        .expect(
            "failed to fetch Hyperliquid Meta for test"
        );


    let meta_cache: Arc<RwLock<Meta>> =
        Arc::new(
            RwLock::new(
                meta
            )
        );


    let adapter_task: JoinHandle<()> =
        tokio::spawn(
            run_hyperliquid_adapter(
                cfg,
                meta_cache,
                rx_hyperliquid,
                tx_evt,
            )
        );

    // ============================================================
    // ENTRY ORDER
    // ============================================================

    let entry_order_id =
        OrderId::new(1);

    let entry_client_order_id =
        ClientOrderId::new();


    let entry_order =
        Order {
            id:
                entry_order_id,

            venue:
                Venue::Hyperliquid,

            venue_order_id:
                None,

            client_order_id:
                entry_client_order_id,

            strategy_id:
                "HyperliquidIntegrationTest".to_string(),

            trade_id:
                None,

            symbol:
                SYMBOL.to_string(),

            side:
                Side::Buy,

            purpose:
                OrderPurpose::Entry,

            order_type:
                OrderType::Market,

            time_in_force:
                TimeInForce::Day,

            quantity:
                BTC_QUANTITY,

            limit_px:
                None,

            stop_px:
                None,

            reduce_only:
                false,

            post_only:
                false,

            filled_quantity:
                0.0,

            avg_fill_px:
                None,

            pending_replace:
                None,

            state:
                OrderState::PendingSubmit,

            created_ts_ms:
                chrono::Utc::now()
                    .timestamp_millis(),

            updated_ts_ms:
                chrono::Utc::now()
                    .timestamp_millis(),
        };


    println!();
    println!("============================================================");
    println!("[TEST] SUBMIT ENTRY ORDER");
    println!("============================================================");
    println!("[TEST] order_id={}", entry_order_id);
    println!(
        "[TEST] client_order_id={}",
        entry_client_order_id
    );
    println!("[TEST] symbol={}", SYMBOL);
    println!("[TEST] side=BUY");
    println!("[TEST] quantity={}", BTC_QUANTITY);


    tx_exec
        .send(
            ExecCommand::SubmitOrder {
                order:
                    entry_order,
            }
        )
        .expect(
            "Failed to send entry order to execution router"
        );


    // ============================================================
    // WAIT FOR ENTRY EVENTS
    // ============================================================

    let (
        entry_venue_order_id,
        entry_fill,
    ) =
        wait_for_market_fill(
            &mut rx_evt,
            entry_order_id,
            EVENT_TIMEOUT_SECS,
            "ENTRY",
        )
        .await;


    println!();
    println!("============================================================");
    println!("[TEST] ENTRY COMPLETE");
    println!("============================================================");
    println!(
        "[TEST] venue_order_id={}",
        entry_venue_order_id
    );
    println!(
        "[TEST] venue_execution_id={:?}",
        entry_fill.venue_execution_id
    );
    println!(
        "[TEST] fill_quantity={}",
        entry_fill.quantity
    );
    println!(
        "[TEST] fill_price={}",
        entry_fill.price
    );
    println!(
        "[TEST] fee={:?}",
        entry_fill.fee
    );


    // ============================================================
    // ENTRY ASSERTIONS
    // ============================================================

    assert_eq!(
        entry_fill.order_id,
        entry_order_id,
    );

    assert_eq!(
        entry_fill.venue,
        Venue::Hyperliquid,
    );

    assert_eq!(
        entry_fill.symbol,
        SYMBOL,
    );

    assert_eq!(
        entry_fill.side,
        Side::Buy,
    );

    assert!(
        entry_fill.venue_execution_id.is_some(),
        "Entry fill should contain Hyperliquid tid"
    );

    assert!(
        entry_fill.quantity > 0.0,
        "Entry fill quantity must be positive"
    );

    assert!(
        entry_fill.price > 0.0,
        "Entry fill price must be positive"
    );

    assert!(
        (entry_fill.quantity - BTC_QUANTITY).abs() < 1e-12,
        "Entry fill quantity does not match requested quantity"
    );


    // ============================================================
    // WAIT BEFORE EXIT
    // ============================================================

    println!();
    println!(
        "[TEST] waiting 5 seconds before submitting exit..."
    );


    tokio::time::sleep(
        Duration::from_secs(5)
    )
    .await;


    // ============================================================
    // EXIT ORDER
    // ============================================================

    let exit_order_id =
        OrderId::new(2);

    let exit_client_order_id =
        ClientOrderId::new();


    let exit_order =
        Order {
            id:
                exit_order_id,

            venue:
                Venue::Hyperliquid,

            venue_order_id:
                None,

            client_order_id:
                exit_client_order_id,

            strategy_id:
                "HyperliquidIntegrationTest".to_string(),

            trade_id:
                None,

            symbol:
                SYMBOL.to_string(),

            side:
                Side::Sell,

            purpose:
                OrderPurpose::Exit,

            order_type:
                OrderType::Market,

            time_in_force:
                TimeInForce::Day,

            // Close exactly what this test opened.
            quantity:
                entry_fill.quantity,

            limit_px:
                None,

            stop_px:
                None,

            // Canonically this is a reducing exit.
            reduce_only:
                true,

            post_only:
                false,

            filled_quantity:
                0.0,

            avg_fill_px:
                None,

            pending_replace:
                None,

            state:
                OrderState::PendingSubmit,

            created_ts_ms:
                chrono::Utc::now()
                    .timestamp_millis(),

            updated_ts_ms:
                chrono::Utc::now()
                    .timestamp_millis(),
        };


    println!();
    println!("============================================================");
    println!("[TEST] SUBMIT EXIT ORDER");
    println!("============================================================");
    println!("[TEST] order_id={}", exit_order_id);
    println!(
        "[TEST] client_order_id={}",
        exit_client_order_id
    );
    println!("[TEST] symbol={}", SYMBOL);
    println!("[TEST] side=SELL");
    println!(
        "[TEST] quantity={}",
        entry_fill.quantity
    );
    println!("[TEST] reduce_only=true");


    tx_exec
        .send(
            ExecCommand::SubmitOrder {
                order:
                    exit_order,
            }
        )
        .expect(
            "Failed to send exit order to execution router"
        );


    // ============================================================
    // WAIT FOR EXIT EVENTS
    // ============================================================

    let (
        exit_venue_order_id,
        exit_fill,
    ) =
        wait_for_market_fill(
            &mut rx_evt,
            exit_order_id,
            EVENT_TIMEOUT_SECS,
            "EXIT",
        )
        .await;


    println!();
    println!("============================================================");
    println!("[TEST] EXIT COMPLETE");
    println!("============================================================");
    println!(
        "[TEST] venue_order_id={}",
        exit_venue_order_id
    );
    println!(
        "[TEST] venue_execution_id={:?}",
        exit_fill.venue_execution_id
    );
    println!(
        "[TEST] fill_quantity={}",
        exit_fill.quantity
    );
    println!(
        "[TEST] fill_price={}",
        exit_fill.price
    );
    println!(
        "[TEST] fee={:?}",
        exit_fill.fee
    );


    // ============================================================
    // EXIT ASSERTIONS
    // ============================================================

    assert_eq!(
        exit_fill.order_id,
        exit_order_id,
    );

    assert_eq!(
        exit_fill.venue,
        Venue::Hyperliquid,
    );

    assert_eq!(
        exit_fill.symbol,
        SYMBOL,
    );

    assert_eq!(
        exit_fill.side,
        Side::Sell,
    );

    assert!(
        exit_fill.venue_execution_id.is_some(),
        "Exit fill should contain Hyperliquid tid"
    );

    assert!(
        exit_fill.quantity > 0.0,
        "Exit fill quantity must be positive"
    );

    assert!(
        exit_fill.price > 0.0,
        "Exit fill price must be positive"
    );

    assert!(
        (exit_fill.quantity - entry_fill.quantity).abs() < 1e-12,
        "Exit quantity does not match entry fill quantity"
    );


    // ============================================================
    // EXECUTION IDENTITY ASSERTIONS
    // ============================================================

    assert_ne!(
        entry_order_id,
        exit_order_id,
        "Entry and exit must be separate canonical Orders"
    );

    assert_ne!(
        entry_client_order_id,
        exit_client_order_id,
        "Entry and exit must have separate ClientOrderIds"
    );

    assert_ne!(
        entry_venue_order_id,
        exit_venue_order_id,
        "Entry and exit should have separate Hyperliquid order IDs"
    );

    assert_ne!(
        entry_fill.venue_execution_id,
        exit_fill.venue_execution_id,
        "Entry and exit should have separate Hyperliquid execution IDs"
    );


    // ============================================================
    // SUCCESS
    // ============================================================

    println!();
    println!("============================================================");
    println!("[TEST] PRODUCTION EXECUTION PIPELINE PASSED");
    println!("============================================================");
    println!("[TEST] router             -> OK");
    println!("[TEST] adapter            -> OK");
    println!("[TEST] execution WS       -> OK");
    println!("[TEST] CLOID correlation  -> OK");
    println!("[TEST] market entry       -> OK");
    println!("[TEST] entry fill mapping -> OK");
    println!("[TEST] market exit        -> OK");
    println!("[TEST] exit fill mapping  -> OK");
    println!("============================================================");


    // ============================================================
    // SHUTDOWN
    // ============================================================

    // Closing the engine-side sender allows the router to shut down.
    drop(
        tx_exec
    );

    router_task.abort();
    adapter_task.abort();
}


// ============================================================
// WAIT FOR CANONICAL MARKET FILL
// ============================================================

/// Waits for the canonical OrderAccepted + real UserEvents-derived Fill
/// belonging to one specific OrderId.
///
/// Other ExecEvents may arrive before or between those events. That is normal.
///
/// In particular:
///
///     OrderUpdates "open"
///         -> ExecEvent::OrderWorking
///
/// may race:
///
///     immediate SDK response
///         -> ExecEvent::OrderAccepted
///
/// and:
///
///     UserEvents::Fills
///         -> ExecEvent::Fill
///
/// The test therefore does not depend on a strict event ordering.
async fn wait_for_market_fill(
    rx_evt: &mut mpsc::UnboundedReceiver<ExecEvent>,
    expected_order_id: OrderId,
    timeout_secs: u64,
    label: &str,
) -> (
    String,
    crate::core::execution::Fill,
) {
    let future =
        async {
            let mut venue_order_id:
                Option<String> =
                None;

            let mut canonical_fill:
                Option<crate::core::execution::Fill> =
                None;


            loop {
                let event =
                    rx_evt
                        .recv()
                        .await
                        .expect(
                            "Hyperliquid ExecEvent channel closed unexpectedly"
                        );


                println!(
                    "[TEST] {} EVENT | {:?}",
                    label,
                    event,
                );


                match event {
                    ExecEvent::OrderAccepted {
                        order_id,
                        venue_order_id:
                            received_venue_order_id,
                        ..
                    } if order_id == expected_order_id => {
                        venue_order_id =
                            Some(
                                received_venue_order_id
                            );
                    }


                    ExecEvent::OrderWorking {
                        order_id,
                        ..
                    } if order_id == expected_order_id => {
                        println!(
                            "[TEST] {} order working | order_id={}",
                            label,
                            order_id,
                        );
                    }


                    ExecEvent::Fill {
                        fill,
                    } if fill.order_id == expected_order_id => {
                        // The production WebSocket mapping should give us the
                        // real Hyperliquid `tid`.
                        //
                        // This intentionally distinguishes the real WS Fill
                        // from the old temporary immediate-response synthetic
                        // Fill implementation.
                        if fill.venue_execution_id.is_some() {
                            canonical_fill =
                                Some(
                                    fill
                                );
                        }
                    }


                    ExecEvent::OrderRejected {
                        order_id,
                        reason,
                        ..
                    } if order_id == expected_order_id => {
                        panic!(
                            "{} Hyperliquid order rejected | order_id={} | reason={}",
                            label,
                            order_id,
                            reason,
                        );
                    }


                    ExecEvent::Error {
                        order_id:
                            Some(order_id),
                        message,
                        ..
                    } if order_id == expected_order_id => {
                        panic!(
                            "{} Hyperliquid execution error | order_id={} | message={}",
                            label,
                            order_id,
                            message,
                        );
                    }


                    _ => {
                        // Ignore unrelated or non-terminal lifecycle events.
                    }
                }


                if venue_order_id.is_some()
                    && canonical_fill.is_some()
                {
                    return (
                        venue_order_id.unwrap(),
                        canonical_fill.unwrap(),
                    );
                }
            }
        };


    timeout(
        Duration::from_secs(
            timeout_secs
        ),
        future,
    )
    .await
    .unwrap_or_else(
        |_| {
            panic!(
                "Timed out after {} seconds waiting for {} OrderAccepted + real WS Fill | order_id={}",
                timeout_secs,
                label,
                expected_order_id,
            )
        }
    )
}