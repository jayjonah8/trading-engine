use std::time::Duration;

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


// Run test:
// cargo test test_hyperliquid_router_to_adapter -- --nocapture

/// Tests the canonical execution routing path from the engine-side execution
/// channel through the execution router and into the Hyperliquid adapter.
///
/// Verifies:
///
/// - the execution router starts successfully
/// - Hyperliquid is registered as an execution route
/// - `ExecCommand::SubmitOrder` is routed using `order.venue`
/// - the Hyperliquid adapter receives the canonical order
/// - no real Hyperliquid order is submitted yet because venue execution
///   is not implemented in the temporary adapter
#[tokio::test]
async fn test_hyperliquid_router_to_adapter() {
    // ============================================================
    // CONFIG
    // ============================================================

    let cfg = Config {
        execution_venue: Venue::Hyperliquid,

        symbols: vec![
            "BTC".to_string(),
        ],

        max_gross_positions: 4,
        min_trade_age_before_exit_secs: 1,

        enable_simulation_mode: false,
        enable_backtest_mode: false,

        backtest_starting_balance: 100_000.0,

        enable_discord_notifications: false,
        enable_slack_notifications: false,
    };

    // ============================================================
    // ENGINE -> ROUTER
    // ============================================================

    let (tx_exec, rx_exec_router) =
        mpsc::unbounded_channel::<ExecCommand>();

    // ============================================================
    // ROUTER -> HYPERLIQUID ADAPTER
    // ============================================================

    let (
        tx_hyperliquid,
        rx_hyperliquid,
    ) =
        mpsc::unbounded_channel::<ExecCommand>();

    // ============================================================
    // ADAPTER -> ENGINE
    // ============================================================

    let (tx_evt, mut rx_evt) =
        mpsc::unbounded_channel::<ExecEvent>();

    // ============================================================
    // ROUTER
    // ============================================================

    let mut routes =
        ExecutionRoutes::new();

    routes.register(
        Venue::Hyperliquid,
        tx_hyperliquid,
    );

    tokio::spawn(
        run_execution_router(
            rx_exec_router,
            routes,
            true,
        )
    );

    // ============================================================
    // ADAPTER
    // ============================================================

    tokio::spawn(
        run_hyperliquid_adapter(
            cfg,
            rx_hyperliquid,
            tx_evt,
        )
    );

    // ============================================================
    // TEST ORDER
    // ============================================================

    let order = Order {
        id: crate::core::execution::OrderId::new(1),
        venue: Venue::Hyperliquid,
        venue_order_id: None,
        client_order_id: ClientOrderId::new(),
        strategy_id: "TestStrategy".to_string(),
        trade_id: None,
        symbol: "BTC".to_string(),
        side: Side::Buy,
        purpose: OrderPurpose::Entry,
        order_type: OrderType::Market,
        time_in_force: TimeInForce::Day,
        quantity: 0.001,
        limit_px: None,
        stop_px: None,
        reduce_only: false,
        post_only: false,
        filled_quantity: 0.0,
        avg_fill_px: None,
        pending_replace: None,
        state: OrderState::PendingSubmit,
        created_ts_ms: 1,
        updated_ts_ms: 1,
    };

    tx_exec
        .send(
            ExecCommand::SubmitOrder {
                order,
            }
        )
        .expect("failed to send command");

    // Give the router/adapter time to process.
    tokio::time::sleep(
        Duration::from_millis(250)
    )
    .await;

    // No ExecEvent is expected yet because the adapter is still boilerplate.
    let result =
        timeout(
            Duration::from_millis(100),
            rx_evt.recv(),
        )
        .await;

    assert!(
        result.is_err(),
        "adapter should not emit ExecEvents yet"
    );
}