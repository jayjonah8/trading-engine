use std::sync::Arc;

use tokio::sync::{
    mpsc,
    watch,
};

use crate::core::{
    config::Config,

    engine::engine::Engine,

    engine_view::EngineView,

    execution::{
        ExecCommand,
        OrderState,
        OrderType,
        TimeInForce,
        Venue,
    },

    trade_intent::TradeIntent,

    trades::{
        Direction,
        TradeState,
    },
};


// Run test:
// cargo test test_handle_open_trade_creates_state_and_command -- --nocapture

/// Tests the first half of the canonical trade-opening lifecycle.
///
/// Verifies that a valid `TradeIntent`:
///
/// - passes intent validation
/// - creates one logical `Trade` in `Opening` state
/// - creates one entry `Order` in `PendingSubmit` state
/// - creates one entry `RiskReservation`
/// - emits one canonical `ExecCommand::SubmitOrder`
///
/// No real venue adapter is started in this test.
#[tokio::test]
async fn test_handle_open_trade_creates_state_and_command() {
    // ============================================================
    // CONFIG
    // ============================================================

    let cfg = Config {
        execution_venue: Venue::Hyperliquid,

        symbols: vec![
            "BTC".to_string(),
        ],

        max_active_trades: Some(4),
        max_active_assets: Some(4),
        max_active_trades_per_symbol: Some(2),
        max_active_trades_per_strategy: Some(4),

        max_order_notional: Some(10_000.0),
        max_gross_notional: Some(50_000.0),
        max_symbol_notional: Some(20_000.0),

        min_trade_age_before_exit_secs: 1,

        enable_simulation_mode: false,
        enable_backtest_mode: false,

        backtest_starting_balance: 100_000.0,

        enable_discord_notifications: false,
        enable_slack_notifications: false,
    };


    // ============================================================
    // ENGINE CHANNELS
    // ============================================================

    let (tx_engine, _rx_engine) =
        mpsc::unbounded_channel();

    let (tx_exec, mut rx_exec) =
        mpsc::unbounded_channel::<ExecCommand>();


    // ============================================================
    // ENGINE VIEW
    // ============================================================

    let (tx_view, _rx_view) =
        watch::channel::<Arc<EngineView>>(
            EngineView::empty(
                cfg.clone()
            )
        );


    // ============================================================
    // ENGINE
    // ============================================================

    let mut engine =
        Engine::new(
            cfg,
            tx_view,
            tx_engine,
            tx_exec,
        );


    // ============================================================
    // TRADE INTENT
    // ============================================================

    let intent =
        TradeIntent {
            strategy_id: "TestStrategy".to_string(),
            symbol: "BTC".to_string(),
            direction: Direction::Long,

            signal_px: 100_000.0,
            target_notional: 100.0,

            order_type: OrderType::Market,
            time_in_force: TimeInForce::Day,

            limit_px: None,
            stop_px: None,
            post_only: false,

            take_profit_percent: Some(2.0),
            stop_loss_percent: Some(1.0),
            emergency_stop_percent: Some(3.0),
            stop_loss_delay_secs: Some(30),
            max_duration_secs: Some(3600),

            ts_ms: 1_789_105_360_000,
        };


    // ============================================================
    // HANDLE OPEN TRADE
    // ============================================================

    engine.handle_open_trade(
        intent
    );


    // ============================================================
    // VERIFY ENGINE STATE
    // ============================================================

    assert_eq!(
        engine.state.trades.len(),
        1,
        "expected one logical trade"
    );

    assert_eq!(
        engine.state.orders.len(),
        1,
        "expected one execution order"
    );

    assert_eq!(
        engine.state.risk.reservations.len(),
        1,
        "expected one risk reservation"
    );


    // ============================================================
    // VERIFY TRADE
    // ============================================================

    let (_, trade) =
        engine.state
            .trades
            .iter()
            .next()
            .expect("trade missing");

    assert_eq!(
        trade.strategy_id,
        "TestStrategy"
    );

    assert_eq!(
        trade.symbol,
        "BTC"
    );

    assert_eq!(
        trade.state,
        TradeState::Opening
    );

    // No quantity has actually executed yet.
    assert_eq!(
        trade.quantity,
        0.0
    );

    assert_eq!(
        trade.avg_entry_px,
        None
    );


    // ============================================================
    // VERIFY ORDER
    // ============================================================

    let (_, order) =
        engine.state
            .orders
            .iter()
            .next()
            .expect("order missing");

    assert_eq!(
        order.venue,
        Venue::Hyperliquid
    );

    assert_eq!(
        order.symbol,
        "BTC"
    );

    assert_eq!(
        order.state,
        OrderState::PendingSubmit
    );

    assert_eq!(
        order.filled_quantity,
        0.0
    );

    // $100 / $100,000 BTC = 0.001 BTC.
    assert!(
        (order.quantity - 0.001).abs() < f64::EPSILON
    );


    // ============================================================
    // VERIFY EXEC COMMAND
    // ============================================================

    let command =
        rx_exec
            .recv()
            .await
            .expect("expected ExecCommand::SubmitOrder");

    match command {
        ExecCommand::SubmitOrder {
            order,
        } => {
            assert_eq!(
                order.venue,
                Venue::Hyperliquid
            );

            assert_eq!(
                order.symbol,
                "BTC"
            );

            assert_eq!(
                order.state,
                OrderState::PendingSubmit
            );

            assert!(
                (order.quantity - 0.001).abs() < f64::EPSILON
            );
        }

        other => {
            panic!(
                "expected SubmitOrder, received {:?}",
                other
            );
        }
    }
}