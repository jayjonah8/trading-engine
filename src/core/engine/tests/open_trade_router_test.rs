use chrono::Utc;

use std::{
    sync::Arc,
    time::Duration,
};

use hyperliquid_rust_sdk::Meta;

use tokio::{
    sync::{
        mpsc,
        watch,
        RwLock,
    },
    task::JoinHandle,
    time::timeout,
};

use crate::{
    core::{
        engine::engine::Engine,
        general::General,
        metrics::Metrics,
        engine_view::EngineView,
        execution::{
            ExecCommand,
            ExecEvent,
            OrderType,
            TimeInForce,
            Venue,
        },
        trade_intent::TradeIntent,
        trades::Direction,
    },

    data::{
        historical::provider::HistoricalDataProvider,
        live::provider::LiveMarketDataProvider,
    },

    exchange::{
        hyperliquid::{
            adapter::run_hyperliquid_adapter,
            meta::fetch_meta,
        },
        router::{
            run_execution_router,
            ExecutionRoutes,
        },
    },

    shared::tests::default_test_config,
};

// Run test:
// cargo test test_open_trade_routes_to_hyperliquid_adapter -- --nocapture

/// Tests the complete pre-venue opening path.
///
/// Verifies:
///
/// TradeIntent
///     -> Engine::handle_open_trade()
///     -> ExecCommand::SubmitOrder
///     -> ExecutionRouter
///     -> Hyperliquid execution adapter
///
/// The Hyperliquid adapter is still boilerplate, so this test does not
/// submit a real order and does not expect an ExecEvent yet.
#[tokio::test]
async fn test_open_trade_routes_to_hyperliquid_adapter() {
    // ============================================================
    // CONFIG
    // ============================================================

    let mut cfg = default_test_config();

    cfg.execution_venue = Venue::Hyperliquid;
    cfg.live_market_data_provider = LiveMarketDataProvider::Hyperliquid;
    cfg.historical_data_provider = HistoricalDataProvider::Hyperliquid;

    cfg.symbols = vec!["BTC".to_string()];

    cfg.max_active_trades = Some(1);
    cfg.max_active_assets = Some(1);
    cfg.max_active_trades_per_symbol = Some(1);
    cfg.max_active_trades_per_strategy = Some(1);

    cfg.max_order_notional = Some(20.0);
    cfg.max_gross_notional = Some(20.0);
    cfg.max_symbol_notional = Some(20.0);

    cfg.custom_starting_balance = 100_000.0;

    cfg.close_trades_on_startup = false;

    cfg.enable_discord_notifications = false;
    cfg.enable_slack_notifications = false;


    // ============================================================
    // ENGINE EVENT CHANNEL
    // ============================================================

    let (tx_engine, _rx_engine) =
        mpsc::unbounded_channel();


    // ============================================================
    // ENGINE -> ROUTER
    // ============================================================

    let (tx_exec, rx_exec_router) =
        mpsc::unbounded_channel::<ExecCommand>();


    // ============================================================
    // ROUTER -> HYPERLIQUID
    // ============================================================

    let (
        tx_hyperliquid_exec,
        rx_hyperliquid_exec,
    ) =
        mpsc::unbounded_channel::<ExecCommand>();


    // ============================================================
    // ADAPTER -> ENGINE
    // ============================================================

    let (tx_exec_evt, mut rx_exec_evt) =
        mpsc::unbounded_channel::<ExecEvent>();


    // ============================================================
    // EXECUTION ROUTER
    // ============================================================

    let mut routes =
        ExecutionRoutes::new();

    routes.register(
        Venue::Hyperliquid,
        tx_hyperliquid_exec,
    );

    let router_handle =
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

    let meta_cache =
        Arc::new(
            RwLock::new(
                meta
            )
        );


    let adapter_handle: JoinHandle<()> =
        tokio::spawn(
            run_hyperliquid_adapter(
                cfg.clone(),
                meta_cache,
                rx_hyperliquid_exec,
                tx_exec_evt,
                true
            )
        );


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

    let general = General {
        realized_pnl: 0.0,
        sim_realized_pnl: 0.0,
        starting_balance: 0.0,
        starting_date: Utc::now(),
        live_closed_trades: 0,
        sim_closed_trades: 0,
    };

    let metrics = Metrics::new();

    let mut engine =
        Engine::new(
            cfg,
            general,
            metrics,
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
            trade_duration_secs: Some(3600),

            trailing_stop: None,

            ts_ms: 1_789_105_360_000,
        };


    // ============================================================
    // OPEN TRADE
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
        "expected one trade"
    );

    assert_eq!(
        engine.state.orders.len(),
        1,
        "expected one order"
    );

    assert_eq!(
        engine.state.risk.reservations.len(),
        1,
        "expected one risk reservation"
    );


    // ============================================================
    // ALLOW ROUTER + ADAPTER TO PROCESS COMMAND
    // ============================================================

    tokio::time::sleep(
        Duration::from_millis(250)
    )
    .await;


    // ============================================================
    // VERIFY NO EXEC EVENT YET
    // ============================================================

    // The temporary adapter does not submit to Hyperliquid yet, so it
    // should not emit any execution event.
    let result =
        timeout(
            Duration::from_millis(100),
            rx_exec_evt.recv(),
        )
        .await;

    assert!(
        result.is_err(),
        "temporary adapter should not emit ExecEvents yet"
    );


    // ============================================================
    // CLEANUP
    // ============================================================

    router_handle.abort();
    adapter_handle.abort();
}