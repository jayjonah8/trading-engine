mod core;
mod exchange;
mod strategies;
mod indicators;
mod market;
mod shared;
mod notifications;
mod data;

use std::sync::Arc;

use anyhow::Result;

use hyperliquid_rust_sdk::Meta;

use tokio::sync::{
    broadcast,
    mpsc,
    watch,
    RwLock,
};

use crate::core::{
    config::Config,

    engine::engine::{
        Engine,
        EngineEvent,
    },

    engine_view::EngineView,

    events::AppEventEnvelope,

    execution::{
        ExecCommand,
        ExecEvent,
        Venue,
    },

    venue_state::VenueStateEvent,
};

use crate::data::{
    historical::strategy_event::BacktestStrategyEvent,
    live::event_mapper::map_app_to_live_engine_event,
};

use crate::exchange::{
    hyperliquid::{
        adapter::run_hyperliquid_adapter,

        meta::{
            fetch_meta,
            map_meta_to_instruments,
        },

        pollers::run_hyperliquid_meta_poller,
    },

    router::{
        run_execution_router,
        ExecutionRoutes,
    },
};

use crate::strategies::{
    make_backtest_strategy,
    make_strategy,
    Strategy,
    StrategyKind,
};


// ============================================================
// MAIN
// ============================================================

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    println!(
        "Starting trading engine..."
    );


    // ============================================================
    // CONFIG
    // ============================================================

    let cfg = Config {
        execution_venue: Venue::Hyperliquid,

        symbols: vec![
            "BTC".to_string(),
            "ETH".to_string(),
            "SOL".to_string(),
            "HYPE".to_string(),
            "ZEC".to_string(),
        ],

        // Example equity configuration:
        //
        // execution_venue: Venue::Alpaca,
        //
        // symbols: vec![
        //     "AAPL".to_string(),
        //     "MSFT".to_string(),
        //     "NVDA".to_string(),
        // ],

        // ============================================================
        // RISK LIMITS
        // ============================================================

        // Maximum active directional Trades across the entire Engine.
        max_active_trades: Some(0),

        // Maximum number of distinct assets with active Trades.
        max_active_assets: Some(2),

        // Maximum active Trades allowed for a single symbol.
        max_active_trades_per_symbol: Some(1),

        // Maximum active Trades allowed for a single strategy.
        max_active_trades_per_strategy: Some(2),

        // Maximum notional value of one Order.
        max_order_notional: Some(60.0),

        // Maximum gross notional exposure across the entire Engine.
        max_gross_notional: Some(60.0),

        // Maximum gross notional exposure for one symbol.
        max_symbol_notional: Some(60.0),

        // ============================================================
        // ENGINE SETTINGS
        // ============================================================

        min_trade_age_before_exit_secs: 1,

        enable_simulation_mode: false,

        enable_backtest_mode: false,

        backtest_starting_balance: 100_000.0,

        enable_discord_notifications: true,

        enable_slack_notifications: true,
    };


    // ============================================================
    // MARKET DATA -> MASTER CHANNEL
    // ============================================================

    // All live market-data providers normalize their venue-specific data
    // into canonical AppEventEnvelopes before sending them here.
    let (
        tx_master,
        mut rx_master,
    ) =
        mpsc::unbounded_channel::<AppEventEnvelope>();


    // ============================================================
    // MASTER -> BROADCAST BUS
    // ============================================================

    let (
        tx_bus,
        _drop,
    ) =
        broadcast::channel::<AppEventEnvelope>(
            4096
        );


    {
        let tx_bus =
            tx_bus.clone();


        tokio::spawn(
            async move {
                while let Some(ev) =
                    rx_master.recv().await
                {
                    let _ =
                        tx_bus.send(
                            ev
                        );
                }
            }
        );
    }


    // ============================================================
    // ENGINE EVENT CHANNEL
    // ============================================================

    // Strategies and market-data mapping send canonical EngineEvents here.
    let (
        tx_engine,
        mut rx_engine,
    ) =
        mpsc::unbounded_channel::<EngineEvent>();


    // ============================================================
    // ENGINE -> EXECUTION ROUTER
    // ============================================================

    // The Engine sends canonical ExecCommands through this channel.
    //
    // The execution router owns the receiver and forwards each command to
    // the adapter responsible for the command's Venue.
    let (
        tx_exec,
        rx_exec_router,
    ) =
        mpsc::unbounded_channel::<ExecCommand>();


    // ============================================================
    // EXECUTION ADAPTERS -> ENGINE
    // ============================================================

    // All execution adapters report canonical execution facts through this
    // channel.
    let (
        tx_exec_evt,
        mut rx_exec_evt,
    ) =
        mpsc::unbounded_channel::<ExecEvent>();


    // ============================================================
    // VENUE STATE -> ENGINE
    // ============================================================

    // Venue-specific pollers/runtime services publish normalized account and
    // instrument snapshots through this separate canonical state channel.
    //
    // Pollers are live/simulation infrastructure and must never run during
    // historical backtesting.
    let (
        tx_venue_state,
        mut rx_venue_state,
    ) =
        mpsc::unbounded_channel::<VenueStateEvent>();


    // ============================================================
    // INITIAL HYPERLIQUID META
    // ============================================================

    // Hyperliquid Meta has two consumers:
    //
    // 1. Canonical Engine instrument state.
    // 2. Hyperliquid-specific runtime/execution code.
    //
    // Fetch once here during startup so required metadata exists before any
    // strategy is allowed to trade.
    //
    // Historical backtesting must remain isolated from live venue APIs.
    let mut initial_hyperliquid_instruments =
        None;

    let hyperliquid_meta_cache:
        Option<Arc<RwLock<Meta>>> =
        if !cfg.enable_backtest_mode
            && cfg.execution_venue == Venue::Hyperliquid
        {
            // ========================================================
            // FETCH REQUIRED STARTUP META
            // ========================================================

            let meta =
                fetch_meta()
                    .await
                    .expect(
                        "Failed to fetch initial Hyperliquid Meta"
                    );


            // ========================================================
            // MAP TO CANONICAL INSTRUMENT STATE
            // ========================================================

            let instruments =
                map_meta_to_instruments(
                    &meta
                );


            let instrument_count =
                instruments.len();


            println!(
                "[HYPERLIQUID META] initial snapshot loaded | instruments={}",
                instrument_count,
            );


            // Keep the canonical snapshot until Engine has been constructed.
            //
            // It will be applied directly to Engine before the Engine task or
            // any strategies are started.
            initial_hyperliquid_instruments =
                Some(
                    instruments
                );


            // Keep the raw SDK Meta separately for Hyperliquid-specific runtime
            // consumers.
            //
            // The refresh poller will update this same cache later.
            Some(
                Arc::new(
                    RwLock::new(
                        meta
                    )
                )
            )
        }
        else {
            None
        };


    // ============================================================
    // ENGINE VIEW
    // ============================================================

    let (
        tx_view,
        rx_view,
    ) =
        watch::channel::<Arc<EngineView>>(
            EngineView::empty(
                cfg.clone()
            )
        );


    // ============================================================
    // ENGINE
    // ============================================================

    // `tx_exec` points to the execution router rather than directly to a
    // venue adapter.
    let mut engine =
        Engine::new(
            cfg.clone(),
            tx_view.clone(),
            tx_engine.clone(),
            tx_exec.clone(),
        );


    // ============================================================
    // APPLY INITIAL VENUE STATE
    // ============================================================

    // Apply required startup metadata directly before the Engine is moved into
    // its runtime task.
    //
    // This guarantees strategies can never race the initial metadata snapshot.
    if let Some(instruments) =
        initial_hyperliquid_instruments
    {
        engine.on_venue_state_event(
            VenueStateEvent::InstrumentsUpdated {
                venue: Venue::Hyperliquid,
                instruments,
            }
        );
    }


    // ============================================================
    // ENGINE MAIN LOOP
    // ============================================================

    // The Engine is the single authoritative owner of trading state.
    //
    // It processes three independent canonical input streams:
    //
    // EngineEvent
    //     -> strategy / market decisions
    //
    // ExecEvent
    //     -> execution lifecycle facts
    //
    // VenueStateEvent
    //     -> external account / instrument snapshots
    tokio::spawn(
        async move {
            loop {
                tokio::select! {

                    // ================================================
                    // ENGINE EVENT
                    // ================================================

                    Some(ev) =
                        rx_engine.recv() =>
                    {
                        engine.on_event(
                            ev
                        );
                    }


                    // ================================================
                    // EXECUTION EVENT
                    // ================================================

                    Some(exec_evt) =
                        rx_exec_evt.recv() =>
                    {
                        engine.on_exec_event(
                            exec_evt
                        );
                    }


                    // ================================================
                    // VENUE STATE EVENT
                    // ================================================

                    Some(venue_state_evt) =
                        rx_venue_state.recv() =>
                    {
                        engine.on_venue_state_event(
                            venue_state_evt
                        );
                    }


                    // ================================================
                    // CHANNELS CLOSED
                    // ================================================

                    else => {
                        break;
                    }
                }
            }


            println!(
                "[ENGINE] main loop exited"
            );
        }
    );


    // ============================================================
    // START HYPERLIQUID META POLLER
    // ============================================================

    // Startup already fetched the first required Meta snapshot.
    //
    // The poller only performs later refreshes and updates:
    //
    // - the shared raw Hyperliquid Meta cache
    // - canonical Engine VenueState
    //
    // Pollers run in live and simulation modes, never historical backtests.
    if !cfg.enable_backtest_mode {
        if let Some(meta_cache) =
            hyperliquid_meta_cache
                .as_ref()
        {
            let meta_cache_for_poller =
                Arc::clone(
                    meta_cache
                );

            let tx_venue_state_for_meta =
                tx_venue_state.clone();


            tokio::spawn(
                async move {
                    run_hyperliquid_meta_poller(
                        meta_cache_for_poller,
                        tx_venue_state_for_meta,
                    )
                    .await;
                }
            );
        }
    }


    // ============================================================
    // EXECUTION ROUTES
    // ============================================================

    // Each registered venue receives its own execution command channel.
    let mut execution_routes =
        ExecutionRoutes::new();


    // ============================================================
    // HYPERLIQUID EXECUTION CHANNEL
    // ============================================================

    let (
        tx_hyperliquid_exec,
        rx_hyperliquid_exec,
    ) =
        mpsc::unbounded_channel::<ExecCommand>();


    execution_routes.register(
        Venue::Hyperliquid,
        tx_hyperliquid_exec,
    );


    // ============================================================
    // FUTURE ALPACA EXECUTION ROUTE
    // ============================================================

    // TODO:
    // Add once the Alpaca execution adapter has been migrated to the new
    // canonical Order / ExecCommand / ExecEvent architecture.
    //
    // let (tx_alpaca_exec, rx_alpaca_exec) =
    //     mpsc::unbounded_channel::<ExecCommand>();
    //
    // execution_routes.register(
    //     Venue::Alpaca,
    //     tx_alpaca_exec,
    // );


    // ============================================================
    // FUTURE IBKR EXECUTION ROUTE
    // ============================================================

    // TODO:
    // Add once the IBKR execution adapter is implemented against the
    // canonical execution architecture.
    //
    // let (tx_ibkr_exec, rx_ibkr_exec) =
    //     mpsc::unbounded_channel::<ExecCommand>();
    //
    // execution_routes.register(
    //     Venue::InteractiveBrokers,
    //     tx_ibkr_exec,
    // );


    // ============================================================
    // START EXECUTION ROUTER
    // ============================================================

    // The router is completely venue-independent.
    tokio::spawn(
        run_execution_router(
            rx_exec_router,
            execution_routes,
            true,
        )
    );


    // ============================================================
    // START HYPERLIQUID EXECUTION ADAPTER
    // ============================================================

    // Engine
    //     -> tx_exec
    //     -> ExecutionRouter
    //     -> tx_hyperliquid_exec
    //     -> HyperliquidAdapter
    //     -> Hyperliquid
    //
    // Hyperliquid
    //     -> HyperliquidAdapter
    //     -> tx_exec_evt
    //     -> Engine
    //
    // Live execution adapters do not run during simulation or historical
    // backtesting.
    if !cfg.enable_backtest_mode
        && !cfg.enable_simulation_mode
    {
        let adapter_cfg =
            cfg.clone();

        let tx_exec_evt =
            tx_exec_evt.clone();


        // The shared Hyperliquid Meta cache is intentionally kept alive here.
        //
        // Next, run_hyperliquid_adapter() can accept Arc<RwLock<Meta>> and use
        // the already-fetched startup Meta when constructing ExchangeClient,
        // eliminating another metadata API request.
        let _hyperliquid_meta_cache_for_adapter =
            hyperliquid_meta_cache
                .as_ref()
                .map(
                    Arc::clone
                );


        tokio::spawn(
            async move {
                run_hyperliquid_adapter(
                    adapter_cfg,
                    rx_hyperliquid_exec,
                    tx_exec_evt,
                )
                .await;
            }
        );
    }


    // ============================================================
    // LIVE MARKET DATA -> ENGINE PRICE TICKS
    // ============================================================

    // In live/simulation mode, subscribe the Engine to the market-data
    // broadcast bus and convert incoming canonical market-data events into
    // EngineEvents.
    //
    // Backtest mode uses its historical deterministic event path instead.
    if !cfg.enable_backtest_mode {
        let mut rx_for_engine =
            tx_bus.subscribe();

        let tx_engine_for_market =
            tx_engine.clone();


        tokio::spawn(
            async move {
                loop {
                    match rx_for_engine
                        .recv()
                        .await
                    {
                        Ok(env) => {
                            if let Some(event) =
                                map_app_to_live_engine_event(
                                    &env
                                )
                            {
                                let _ =
                                    tx_engine_for_market.send(
                                        event
                                    );
                            }
                        }


                        Err(
                            broadcast::error::RecvError::Lagged(n)
                        ) => {
                            eprintln!(
                                "[ENGINE MARKET] lagged {n} events"
                            );
                        }


                        Err(
                            broadcast::error::RecvError::Closed
                        ) => {
                            break;
                        }
                    }
                }
            }
        );
    }


    // ============================================================
    // ENABLED STRATEGIES
    // ============================================================

    // The same StrategyKind identifies a strategy family while the factory
    // chooses the live or historical implementation.
    let enabled =
        vec![
            // StrategyKind::Base,
            // StrategyKind::MeanReversion,
            StrategyKind::MarketOrder,
        ];


    // ============================================================
    // BACKTEST STRATEGY STATE
    // ============================================================

    // Each backtest strategy receives its own bounded channel to preserve
    // deterministic ordering/backpressure.
    let mut backtest_senders:
        Vec<mpsc::Sender<BacktestStrategyEvent>> =
        Vec::new();

    let mut backtest_strategy_handles =
        Vec::new();


    // ============================================================
    // START STRATEGIES
    // ============================================================

    if cfg.enable_backtest_mode {

        // ========================================================
        // BACKTEST STRATEGIES
        // ========================================================

        for kind in enabled {
            let mut strategy:
                Box<dyn Strategy> =
                make_backtest_strategy(
                    &kind,
                    tx_engine.clone(),
                    rx_view.clone(),
                );


            let (
                tx_strategy,
                mut rx_strategy,
            ) =
                mpsc::channel::<BacktestStrategyEvent>(
                    1024
                );


            backtest_senders.push(
                tx_strategy
            );


            let handle =
                tokio::spawn(
                    async move {
                        while let Some(msg) =
                            rx_strategy.recv().await
                        {
                            strategy
                                .on_event(
                                    msg.env
                                )
                                .await;


                            // Signal that this strategy has fully processed
                            // the current historical event.
                            let _ =
                                msg.done.send(
                                    ()
                                );
                        }


                        println!(
                            "[{:?}] backtest event loop exited",
                            kind
                        );
                    }
                );


            backtest_strategy_handles.push(
                handle
            );
        }

    } else {

        // ========================================================
        // LIVE / SIMULATION STRATEGIES
        // ========================================================

        for kind in enabled {
            let mut strategy:
                Box<dyn Strategy> =
                make_strategy(
                    &kind,
                    tx_engine.clone(),
                    rx_view.clone(),
                );


            let mut rx =
                tx_bus.subscribe();


            tokio::spawn(
                async move {
                    loop {
                        match rx
                            .recv()
                            .await
                        {
                            Ok(ev) => {
                                strategy
                                    .on_event(
                                        ev
                                    )
                                    .await;
                            }


                            Err(
                                broadcast::error::RecvError::Lagged(n)
                            ) => {
                                eprintln!(
                                    "[{:?}] lagged {n}",
                                    kind
                                );
                            }


                            Err(
                                broadcast::error::RecvError::Closed
                            ) => {
                                break;
                            }
                        }
                    }
                }
            );
        }
    }


    // ============================================================
    // MARKET DATA SOURCE
    // ============================================================

    if cfg.enable_backtest_mode {

        // ========================================================
        // RUN BACKTEST
        // ========================================================

        data::historical::alpaca::runner::run_alpaca_backtest_feed(
            &backtest_senders,
            &tx_engine,
        )
        .await
        .map_err(
            |error| {
                anyhow::anyhow!(
                    error.to_string()
                )
            }
        )?;


        // ========================================================
        // CLOSE BACKTEST STRATEGY CHANNELS
        // ========================================================

        drop(
            backtest_senders
        );


        // ========================================================
        // WAIT FOR BACKTEST STRATEGIES
        // ========================================================

        for handle in
            backtest_strategy_handles
        {
            handle.await?;
        }


        println!(
            "[BACKTEST] all strategies finished processing historical events"
        );

    } else {

        // ========================================================
        // RUN LIVE HYPERLIQUID MARKET DATA
        // ========================================================

        let market_data_cfg =
            cfg.clone();


        data::live::hyperliquid::market_data::run_hyperliquid_feed(
            market_data_cfg.clone(),
            tx_master,
            market_data_cfg.symbols.clone(),
        )
        .await
        .map_err(
            |error| {
                anyhow::anyhow!(
                    error.to_string()
                )
            }
        )?;


        // ========================================================
        // FUTURE ALPACA MARKET DATA
        // ========================================================

        // data::live::alpaca::market_data::run_alpaca_feed(
        //     market_data_cfg,
        //     tx_master,
        // )
        // .await
        // .map_err(
        //     |error| {
        //         anyhow::anyhow!(
        //             error.to_string()
        //         )
        //     }
        // )?;
    }


    Ok(())
}