mod core;
mod exchange;
mod strategies;
mod toolbox;
mod indicators;
mod market;
mod shared;
mod notifications;
mod data;
mod fees;
mod storage;

use std::sync::Arc;
use anyhow::Result;
use anyhow::Context;
use chrono::{DateTime, Utc};

use hyperliquid_rust_sdk::Meta;

use tokio::sync::{
    broadcast,
    mpsc,
    watch,
    RwLock,
};

use crate::core::{
    config::Config,
    general::General,
    metrics::Metrics,

    engine::engine::{
        Engine,
        EngineEvent,
    },
    engine::state::ExecutionMode,

    engine_view::EngineView,

    events::AppEventEnvelope,

    execution::{
        ExecCommand,
        ExecEvent,
        Venue,
    },

    venue::VenueStateEvent,
};

use crate::data::{
    historical::{
        provider::HistoricalDataProvider,
        strategy_event::BacktestStrategyEvent,
    },
    live::{
        event_mapper::map_app_to_live_engine_event,
        provider::LiveMarketDataProvider,
        synthetic::asset::default_synthetic_assets,
    },
};

use crate::exchange::{
    alpaca::{
        adapter::run_alpaca_adapter,
        api::get_alpaca_account_balances,
    },

    backtest::{
        adapter::run_backtest_adapter,
        BacktestMarketState,
        SharedBacktestMarketState,
    },

    simulation::{
        adapter::run_simulation_adapter,
        SimulationMarketState,
        SharedSimulationMarketState,
    },

    hyperliquid::{
        adapter::run_hyperliquid_adapter,
        api::{close_all_positions, get_hyperliquid_account_balances},
        meta::{fetch_meta, map_meta_to_instruments},
        pollers::run_hyperliquid_meta_poller,
    },

    ibkr::{
        adapter::run_ibkr_adapter,
        api::get_ibkr_account_balances,
    },

    router::{run_execution_router, ExecutionRoutes},
};

use crate::storage::backtest::save_backtest_report;

use crate::strategies::{
    make_backtest_strategy,
    make_strategy,
    Strategy,
    StrategyKind,
};



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
        // ============================================================
        // EXECUTION / MARKET DATA
        // ============================================================

        execution_mode: ExecutionMode::Backtest,

        execution_venue: Venue::Hyperliquid,

        // Live market data is intentionally independent from execution.
        live_market_data_provider: LiveMarketDataProvider::Hyperliquid,

        // Used only when execution_mode = ExecutionMode::Backtest.
        historical_data_provider: HistoricalDataProvider::Hyperliquid,

        symbols: vec![
            "BTC".to_string(),
            "ETH".to_string(),
            "SOL".to_string(),
            "HYPE".to_string(),
            "NEAR".to_string(),
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

        // symbols: vec![
        //     "FAKEA".to_string(),
        //     "FAKEB".to_string(),
        //     "FAKEC".to_string(),
        // ],

        // ============================================================
        // RISK LIMITS
        // ============================================================
        max_active_trades: Some(20),
        max_active_assets: Some(20),
        max_active_trades_per_symbol: Some(20),
        max_active_trades_per_strategy: Some(20),

        max_order_notional: Some(150_000.0),
        max_gross_notional: Some(150_000.0),
        max_symbol_notional: Some(150_000.0),

        // ============================================================
        // ENGINE SETTINGS
        // ============================================================

        close_trades_on_startup: false,
        min_trade_age_before_exit_secs: 1, // delete later

        custom_starting_balance: 250_000.0,

        enable_discord_notifications: true,
        enable_slack_notifications: true,
    };


    // ============================================================
    // STARTUP POSITION CLEANUP
    // ============================================================

    // Startup cleanup only applies to venue-backed execution modes.
    // Never close venue positions during backtest or simulation mode.
    if cfg.close_trades_on_startup {
        match (cfg.execution_mode, cfg.execution_venue) {
            (ExecutionMode::Live, Venue::Hyperliquid) => {
                if let Err(error) = close_all_positions().await {
                    // TODO: Log fatal Hyperliquid startup position cleanup failure.
                    // TODO: Send Discord/Slack fatal error message before terminating the bot.

                    panic!("Hyperliquid startup position cleanup failed: {}", error);
                }
            }

            (ExecutionMode::Live | ExecutionMode::Paper, Venue::Alpaca) => {
                // TODO: Add Alpaca startup position cleanup.
            }

            (ExecutionMode::Live | ExecutionMode::Paper, Venue::InteractiveBrokers) => {
                // TODO: Add Interactive Brokers startup position cleanup.
            }

            _ => {}
        }
    }

 
    // ============================================================
    // INITIAL ACCOUNT BALANCE
    // ============================================================

    // Backtest and simulation use configured starting capital.
    // Live and supported paper modes obtain the account balance
    // from the configured execution venue.

    let starting_balance = match (cfg.execution_mode, cfg.execution_venue) {
        (ExecutionMode::Backtest | ExecutionMode::Simulation, _) => {
            cfg.custom_starting_balance
        }

        (ExecutionMode::Live, Venue::Hyperliquid) => {
            let (account_value, _, _, _, _) =
                get_hyperliquid_account_balances().await?;

            account_value
        }

        (ExecutionMode::Live | ExecutionMode::Paper, Venue::Alpaca) => {
            let account = get_alpaca_account_balances().await?;

            account.portfolio_value.parse::<f64>()
                .context("Failed to parse Alpaca portfolio value")?
        }

        (ExecutionMode::Live | ExecutionMode::Paper, Venue::InteractiveBrokers) => {
            // TODO: Move IBKR connection settings into Config or .env.
            let address = "127.0.0.1:7497";
            let client_id = 201; // Must be unique from other active IBKR client ID connections.

            let timezone_name = "Hawaii-Aleutian Standard Time";
            let timezone_alias = "Pacific/Honolulu";

            // TODO: Potentially add account_id filtering for future multi-account support.
            get_ibkr_account_balances(
                address,
                client_id,
                timezone_name,
                timezone_alias,
            ).await?
        }

        (ExecutionMode::Paper, Venue::Hyperliquid) => {
            anyhow::bail!("Hyperliquid does not support paper trading");
        }
    };

    let starting_date = match cfg.execution_mode {
        ExecutionMode::Live | ExecutionMode::Paper | ExecutionMode::Simulation => {
            Utc::now()
        }

        ExecutionMode::Backtest => {
           DateTime::<Utc>::from_timestamp_millis(0).unwrap()
        }
    };

    let general = General {
        realized_pnl: 0.0,
        sim_realized_pnl: 0.0,
        starting_balance,
        starting_date,
        live_closed_trades: 0,
        sim_closed_trades: 0,
    };

    println!("Starting balance: ${:.2}", general.starting_balance);


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


    // The was move to the below section:   MASTER -> BROADCAST BUS DISPATCHER
    // {
    //     let tx_bus =
    //         tx_bus.clone();


    //     tokio::spawn(
    //         async move {
    //             while let Some(ev) =
    //                 rx_master.recv().await
    //             {
    //                 let _ =
    //                     tx_bus.send(
    //                         ev
    //                     );
    //             }
    //         }
    //     );
    // }


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
    // BACKTEST MARKET STATE
    // ============================================================

    // Shared historical market state used by the historical replay and
    // backtest execution adapter.
    //
    // The replay updates this state before strategies process each historical
    // market event. The backtest adapter reads it when simulating execution.
    let backtest_market_state:
        SharedBacktestMarketState =
        Arc::new(
            RwLock::new(
                BacktestMarketState::new()
            )
        );
    

    // ============================================================
    // SIMULATION MARKET STATE
    // ============================================================

    // Shared live market state used by the simulation execution adapter.
    //
    // The master market-data dispatcher updates this state before
    // broadcasting live events to the Engine and strategies.
    let simulation_market_state:
        SharedSimulationMarketState =
        Arc::new(
            RwLock::new(
                SimulationMarketState::new()
            )
        );


    // ============================================================
    // MASTER -> BROADCAST BUS DISPATCHER
    // ============================================================

    // In Simulation mode, update shared market state before
    // broadcasting events to the Engine and strategies.
    {
        let tx_bus =
            tx_bus.clone();

        let simulation_market_state =
            Arc::clone(
                &simulation_market_state
            );

        let execution_mode =
            cfg.execution_mode;

        tokio::spawn(
            async move {
                while let Some(ev) =
                    rx_master.recv().await
                {
                    // ====================================================
                    // UPDATE SIMULATION MARKET STATE
                    // ====================================================

                    if execution_mode == ExecutionMode::Simulation {
                        let mut state =
                            simulation_market_state.write().await;

                        state.update_event(
                            &ev
                        );
                    }

                    // ====================================================
                    // BROADCAST MARKET EVENT
                    // ====================================================

                    let _ =
                        tx_bus.send(
                            ev
                        );
                }
            }
        );
    }


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
    if cfg.execution_mode == ExecutionMode::Live
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
    
    let metrics = Metrics::new();

    let mut engine = Engine::new(
        cfg.clone(),
        general,
        metrics,
        tx_view.clone(),
        tx_engine.clone(),
        tx_exec.clone(),
    );

    let debug_mode = engine.debug_mode;

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
    if matches!(
        cfg.execution_mode,
        ExecutionMode::Live | ExecutionMode::Simulation
    ) {
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


    // ============================================================
    // REGISTER ROUTING PER VENUE ADAPTER
    // ============================================================

    execution_routes.register(
        Venue::Hyperliquid,
        tx_hyperliquid_exec,
    );


    let (tx_alpaca_exec, rx_alpaca_exec) =
        mpsc::unbounded_channel::<ExecCommand>();
    
    execution_routes.register(
        Venue::Alpaca,
        tx_alpaca_exec,
    );


    let (tx_ibkr_exec, rx_ibkr_exec) =
        mpsc::unbounded_channel::<ExecCommand>();

    execution_routes.register(
        Venue::InteractiveBrokers,
        tx_ibkr_exec,
    );


    // ============================================================
    // START EXECUTION ROUTER
    // ============================================================

    // The router is completely venue-independent.
    tokio::spawn(
        run_execution_router(
            rx_exec_router,
            execution_routes,
            debug_mode,
        )
    );


    // ============================================================
    // START EXECUTION ADAPTERS
    // ============================================================

    // The execution router routes canonical ExecCommands by Venue.
    //
    // Live/Paper mode:
    //   Commands are consumed by their venue-specific execution adapter.
    //
    // Backtest mode:
    //   Commands are consumed by the shared backtest execution adapter
    //   using the receiver associated with the configured execution venue.
    //
    // Historical market data providers are independent of execution venues.
    // Synthetic historical data can therefore execute simulated orders
    // through Hyperliquid, Alpaca, or InteractiveBrokers.
    //
    // The Engine -> ExecCommand -> ExecutionRouter architecture remains
    // identical across Live, Paper, Backtest, and Simulation modes.
    //
    // Live/Paper:
    //   Venue-specific execution adapters.
    //
    // Backtest:
    //   Shared backtest adapter using historical market data.
    //
    // Simulation:
    //   Shared simulation adapter using live market data.

    match cfg.execution_mode {

        // ========================================================
        // LIVE / PAPER EXECUTION
        // ========================================================

        ExecutionMode::Live | ExecutionMode::Paper => {

            match cfg.execution_venue {

                // ========================================================
                // HYPERLIQUID LIVE
                // ========================================================

                Venue::Hyperliquid => {
                    if cfg.execution_mode == ExecutionMode::Paper {
                        return Err(anyhow::anyhow!(
                            "Hyperliquid does not support ExecutionMode::Paper. Use Live or Backtest."
                        ));
                    }

                    let adapter_cfg =
                        cfg.clone();

                    let tx_exec_evt =
                        tx_exec_evt.clone();

                    let meta_cache_for_adapter =
                        Arc::clone(
                            hyperliquid_meta_cache
                                .as_ref()
                                .expect(
                                    "Hyperliquid Meta cache missing before adapter startup"
                                )
                        );

                    tokio::spawn(
                        async move {
                            run_hyperliquid_adapter(
                                adapter_cfg,
                                meta_cache_for_adapter,
                                rx_hyperliquid_exec,
                                tx_exec_evt,
                                debug_mode,
                            )
                            .await;
                        }
                    );
                }


                // ========================================================
                // ALPACA LIVE / PAPER
                // ========================================================

                Venue::Alpaca => {
                    let adapter_cfg =
                        cfg.clone();

                    let tx_exec_evt =
                        tx_exec_evt.clone();

                    tokio::spawn(
                        async move {
                            run_alpaca_adapter(
                                adapter_cfg,
                                rx_alpaca_exec,
                                tx_exec_evt,
                                debug_mode,
                            )
                            .await;
                        }
                    );
                }


                // ========================================================
                // IBKR LIVE / PAPER
                // ========================================================
                //
                // Engine
                //     -> tx_exec
                //     -> ExecutionRouter
                //     -> tx_ibkr_exec
                //     -> IBKRAdapter
                //     -> TWS / IB Gateway
                //     -> IBKR
                //
                // IBKR order updates
                //     -> IBKRAdapter
                //     -> tx_exec_evt
                //     -> Engine

                Venue::InteractiveBrokers => {
                    let tx_exec_evt =
                        tx_exec_evt.clone();

                    // Temporary IBKR connection configuration while proving the adapter.
                    //
                    // TODO:
                    // Move IBKR connection settings into Config or .env after the adapter
                    // lifecycle has been fully tested.
                    let address = "127.0.0.1:7497".to_string();
                    let client_id = 200;

                    // Temporary TWS timezone mapping.
                    // Move into canonical IBKR configuration later.
                    let timezone_name =
                        "Hawaii-Aleutian Standard Time".to_string();

                    let timezone_alias =
                        "Pacific/Honolulu".to_string();

                    tokio::spawn(
                        async move {
                            run_ibkr_adapter(
                                address,
                                client_id,
                                timezone_name,
                                timezone_alias,
                                rx_ibkr_exec,
                                tx_exec_evt,
                                debug_mode,
                            )
                            .await;
                        }
                    );
                }
            }
        }


        // ========================================================
        // SHARED BACKTEST EXECUTION
        // ========================================================

        ExecutionMode::Backtest => {

            // Select the execution receiver corresponding to the
            // configured execution venue.
            //
            // The execution router still routes by canonical Venue,
            // but all backtest venues use the same execution simulator.

            let rx_backtest_exec =
                match cfg.execution_venue {

                    Venue::Hyperliquid => {
                        rx_hyperliquid_exec
                    }

                    Venue::Alpaca => {
                        rx_alpaca_exec
                    }

                    Venue::InteractiveBrokers => {
                        rx_ibkr_exec
                    }
                };


            let market_state =
                Arc::clone(
                    &backtest_market_state
                );

            let tx_exec_evt =
                tx_exec_evt.clone();

            tokio::spawn(
                async move {
                    run_backtest_adapter(
                        rx_backtest_exec,
                        tx_exec_evt,
                        market_state,
                        debug_mode,
                    )
                    .await;
                }
            );
        }


        // ========================================================
        // SHARED SIMULATION EXECUTION
        // ========================================================

        ExecutionMode::Simulation => {

            // Select the execution receiver corresponding to the
            // configured execution venue.
            //
            // The execution router continues routing commands by Venue.
            //
            // All simulation venues use the shared simulation adapter
            // instead of submitting orders to real exchanges.
            //
            // Live market-data providers update SharedSimulationMarketState
            // independently through the master market-data dispatcher.
            //
            // The simulation adapter reads the latest bid/ask quotes
            // when executing simulated market orders.

            let rx_simulation_exec =
                match cfg.execution_venue {

                    Venue::Hyperliquid => {
                        rx_hyperliquid_exec
                    }

                    Venue::Alpaca => {
                        rx_alpaca_exec
                    }

                    Venue::InteractiveBrokers => {
                        rx_ibkr_exec
                    }
                };


            // ========================================================
            // SHARED LIVE SIMULATION MARKET STATE
            // ========================================================

            let market_state =
                Arc::clone(
                    &simulation_market_state
                );


            // ========================================================
            // EXECUTION EVENT CHANNEL
            // ========================================================

            let tx_exec_evt =
                tx_exec_evt.clone();


            // ========================================================
            // CONFIGURED EXECUTION VENUE
            // ========================================================

            let execution_venue =
                cfg.execution_venue;


            // ========================================================
            // START SIMULATION ADAPTER
            // ========================================================

            tokio::spawn(
                async move {
                    run_simulation_adapter(
                        rx_simulation_exec,
                        tx_exec_evt,
                        market_state,
                        execution_venue,
                        debug_mode,
                    )
                    .await;
                }
            );
        }
    }


    // ============================================================
    // LIVE MARKET DATA -> ENGINE PRICE TICKS
    // ============================================================

    // Live, paper, and simulation modes subscribe the Engine to the
    // live market-data broadcast bus and convert incoming canonical
    // market-data events into EngineEvents.
    //
    // Backtest mode uses its historical deterministic event path instead.
    if matches!(
        cfg.execution_mode,
        ExecutionMode::Live | ExecutionMode::Paper | ExecutionMode::Simulation
    ) {
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
    let enabled = vec![
        // StrategyKind::ResearchV1,
        // StrategyKind::ResearchV2,
        StrategyKind:: ResearchV3,
        // StrategyKind::Base,
        // StrategyKind::Indicator,
        // StrategyKind::MeanReversion,
        // StrategyKind::MarketOrder,
        // StrategyKind::LimitOrder,
        // StrategyKind::LimitOrderAdvanced,
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

    if cfg.execution_mode == ExecutionMode::Backtest {

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

    if cfg.execution_mode == ExecutionMode::Backtest {

        // ========================================================
        // RUN BACKTEST
        // ========================================================

        // Historical market-data selection is independent from both
        // the execution venue and live market-data provider.
        //
        // Each historical provider produces canonical historical
        // market data which is replayed deterministically through
        // the backtest strategy/Engine pipeline.
        match cfg.historical_data_provider {
            // ====================================================
            // ALPACA
            // ====================================================

            HistoricalDataProvider::Alpaca => {
                data::historical::alpaca::runner::run_alpaca_backtest_feed(
                    &backtest_senders,
                    &tx_engine,
                    &tx_exec,
                    &tx_exec_evt,
                    &backtest_market_state,
                    cfg.execution_venue,
                )
                .await
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
            }


            // ====================================================
            // SYNTHETIC
            // ====================================================

            HistoricalDataProvider::Synthetic => {
                // TODO:
                // Move the backtest date range into a dedicated backtest
                // configuration so every historical provider uses the
                // same configured start/end dates.
                //
                // Synthetic historical time is generated and replayed as
                // fast as possible. It does not wait for real wall-clock
                // time, allowing large historical ranges to be simulated
                // efficiently.
                let start =
                    chrono::DateTime::parse_from_rfc3339(
                        "2026-08-03T13:30:00Z"
                    )?
                    .with_timezone(&chrono::Utc);

                let end =
                    chrono::DateTime::parse_from_rfc3339(
                        "2026-08-03T20:00:00Z"
                    )?
                    .with_timezone(&chrono::Utc);

                let assets =
                    data::live::synthetic::asset::default_synthetic_assets();

                data::historical::synthetic::runner::run_synthetic_backtest_feed(
                    assets,
                    start,
                    end,
                    &backtest_senders,
                    &tx_engine,
                    &tx_exec,
                    &tx_exec_evt,
                    &backtest_market_state,
                    cfg.execution_venue,
                )
                .await
                .map_err(|error: Box<dyn std::error::Error>| anyhow::anyhow!(error.to_string()))?;
            }


            // ====================================================
            // HYPERLIQUID
            // ====================================================

            HistoricalDataProvider::Hyperliquid => {
                data::historical::hyperliquid::runner::run_hyperliquid_backtest_feed(
                    &backtest_senders,
                    &tx_engine,
                    &tx_exec,       // NEW: Engine -> Execution Router
                    &tx_exec_evt,
                    &backtest_market_state,
                    cfg.execution_venue,
                )
                .await
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
            }


            // ====================================================
            // DATABENTO
            // ====================================================

            HistoricalDataProvider::Databento => {
                return Err(
                    anyhow::anyhow!(
                        "Databento historical backtest provider is not connected yet"
                    )
                );
            }
        }


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

        // ============================================================
        // SAVE FINAL BACKTEST RESULTS
        // ============================================================

        // IMPORTANT:
        // All historical events, execution commands, and execution
        // fills must be fully processed before requesting results.
        //
        // Waiting for strategies to finish does NOT guarantee that
        // all execution events have been processed by the Engine

        let (reply_tx, reply_rx) =
            tokio::sync::oneshot::channel();

        // Request results from authoritative Engine state.
        tx_engine.send(
            EngineEvent::GetBacktestResult {
                reply: reply_tx,
            }
        )?;

        // Wait for Engine to return the final results.
        let result =
            reply_rx.await?;

        // Print and save the results.
        save_backtest_report(
            &result
        )?;

    } else {

        // ========================================================
        // RUN LIVE MARKET DATA
        // ========================================================

        let market_data_cfg = cfg.clone();
        let market_data_symbols = market_data_cfg.symbols.clone();


        // Select the live market-data provider independently from the
        // configured execution venue.
        //
        // The execution venue determines where orders are sent.
        // The live market-data provider determines where Quote, TapeTrade,
        // and BarClosed events originate.
        //
        // Examples:
        //
        //     Hyperliquid execution + Hyperliquid market data
        //     Alpaca execution + Alpaca market data
        //     IBKR execution + Alpaca market data
        //     Simulation execution + Synthetic market data
        match cfg.live_market_data_provider {
            // ====================================================
            // HYPERLIQUID
            // ====================================================

            LiveMarketDataProvider::Hyperliquid => {
                data::live::hyperliquid::market_data::run_hyperliquid_feed(
                    market_data_cfg,
                    tx_master,
                    market_data_symbols,
                )
                .await
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
            }


            // ====================================================
            // ALPACA
            // ====================================================

            LiveMarketDataProvider::Alpaca => {
                data::live::alpaca::market_data::run_alpaca_feed(
                    market_data_cfg,
                    tx_master,
                    market_data_symbols,
                )
                .await
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
            }


            // ====================================================
            // SYNTHETIC
            // ====================================================

            LiveMarketDataProvider::Synthetic => {
                data::live::synthetic::market_data::run_synthetic_feed(
                    data::live::synthetic::asset::default_synthetic_assets(),
                    tx_master,
                )
                .await
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
            }
        }
    }


    Ok(())
}