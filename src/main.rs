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

use tokio::sync::{
    broadcast,
    mpsc,
    watch,
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
};

use crate::strategies::{
    make_backtest_strategy,
    make_strategy,
    Strategy,
    StrategyKind,
};

use crate::data::{
    historical::strategy_event::BacktestStrategyEvent,
    live::event_mapper::map_app_to_live_engine_event,
};

use crate::exchange::router::{
    run_execution_router,
    ExecutionRoutes,
};

use crate::exchange::hyperliquid::adapter::{
    run_hyperliquid_exchange_adapter,
};


#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    println!("Starting trading engine...");


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

        max_gross_positions: 4,

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
    let (tx_master, mut rx_master) =
        mpsc::unbounded_channel::<AppEventEnvelope>();


    // ============================================================
    // MASTER -> BROADCAST BUS
    // ============================================================

    let (tx_bus, _drop) =
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
    let (tx_engine, mut rx_engine) =
        mpsc::unbounded_channel::<EngineEvent>();


    // ============================================================
    // ENGINE -> EXECUTION ROUTER
    // ============================================================

    // The Engine sends every canonical ExecCommand through this channel.
    //
    // The receiver belongs to the execution router rather than directly
    // to Alpaca, Hyperliquid, IBKR, or any other venue adapter.
    let (tx_exec, rx_exec_router) =
        mpsc::unbounded_channel::<ExecCommand>();


    // ============================================================
    // EXECUTION ADAPTERS -> ENGINE
    // ============================================================

    // All live venue adapters eventually clone this sender and report
    // canonical execution facts back to the authoritative Engine.
    let (tx_exec_evt, mut rx_exec_evt) =
        mpsc::unbounded_channel::<ExecEvent>();


    // ============================================================
    // EXECUTION ROUTER
    // ============================================================

    // Each registered venue receives its own command channel.
    //
    // Only register a venue when its adapter is actually running and
    // consuming the corresponding receiver.
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

    // The router is completely venue-independent. It receives canonical
    // ExecCommands from the Engine and forwards them according to Venue.
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
    if !cfg.enable_backtest_mode
        && !cfg.enable_simulation_mode
    {
        let adapter_cfg =
            cfg.clone();

        let tx_exec_evt =
            tx_exec_evt.clone();

        tokio::spawn(
            async move {
                run_hyperliquid_exchange_adapter(
                    adapter_cfg,
                    rx_hyperliquid_exec,
                    tx_exec_evt,
                )
                .await;
            }
        );
    }



    // ============================================================
    // ENGINE VIEW
    // ============================================================

    let (tx_view, rx_view) =
        watch::channel::<Arc<EngineView>>(
            EngineView::empty(
                cfg.clone()
            )
        );


    // ============================================================
    // LIVE MARKET DATA -> ENGINE PRICE TICKS
    // ============================================================

    // In live/simulation mode, subscribe the Engine to the market-data
    // broadcast bus and convert incoming canonical market-data events
    // into EngineEvents.
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
                                    tx_engine_for_market
                                        .send(
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
    // ENGINE
    // ============================================================

    // `tx_exec` now points to the execution router rather than directly
    // to a specific exchange adapter.
    let mut engine =
        Engine::new(
            cfg.clone(),
            tx_view.clone(),
            tx_engine.clone(),
            tx_exec.clone(),
        );


    // ============================================================
    // ENGINE MAIN LOOP
    // ============================================================

    // The Engine is the single authoritative owner of trading state.
    //
    // It processes:
    //
    // 1. EngineEvents from strategies and market data.
    // 2. Canonical ExecEvents returned by execution adapters.
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
    // ENABLED STRATEGIES
    // ============================================================

    // This list selects which strategy families are enabled.
    //
    // The same StrategyKind identifies the strategy family in live and
    // backtest modes, while the actual implementation depends on mode.
    let enabled =
        vec![
            StrategyKind::Base,
            StrategyKind::MeanReversion,
        ];


    // ============================================================
    // BACKTEST STRATEGY STATE
    // ============================================================

    // Each backtest strategy receives its own bounded channel.
    //
    // This preserves:
    //
    // - event ordering
    // - delivery to every strategy
    // - deterministic backpressure
    // - protection against silently dropped historical events
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


            // Every strategy receives its own bounded channel.
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
                            rx_strategy
                                .recv()
                                .await
                        {
                            strategy
                                .on_event(
                                    msg.env
                                )
                                .await;

                            // Signal that this strategy has fully processed
                            // the current historical market event.
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
        // LIVE STRATEGIES
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

        // Replay every historical event to every enabled backtest strategy
        // while preserving the existing deterministic synchronization path.
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

        // Dropping all senders closes the strategy channels after any
        // already-buffered historical events have been processed.
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