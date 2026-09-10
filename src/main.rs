mod core;
mod exchange;
//mod strategies;
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

// use crate::core::{
//     config::Config,
//     engine::EngineEvent,
//     engine_state::Engine,
//     exec::{ExecCommand, ExecEvent},
// };

// use crate::core::engine_view::EngineView;
// use crate::core::events::AppEventEnvelope;
// use crate::backtest::strategy_event::BacktestStrategyEvent;

// use crate::strategies::{
//     make_strategy,
//     make_backtest_strategy,
//     Strategy,
//     StrategyKind,
// };

// use crate::exchange::alpaca_exchange_adapter::run_exchange_adapter;
// use crate::core::engine_shared::engine_event_mapper::{
//     map_app_to_live_engine_event,
// };


#[tokio::main]
async fn main() -> Result<()> {
     Ok(())
}


// #[tokio::main]
// async fn main() -> Result<()> {
//     dotenvy::dotenv().ok();

//     println!("Starting Alpaca bot...");

//     // ============================================================
//     // ALPACA -> MASTER CHANNEL
//     // ============================================================
    
//     let (tx_master, mut rx_master) =
//         mpsc::unbounded_channel::<AppEventEnvelope>();


//     // ============================================================
//     // MASTER -> BROADCAST BUS
//     // ============================================================

//     let (tx_bus, _drop) =
//         broadcast::channel::<AppEventEnvelope>(4096);

//     {
//         let tx_bus = tx_bus.clone();

//         tokio::spawn(async move {
//             while let Some(ev) = rx_master.recv().await {
//                 let _ = tx_bus.send(ev);
//             }
//         });
//     }

    
//     // ============================================================
//     // ENGINE EVENT CHANNEL
//     // ============================================================

//     let (tx_engine, mut rx_engine) =
//         mpsc::unbounded_channel::<EngineEvent>();


//     // ============================================================
//     // ENGINE
//     // ============================================================

//     let cfg = Config {
//         enabled_strategies: vec![
//             "BaseStrategy".to_string(),
//         ],

//         symbols: vec![
//             "AAPL".to_string(),
//             "MSFT".to_string(),
//             "NVDA".to_string(),
//         ],

//         cooldown_ms: 1_000,

//         max_gross_positions: 4,

//         min_trade_age_before_exit_secs: 1,

//         enable_simulation_mode: false,

//         enable_backtest_mode: true,

//         backtest_starting_balance: 100_000.0,

//         enable_discord_notifications: true,
        
//         enable_slack_notifications: true,
//     };


//     // ============================================================
//     // EXECUTION ADAPTER CHANNELS
//     // ============================================================

//     // Engine -> Alpaca execution adapter
//     let (tx_exec, rx_exec) =
//         mpsc::unbounded_channel::<ExecCommand>();

//     // Alpaca execution adapter -> Engine
//     let (tx_exec_evt, mut rx_exec_evt) =
//         mpsc::unbounded_channel::<ExecEvent>();

//     // Start execution adapter only for live trading.
//     //
//     // Backtests should never send real Alpaca orders.
//     // Start execution adapter only for live trading.
//     //
//     // Backtests should never send real Alpaca orders.
//     if !cfg.enable_backtest_mode {
//         let adapter_cfg =
//             cfg.clone();

//         tokio::spawn(async move {
//             run_exchange_adapter(
//                 adapter_cfg,
//                 rx_exec,
//                 tx_exec_evt,
//             )
//             .await;
//         });
//     }


//     // ============================================================
//     // ENGINE VIEW
//     // ============================================================

//     let (tx_view, rx_view) =
//         watch::channel::<Arc<EngineView>>(
//             EngineView::empty()
//         );

    
//     // ============================================================
//     // LIVE MARKET DATA -> ENGINE PRICE TICKS
//     // ============================================================

//     // In live/simulation mode, subscribe the engine to the market-data
//     // broadcast bus and convert incoming trades/quotes into PriceTick events.
//     //
//     // Backtest mode does not use this path. Historical PriceTickBackTest
//     // events will be wired separately with deterministic synchronization.
//     if !cfg.enable_backtest_mode {
//         let mut rx_for_engine =
//             tx_bus.subscribe();

//         let tx_engine_for_market =
//             tx_engine.clone();

//         tokio::spawn(async move {
//             loop {
//                 match rx_for_engine.recv().await {
//                     Ok(env) => {
//                         if let Some(event) =
//                             map_app_to_live_engine_event(
//                                 &env
//                             )
//                         {
//                             let _ =
//                                 tx_engine_for_market
//                                     .send(event);
//                         }
//                     }

//                     Err(
//                         broadcast::error::RecvError::Lagged(n)
//                     ) => {
//                         eprintln!(
//                             "[ENGINE MARKET] lagged {n} events"
//                         );
//                     }

//                     Err(
//                         broadcast::error::RecvError::Closed
//                     ) => {
//                         break;
//                     }
//                 }
//             }
//         });
//     }


//     // ============================================================
//     // ENGINE
//     // ============================================================

//     let mut engine = Engine::new(
//         cfg.clone(),
//         tx_view.clone(),
//         tx_engine.clone(),
//         tx_exec.clone(),
//     )
//     .await;

    
//     // ============================================================
//     // ENGINE MAIN LOOP
//     // ============================================================

//     // The Engine is the single owner of trading state.
//     //
//     // It processes both:
//     // 1. Normal EngineEvents from strategies / market data.
//     // 2. ExecEvents returned by the Alpaca execution adapter.
//     tokio::spawn(async move {
//         loop {
//             tokio::select! {

//                 // Normal engine events
//                 Some(ev) = rx_engine.recv() => {
//                     engine.on_event(ev);
//                 }

//                 // Execution events returned by Alpaca
//                 Some(exec_evt) = rx_exec_evt.recv() => {
//                     engine.on_exec_event(exec_evt);
//                 }

//                 // Both channels are closed
//                 else => {
//                     break;
//                 }
//             }
//         }

//         println!("[ENGINE] main loop exited");
//     });


//     // ============================================================
//     // ENABLED STRATEGIES
//     // ============================================================

//     // This list selects which strategy FAMILIES are enabled.
//     //
//     // The same StrategyKind is used for both live and backtest modes,
//     // but the actual strategy implementation depends on the current mode:
//     //
//     // Live mode:
//     // StrategyKind::Base -> make_strategy() -> BaseStrategy
//     //
//     // Backtest mode:
//     // StrategyKind::Base -> make_backtest_strategy() -> BaseBacktestStrategy
//     //
//     // Therefore, the live BaseStrategy is NOT being sent into the backtest.
//     // StrategyKind::Base simply identifies the "Base" strategy family.
//     //
//     // In backtest mode, every strategy listed here automatically gets its
//     // own bounded mpsc channel and receives every historical market event
//     // with backpressure protection.
//     let enabled = vec![
//         StrategyKind::Base,
//         StrategyKind::MeanReversion,
//     ];

//     // Stores the sender for every backtest strategy.
//     //
//     // Each strategy gets its OWN bounded channel so every strategy
//     // receives every historical market event independently.
//     let mut backtest_senders:
//         Vec<mpsc::Sender<BacktestStrategyEvent>> =
//         Vec::new();


//     // Stores the task handle for every backtest strategy so we can
//     // wait until all strategies finish processing before exiting.
//     let mut backtest_strategy_handles =
//         Vec::new();


//     if cfg.enable_backtest_mode {

//         // ========================================================
//         // BACKTEST STRATEGIES
//         // ========================================================

//         for kind in enabled {

//             let mut strategy: Box<dyn Strategy> =
//                 make_backtest_strategy(
//                     &kind,
//                     tx_engine.clone(),
//                     rx_view.clone(),
//                 );


//             // Every backtest strategy receives its own bounded channel.
//             //
//             // This guarantees:
//             //
//             // 1. Every strategy receives every historical event.
//             // 2. Events remain ordered for that strategy.
//             // 3. A slow strategy cannot silently lose events.
//             // 4. Backpressure slows the replay when a strategy falls behind.
//             let (tx_strategy, mut rx_strategy) =
//                 mpsc::channel::<BacktestStrategyEvent>(
//                     1024
//                 );


//             // Save the sender so the backtest runner can publish
//             // historical events to this strategy.
//             backtest_senders.push(
//                 tx_strategy
//             );


//             // Run this strategy independently.
//             let handle =
//                 tokio::spawn(
//                     async move {

//                         while let Some(msg) =
//                             rx_strategy.recv().await
//                         {
//                             strategy
//                                 .on_event(msg.env)
//                                 .await;

//                             // Tell the backtest runner that this strategy has
//                             // completely processed the historical market event.
//                             let _ = msg.done.send(());
//                         }

//                         println!(
//                             "[{:?}] backtest event loop exited",
//                             kind
//                         );
//                     }
//                 );


//             backtest_strategy_handles.push(
//                 handle
//             );
//         }

//     } else {

//         // ========================================================
//         // LIVE STRATEGIES
//         // ========================================================

//         for kind in enabled {

//             let mut strategy: Box<dyn Strategy> =
//                 make_strategy(
//                     &kind,
//                     tx_engine.clone(),
//                     rx_view.clone(),
//                 );

//             let mut rx =
//                 tx_bus.subscribe();


//             tokio::spawn(
//                 async move {

//                     loop {
//                         match rx.recv().await {

//                             Ok(ev) => {
//                                 strategy
//                                     .on_event(ev)
//                                     .await;
//                             }


//                             Err(
//                                 broadcast::error::RecvError::Lagged(n)
//                             ) => {
//                                 eprintln!(
//                                     "[{:?}] lagged {n}",
//                                     kind
//                                 );
//                             }


//                             Err(
//                                 broadcast::error::RecvError::Closed
//                             ) => {
//                                 break;
//                             }
//                         }
//                     }
//                 }
//             );
//         }
//     }

//     // ============================================================
//     // MARKET DATA SOURCE
//     // ============================================================

//     if cfg.enable_backtest_mode {

//         // ========================================================
//         // RUN BACKTEST
//         // ========================================================

//         // Replay every historical market event to every enabled
//         // backtest strategy while synchronizing engine processing.
//         backtest::runner::run_backtest_feed(
//             &backtest_senders,
//             &tx_engine,
//         )
//         .await
//         .map_err(
//             |e| anyhow::anyhow!(
//                 e.to_string()
//             )
//         )?;


//         // ========================================================
//         // CLOSE BACKTEST STRATEGY CHANNELS
//         // ========================================================

//         // Dropping all senders closes each strategy's channel.
//         //
//         // Each strategy will continue processing any events still
//         // buffered before its receiver finally exits.
//         drop(
//             backtest_senders
//         );


//         // ========================================================
//         // WAIT FOR ALL BACKTEST STRATEGIES
//         // ========================================================

//         // Do not finish the backtest until every strategy has
//         // processed every remaining historical event.
//         for handle in
//             backtest_strategy_handles
//         {
//             handle.await?;
//         }


//         println!(
//             "[BACKTEST] all strategies finished processing historical events"
//         );

//     } else {

//     // ========================================================
//     // RUN LIVE ALPACA FEED
//     // ========================================================

//     let market_data_cfg = cfg.clone();

//         exchange::alpaca_market_data::run_alpaca_feed(
//             market_data_cfg,
//             tx_master,
//         )
//         .await
//         .map_err(
//             |e| anyhow::anyhow!(
//                 e.to_string()
//             )
//         )?;
//     }

//     Ok(())
// }