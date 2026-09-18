#![allow(dead_code)]

use std::time::Duration;

use chrono::Utc;
use tokio::{
    sync::mpsc::UnboundedSender,
    time::{self, MissedTickBehavior},
};

use crate::{
    core::events::{AppEvent, AppEventEnvelope},
    data::live::synthetic::{
        asset::SyntheticAsset,
        generator::SyntheticMarketGenerator,
    },
    market::{
        candle::CandleConfig,
        candle_feed::process_trade_for_candles,
        candle_manager::CandleManager,
    },
};


// ============================================================
// SYNTHETIC LIVE MARKET DATA
// ============================================================
//
// Runs a deterministic synthetic live market-data feed.
//
// Each configured synthetic asset maintains its own independent market
// generator. Generated market data is converted directly into the same
// canonical events produced by real market-data providers:
//
//     SyntheticMarketGenerator
//              │
//              ├── Quote ──────────────> tx_master
//              │
//              └── TapeTrade
//                       │
//                       ├───────────────> tx_master
//                       │
//                       └── CandleManager
//                                │
//                                └── BarClosed ──> tx_master
//
// Strategies and the engine therefore do not need synthetic-specific logic.
//
// IMPORTANT:
// This module generates MARKET DATA only.
//
// It does NOT simulate:
//     - order submission
//     - order acceptance
//     - fills
//     - partial fills
//     - cancellations
//     - replacements
//     - slippage
//     - positions
//
// Those responsibilities belong to the execution simulator.
//
// TODO:
// - Add runtime-selectable synthetic market regimes.
//
// - Add correlated multi-asset movement.
//
// - Add market-wide shocks and crashes.
//
// - Add deterministic scripted scenarios.
//
// - Add configurable quote and trade frequencies independently.
//
// - Add burst-mode generation for high-throughput stress testing.
//
// - Add temporary illiquidity and spread widening.
//
// - Add market-session simulation including open/close behavior.
//
// - Add accelerated-time mode so hours or days of synthetic market
//   activity can be generated in seconds.
//
// - Add synthetic historical data under `data/historical/synthetic/`
//   for deterministic backtester testing.
//
// ============================================================


// ============================================================
// RUN SYNTHETIC FEED
// ============================================================

pub async fn run_synthetic_feed(
    assets: Vec<SyntheticAsset>,
    tx_master: UnboundedSender<AppEventEnvelope>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if assets.is_empty() {
        return Err("synthetic market-data feed requires at least one asset".into());
    }

    println!(
        "[SYNTHETIC MARKET] starting | assets={:?}",
        assets.iter().map(|asset| asset.symbol.as_str()).collect::<Vec<_>>(),
    );

    // Each asset runs independently so a slow/high-frequency synthetic
    // symbol does not control the event rate of every other symbol.
    for asset in assets {
        let tx_master = tx_master.clone();

        tokio::spawn(async move {
            if let Err(error) = run_synthetic_asset_feed(asset, tx_master).await {
                eprintln!("[SYNTHETIC MARKET] asset feed stopped | error={}", error);
            }
        });
    }

    // Keep the provider task alive indefinitely.
    //
    // Individual asset generators run in their own Tokio tasks and publish
    // canonical events directly into tx_master.
    std::future::pending::<()>().await;

    Ok(())
}


// ============================================================
// RUN ONE SYNTHETIC ASSET
// ============================================================

async fn run_synthetic_asset_feed(
    asset: SyntheticAsset,
    tx_master: UnboundedSender<AppEventEnvelope>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let symbol = asset.symbol.clone();
    let events_per_second = asset.events_per_second.max(1);

    let interval_duration = Duration::from_secs_f64(1.0 / events_per_second as f64);

    let mut generator = SyntheticMarketGenerator::new(asset);


    // ============================================================
    // CANDLE MANAGER
    // ============================================================

    // Uses exactly the same candle configuration as the other live
    // providers and historical backtest candle construction.
    let candle_config = CandleConfig::default();
    let mut candle_manager = CandleManager::new(&candle_config.timeframes);

    let mut interval = time::interval(interval_duration);

    // Do not generate a burst of delayed synthetic ticks if the runtime
    // temporarily falls behind.
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    println!(
        "[SYNTHETIC MARKET][{}] started | rate={} updates/sec | starting_price=${:.4}",
        symbol,
        events_per_second,
        generator.mid_price(),
    );


    // ========================================================
    // MARKET LOOP
    // ========================================================

    loop {
        interval.tick().await;

        let timestamp_ms = Utc::now().timestamp_millis();
        let update = generator.next_update(timestamp_ms);


        // ====================================================
        // CANDLE CONSTRUCTION
        // ====================================================

        // Feed the generated public trade through the exact same shared
        // live candle path used by real market-data providers.
        process_trade_for_candles(
            &mut candle_manager,
            &tx_master,
            &update.trade.symbol,
            update.trade.timestamp_ms,
            update.trade.price,
            update.trade.size,
        )?;


        // ====================================================
        // PUBLISH TAPE TRADE
        // ====================================================

        let trade_symbol = update.trade.symbol.clone();

        if tx_master.send(AppEventEnvelope {
            symbol: trade_symbol,
            event: AppEvent::TapeTrade(update.trade),
        }).is_err() {
            return Err("master event channel closed while publishing synthetic tape trade".into());
        }


        // ====================================================
        // PUBLISH QUOTE
        // ====================================================

        let quote_symbol = update.quote.symbol.clone();

        if tx_master.send(AppEventEnvelope {
            symbol: quote_symbol,
            event: AppEvent::Quote(update.quote),
        }).is_err() {
            return Err("master event channel closed while publishing synthetic quote".into());
        }
    }
}