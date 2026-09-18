#![allow(dead_code)]

use std::time::Duration;

use chrono::{
    DateTime,
    Utc,
};

use tokio::sync::{
    mpsc::{
        Sender,
        UnboundedSender,
    },
    oneshot,
};

use crate::{
    core::{
        engine::engine::EngineEvent,
        events::{
            AppEvent,
            AppEventEnvelope,
            Bar,
            TapeTrade,
        },
    },

    data::{
        historical::{
            downsample::downsample_trades,
            event_mapper::map_app_to_backtest_engine_event,
            strategy_event::BacktestStrategyEvent,
            synthetic::generator::generate_historical_trades,
            types::HistoricalTrade,
        },

        live::synthetic::asset::SyntheticAsset,
    },

    market::{
        candle::CandleConfig,
        candle_manager::CandleManager,
    },
};


// ============================================================
// SYNTHETIC BACKTEST FEED
// ============================================================

/// Generates deterministic synthetic historical tape data, builds candles
/// from the complete tape, downsamples the strategy trade stream, and replays
/// the resulting historical timeline as quickly as the engine and strategies
/// can process it.
///
/// TODO:
/// - Move the common candle/downsample/replay pipeline shared with Alpaca
///   into a provider-independent historical replay runner.
/// - Add configurable date ranges through Config/backtest configuration.
/// - Add trading calendars and session filtering.
/// - Add chunked generation/replay for very large multi-year datasets.
/// - Add configurable synthetic market regimes.
/// - Add deterministic multi-asset correlation.
/// - Add synthetic historical quotes/order-book data if required by future
///   strategies or realistic limit-order simulation.
pub async fn run_synthetic_backtest_feed(
    assets: Vec<SyntheticAsset>,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    tx_backtest_strategies: &[Sender<BacktestStrategyEvent>],
    tx_engine: &UnboundedSender<EngineEvent>,
) -> Result<(), Box<dyn std::error::Error>> {
    if assets.is_empty() {
        return Err("Synthetic historical asset list cannot be empty".into());
    }

    if end <= start {
        return Err("Synthetic historical end must be after start".into());
    }

    let candle_config = CandleConfig::default();
    let min_trade_interval = Duration::from_millis(300);

    // ============================================================
    // GENERATE RAW HISTORICAL TRADES
    // ============================================================

    let mut raw_trades = Vec::new();

    for asset in &assets {
        let mut trades = generate_historical_trades(
            asset,
            start,
            end,
        );

        raw_trades.append(
            &mut trades
        );
    }

    raw_trades.sort_by_key(
        |trade| trade.timestamp
    );

    println!(
        "[SYNTHETIC BACKTEST] generated {} raw historical trades | assets={} | start={} | end={}",
        raw_trades.len(),
        assets.len(),
        start,
        end,
    );


    // ============================================================
    // BUILD CANDLES FROM FULL RAW TAPE
    // ============================================================

    let mut candle_manager =
        CandleManager::new(
            &candle_config.timeframes
        );

    let mut bars = Vec::new();

    for trade in &raw_trades {
        bars.extend(
            candle_manager.on_trade(
                &trade.symbol,
                trade.timestamp,
                trade.price,
                trade.size,
            )
        );
    }

    bars.extend(
        candle_manager.close_through(
            end
        )
    );


    println!(
        "[SYNTHETIC BACKTEST] built {} historical candles",
        bars.len()
    );


    // ============================================================
    // DOWNSAMPLE STRATEGY TRADE STREAM
    // ============================================================

    let original_trade_count =
        raw_trades.len();

    let trades =
        downsample_trades(
            raw_trades,
            min_trade_interval,
        );


    println!(
        "[SYNTHETIC BACKTEST] downsampled | original={} | kept={} | removed={} | interval={}ms",
        original_trade_count,
        trades.len(),
        original_trade_count - trades.len(),
        min_trade_interval.as_millis(),
    );


    // ============================================================
    // BUILD REPLAY TIMELINE
    // ============================================================

    let mut replay_events =
        Vec::with_capacity(
            trades.len() + bars.len()
        );

    for trade in trades {
        replay_events.push(
            BacktestReplayEvent::Trade(
                trade
            )
        );
    }

    for bar in bars {
        replay_events.push(
            BacktestReplayEvent::Bar(
                bar
            )
        );
    }


    replay_events.sort_by(
        |a, b| {
            a.timestamp()
                .cmp(&b.timestamp())
                .then_with(|| {
                    a.replay_priority()
                        .cmp(
                            &b.replay_priority()
                        )
                })
        }
    );


    println!(
        "[SYNTHETIC BACKTEST] replaying {} historical market events",
        replay_events.len()
    );


    // ============================================================
    // REPLAY
    // ============================================================

    for replay_event in replay_events {
        let event =
            match replay_event {
                BacktestReplayEvent::Trade(trade) => {
                    let symbol = trade.symbol.clone();

                    AppEventEnvelope {
                        symbol: symbol.clone(),

                        event: AppEvent::TapeTrade(
                            TapeTrade {
                                symbol,
                                price: trade.price,
                                size: trade.size,
                                timestamp_ms: trade.timestamp.timestamp_millis(),
                            }
                        ),
                    }
                }

                BacktestReplayEvent::Bar(bar) => {
                    AppEventEnvelope {
                        symbol: bar.symbol.clone(),
                        event: AppEvent::BarClosed(bar),
                    }
                }
            };


        // ========================================================
        // ENGINE HISTORICAL PRICE
        // ========================================================

        if let Some(engine_event) =
            map_app_to_backtest_engine_event(
                &event
            )
        {
            tx_engine
                .send(
                    engine_event
                )
                .map_err(
                    |_| "Engine event channel closed"
                )?;

            wait_for_engine(
                tx_engine
            )
            .await?;
        }


        // ========================================================
        // STRATEGIES
        // ========================================================

        let mut acknowledgements =
            Vec::with_capacity(
                tx_backtest_strategies.len()
            );

        for tx_strategy in tx_backtest_strategies {
            let (tx_done, rx_done) =
                oneshot::channel();

            tx_strategy
                .send(
                    BacktestStrategyEvent {
                        env: event.clone(),
                        done: tx_done,
                    }
                )
                .await
                .map_err(
                    |_| "Backtest strategy channel closed"
                )?;

            acknowledgements.push(
                rx_done
            );
        }


        for done in acknowledgements {
            done
                .await
                .map_err(
                    |_| "Backtest strategy acknowledgement dropped"
                )?;
        }


        // Process any EngineEvents produced by strategies before
        // advancing historical market time.
        wait_for_engine(
            tx_engine
        )
        .await?;
    }


    println!(
        "[SYNTHETIC BACKTEST] finished historical replay"
    );

    Ok(())
}


// ============================================================
// ENGINE BARRIER
// ============================================================

async fn wait_for_engine(
    tx_engine: &UnboundedSender<EngineEvent>,
) -> Result<(), String> {
    let (tx_done, rx_done) =
        oneshot::channel();

    tx_engine
        .send(
            EngineEvent::BacktestBarrier {
                done: tx_done,
            }
        )
        .map_err(
            |_| "engine event channel closed".to_string()
        )?;

    rx_done
        .await
        .map_err(
            |_| "engine barrier was dropped".to_string()
        )?;

    Ok(())
}


// ============================================================
// BACKTEST REPLAY EVENT
// ============================================================

enum BacktestReplayEvent {
    Trade(HistoricalTrade),
    Bar(Bar),
}


impl BacktestReplayEvent {
    fn timestamp(
        &self,
    ) -> DateTime<Utc> {
        match self {
            Self::Trade(trade) => trade.timestamp,
            Self::Bar(bar) => bar.end,
        }
    }


    fn replay_priority(
        &self,
    ) -> u8 {
        match self {
            Self::Bar(_) => 0,
            Self::Trade(_) => 1,
        }
    }
}