#![allow(dead_code)]

use chrono::{DateTime, Utc};
use std::time::Duration;

use tokio::{
    sync::{
        mpsc::{Sender, UnboundedSender},
        oneshot,
    },
    time::sleep,
};

use crate::{
    core::{
        engine::engine::EngineEvent,
        events::{AppEvent, AppEventEnvelope, Bar, TapeTrade},
        execution::{
            ExecCommand,
            ExecEvent,
            Venue,
        },
    },

    exchange::backtest::SharedBacktestMarketState,

    data::historical::{
        downsample::downsample_trades,
        event_mapper::map_app_to_backtest_engine_event,
        hyperliquid::fetch::fetch_hyperliquid_historical_trades,
        strategy_event::BacktestStrategyEvent,
        types::HistoricalTrade,
    },

    market::{
        candle::CandleConfig,
        candle_manager::CandleManager,
    },
};


// Fetches raw Hyperliquid historical trades from the local archive, builds
// accurate candles from the full trade stream, downsamples the trades for
// strategy replay, then merges trades and closed candles into one
// chronological backtest event stream.
//
// TODO:
// The Alpaca and Hyperliquid backtest runners intentionally remain separate
// for now. Later, extract their common candle/replay pipeline into a shared
// provider-independent historical backtest runner.
pub async fn run_hyperliquid_backtest_feed(
    tx_backtest_strategies: &[Sender<BacktestStrategyEvent>],
    tx_engine: &UnboundedSender<EngineEvent>,
    tx_exec: &UnboundedSender<ExecCommand>,
    tx_exec_evt: &UnboundedSender<ExecEvent>,
    market_state: &SharedBacktestMarketState,
    execution_venue: Venue,
) -> Result<(), Box<dyn std::error::Error>> {
    // TODO:
    // Move backtest replay settings into the shared Config so every historical
    // data provider uses the same configurable trade downsampling interval.
    let min_trade_interval = Duration::from_millis(300);

    // Uses the default candle setting timeframes and prune duration.
    // You can change this to a custom config if you want.
    let candle_config = CandleConfig::default();

    // ============================================================
    // BACKTEST TIME RANGE
    // ============================================================

    // let symbols = ["BTC", "ETH", "SOL", "HYPE", "NEAR"];
    // let start = "2025-07-27T10:00:00Z";
    // let end = "2025-07-27T11:00:00Z";

    let symbols = ["BTC", "ETH", "SOL", "HYPE", "NEAR"];
    let start = "2026-10-15T10:00:00Z";
    let end = "2026-10-15T11:00:00Z";

    // ============================================================
    // FETCH RAW HISTORICAL TRADES
    // ============================================================

    // Keep the raw trades untouched here because candles need every
    // trade to calculate accurate OHLCV and trade-count information.
    let raw_trades = fetch_hyperliquid_historical_trades(
        &symbols,
        start,
        end,
    )?;

    println!(
        "[BACKTEST] fetched {} raw Hyperliquid historical trades",
        raw_trades.len()
    );

    // ============================================================
    // BUILD MULTI-TIMEFRAME CANDLES FROM RAW TRADES
    // ============================================================

    // Candles are created BEFORE downsampling so their open, high,
    // low, close, volume, and trade count use the full trade stream.
    let mut candle_manager = CandleManager::new(&candle_config.timeframes);
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

    // ============================================================
    // CLOSE FINAL COMPLETED CANDLES
    // ============================================================

    let end_time = DateTime::parse_from_rfc3339(end)?.with_timezone(&Utc);

    bars.extend(
        candle_manager.close_through(end_time)
    );

    let timeframe_minutes: Vec<u64> = candle_config
        .timeframes
        .iter()
        .map(|timeframe| timeframe.as_secs() / 60)
        .collect();

    println!(
        "[BACKTEST] built {} historical candles | timeframes={:?} minutes",
        bars.len(),
        timeframe_minutes,
    );

    // ============================================================
    // DOWNSAMPLE STRATEGY TRADE STREAM
    // ============================================================

    // Only the trade stream sent to the strategy is downsampled.
    // Candles were already built above using every raw trade.
    let original_trade_count = raw_trades.len();

    let trades = downsample_trades(
        raw_trades,
        min_trade_interval,
    );

    println!(
        "[BACKTEST] downsampled | original={} | kept={} | removed={} | interval={}ms",
        original_trade_count,
        trades.len(),
        original_trade_count - trades.len(),
        min_trade_interval.as_millis(),
    );

    // ============================================================
    // BUILD HISTORICAL REPLAY TIMELINE
    // ============================================================

    let mut replay_events = Vec::with_capacity(trades.len() + bars.len());

    for trade in trades {
        replay_events.push(BacktestReplayEvent::Trade(trade));
    }

    for bar in bars {
        replay_events.push(BacktestReplayEvent::Bar(bar));
    }

    // ============================================================
    // SORT REPLAY EVENTS
    // ============================================================

    // If a candle close and trade have the exact same timestamp,
    // process the closed candle first so the previous candle is
    // available before processing the new interval's trade.
    replay_events.sort_by(|a, b| {
        a.timestamp()
            .cmp(&b.timestamp())
            .then_with(|| a.replay_priority().cmp(&b.replay_priority()))
    });

    println!(
        "[BACKTEST] replaying {} Hyperliquid historical market events",
        replay_events.len()
    );

    println!(
        "[BACKTEST] starting Hyperliquid historical replay in 3 seconds..."
    );

    sleep(Duration::from_secs(3)).await;

    // ============================================================
    // REPLAY HISTORICAL MARKET EVENTS
    // ============================================================

    for replay_event in replay_events {
        let event = match replay_event {
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


        // ============================================================
        // 1. UPDATE BACKTEST EXECUTION MARKET STATE
        // ============================================================

        // The execution simulator must observe the current historical market
        // state before the Engine or strategies process this event.
        //
        // If a strategy generates an order from this event, the backtest
        // execution adapter can therefore execute against the same historical
        // market price and timestamp that caused the strategy decision.
        if let AppEvent::TapeTrade(trade) =
            &event.event
        {
            let mut state =
                market_state.write().await;

            state.update_price(
                trade.symbol.clone(),
                trade.price,
                trade.timestamp_ms,
            );
        }


        // ============================================================
        // 2. PROCESS HISTORICAL PRICE IN ENGINE
        // ============================================================

        // Historical trades update engine prices and run deterministic
        // risk checks before strategies process the same market event.
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
                    |_| {
                        "Engine event channel closed"
                    }
                )?;

            wait_for_engine(
                tx_engine
            )
            .await?;
        }


        // ============================================================
        // 3. SEND EVENT TO ALL BACKTEST STRATEGIES
        // ============================================================

        let mut acknowledgements =
            Vec::with_capacity(
                tx_backtest_strategies.len()
            );

        for tx_strategy in
            tx_backtest_strategies
        {
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
                    |_| {
                        "Backtest strategy channel closed"
                    }
                )?;

            acknowledgements.push(
                rx_done
            );
        }


        // ============================================================
        // 4. WAIT FOR ALL BACKTEST STRATEGIES
        // ============================================================

        // Do not advance until every enabled strategy has completely
        // processed this historical market event.
        for done in acknowledgements {
            done
                .await
                .map_err(
                    |_| {
                        "Backtest strategy acknowledgement dropped"
                    }
                )?;
        }


        // ============================================================
        // 5. WAIT FOR STRATEGY ENGINE EVENTS
        // ============================================================

        // Strategies may have generated EngineEvents while processing this
        // historical market event.
        //
        // Once this barrier completes, those events have been processed and
        // any resulting ExecCommands have been sent into execution.
        wait_for_engine(
            tx_engine
        )
        .await?;


        // ============================================================
        // 6. WAIT FOR BACKTEST EXECUTION COMMANDS
        // ============================================================

        // Wait until the execution router has forwarded all previously
        // queued commands and the backtest adapter has processed them.
        //
        // The adapter acknowledges this barrier after emitting the
        // corresponding ExecEvents, including order fills.
        wait_for_execution_commands(
            tx_exec,
            execution_venue,
        )
        .await?;


        // ============================================================
        // 7. WAIT FOR BACKTEST EXECUTION EVENTS
        // ============================================================

        // The execution-command barrier guarantees the backtest adapter
        // has emitted all ExecEvents for preceding execution commands.
        //
        // This barrier then guarantees the Engine has processed those
        // execution events before historical replay advances.
        wait_for_execution(
            tx_exec_evt
        )
        .await?;


        // ============================================================
        // DEBUG: HISTORICAL REPLAY SPEED (DELETE LATER)
        // ============================================================
        // Slow down historical replay to inspect the execution sequence.
        // This only affects wall-clock speed, not historical timestamps.
        // 100 ms is a good value
        const DEBUG_REPLAY_DELAY_MS: u64 = 0;


        // ============================================================
        // DEBUG: HISTORICAL STEP COMPLETE (DELETE LATER)
        // ============================================================
        if DEBUG_REPLAY_DELAY_MS > 0 {
            println!(
                "[BACKTEST DEBUG] HISTORICAL STEP COMPLETE | symbol={}",
                event.symbol,
            );
        }
        if DEBUG_REPLAY_DELAY_MS > 0 {
            sleep(
                Duration::from_millis(DEBUG_REPLAY_DELAY_MS)
            )
            .await;
        }
    }

    println!(
        "[BACKTEST] finished publishing Hyperliquid historical events"
    );

    Ok(())
}


// ============================================================
// ENGINE SYNCHRONIZATION
// ============================================================

/// Waits until the engine has processed every EngineEvent that was
/// queued before this synchronization point.
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
            |_| {
                "engine event channel closed".to_string()
            }
        )?;

    rx_done
        .await
        .map_err(
            |_| {
                "engine barrier was dropped".to_string()
            }
        )?;

    Ok(())
}


// ============================================================
// EXECUTION COMMAND SYNCHRONIZATION
// ============================================================

/// Waits until the backtest execution adapter has processed every
/// execution command queued before this synchronization point.
///
/// The venue identifies the backtest adapter that should receive
/// the barrier, allowing the same helper to support all venues.
async fn wait_for_execution_commands(
    tx_exec: &UnboundedSender<ExecCommand>,
    venue: Venue,
) -> Result<(), String> {
    let (tx_done, rx_done) =
        oneshot::channel();

    tx_exec
        .send(
            ExecCommand::BacktestBarrier {
                venue,
                done: tx_done,
            }
        )
        .map_err(
            |_| {
                "execution command channel closed".to_string()
            }
        )?;

    rx_done
        .await
        .map_err(
            |_| {
                "execution command barrier was dropped".to_string()
            }
        )?;

    Ok(())
}


// ============================================================
// EXECUTION EVENT SYNCHRONIZATION
// ============================================================

/// Waits until the engine has processed every ExecEvent that was
/// queued before this synchronization point.
async fn wait_for_execution(
    tx_exec_evt: &UnboundedSender<ExecEvent>,
) -> Result<(), String> {
    let (tx_done, rx_done) =
        oneshot::channel();

    tx_exec_evt
        .send(
            ExecEvent::BacktestBarrier {
                done: tx_done,
            }
        )
        .map_err(
            |_| {
                "execution event channel closed".to_string()
            }
        )?;

    rx_done
        .await
        .map_err(
            |_| {
                "execution barrier was dropped".to_string()
            }
        )?;

    Ok(())
}


// ============================================================
// BACKTEST REPLAY EVENT
// ============================================================

// Internal backtest-only event used to combine historical trades and
// completed candles into one chronological Hyperliquid replay timeline.
enum BacktestReplayEvent {
    Trade(HistoricalTrade),
    Bar(Bar),
}

impl BacktestReplayEvent {
    fn timestamp(
        &self,
    ) -> DateTime<Utc> {
        match self {
            Self::Trade(trade) => {
                trade.timestamp
            }

            Self::Bar(bar) => {
                bar.end
            }
        }
    }

    // Closed candles are processed before trades when both become
    // available at exactly the same historical timestamp.
    fn replay_priority(
        &self,
    ) -> u8 {
        match self {
            Self::Bar(_) => 0,
            Self::Trade(_) => 1,
        }
    }
}