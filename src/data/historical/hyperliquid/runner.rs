#![allow(dead_code)]

use chrono::{
    DateTime,
    Duration as ChronoDuration,
    Utc,
};

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
        downsample_stateful::StatefulTradeDownsampler,
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


// ============================================================
// HYPERLIQUID HISTORICAL BACKTEST RUNNER
// ============================================================

// Supports optional configurable day-based chunking.
//
// None:
//     Uses the existing full-range historical loading behavior.
//
// Some(days):
//     Loads historical data in chronological chunks while
//     preserving candle, downsampling, strategy, and engine state.
//
// Only historical input data is released between chunks.
//
// TODO:
// Extract common historical replay functionality into a
// provider-independent runner shared by future data providers.
pub async fn run_hyperliquid_backtest_feed(
    tx_backtest_strategies: &[Sender<BacktestStrategyEvent>],
    tx_engine: &UnboundedSender<EngineEvent>,
    tx_exec: &UnboundedSender<ExecCommand>,
    tx_exec_evt: &UnboundedSender<ExecEvent>,
    market_state: &SharedBacktestMarketState,
    execution_venue: Venue,
    symbols: &[String],
    start: &str,
    end: &str,
    min_trade_interval: Duration,
    chunk_days: Option<u32>,
) -> Result<(), Box<dyn std::error::Error>> {

    // ============================================================
    // VALIDATE CONFIGURATION
    // ============================================================

    if symbols.is_empty() {
        return Err(
            "Hyperliquid backtest symbols cannot be empty".into()
        );
    }

    if matches!(chunk_days, Some(0)) {
        return Err(
            "Backtest chunk days must be greater than zero".into()
        );
    }

    let start_time =
        DateTime::parse_from_rfc3339(start)?.with_timezone(&Utc);

    let end_time =
        DateTime::parse_from_rfc3339(end)?.with_timezone(&Utc);

    if start_time >= end_time {
        return Err(
            "Invalid Hyperliquid backtest date range".into()
        );
    }

    // ============================================================
    // INITIALIZE HISTORICAL COMPONENTS
    // ============================================================

    let candle_config = CandleConfig::default();

    let mut candle_manager =
        CandleManager::new(&candle_config.timeframes);

    let symbol_refs: Vec<&str> =
        symbols.iter().map(String::as_str).collect();

    let timeframe_minutes: Vec<u64> = candle_config
        .timeframes
        .iter()
        .map(|timeframe| timeframe.as_secs() / 60)
        .collect();

    println!(
        "[BACKTEST] Hyperliquid historical replay | start={} | end={} | symbols={} | chunk_days={:?}",
        start_time,
        end_time,
        symbols.len(),
        chunk_days,
    );

    println!(
        "[BACKTEST] candle timeframes={:?} minutes",
        timeframe_minutes,
    );

    // ============================================================
    // INITIALIZE REPLAY
    // ============================================================

    println!(
        "[BACKTEST] starting Hyperliquid historical replay in 3 seconds..."
    );

    sleep(Duration::from_secs(3)).await;

    // ============================================================
    // UNCHUNKED BACKTEST
    // ============================================================

    // Preserve the existing full-range loading and downsampling
    // behavior when chunking is disabled.
    if chunk_days.is_none() {

        let raw_trades =
            fetch_hyperliquid_historical_trades(
                &symbol_refs,
                start,
                end,
            )?;

        let original_trade_count = raw_trades.len();

        println!(
            "[BACKTEST] fetched {} raw Hyperliquid historical trades",
            original_trade_count,
        );

        // ========================================================
        // BUILD CANDLES
        // ========================================================

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
            candle_manager.close_through(end_time)
        );

        println!(
            "[BACKTEST] built {} historical candles",
            bars.len(),
        );

        // ========================================================
        // DOWNSAMPLE TRADES
        // ========================================================

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

        // ========================================================
        // BUILD REPLAY EVENTS
        // ========================================================

        let mut replay_events =
            build_replay_events(trades, bars);

        sort_replay_events(&mut replay_events);

        println!(
            "[BACKTEST] replaying {} Hyperliquid historical market events",
            replay_events.len(),
        );

        // ========================================================
        // REPLAY
        // ========================================================

        replay_historical_events(
            replay_events,
            tx_backtest_strategies,
            tx_engine,
            tx_exec,
            tx_exec_evt,
            market_state,
            execution_venue,
        )
        .await?;

    } else {

        // ========================================================
        // CHUNKED BACKTEST
        // ========================================================

        let days = chunk_days.ok_or(
            "Missing Hyperliquid chunk configuration"
        )?;

        let chunk_duration =
            ChronoDuration::days(i64::from(days));

        // Persistent downsampling state across every chunk.
        let mut downsampler =
            StatefulTradeDownsampler::new(min_trade_interval);

        // Events not yet safe to replay remain buffered.
        let mut pending_events: Vec<BacktestReplayEvent> =
            Vec::new();

        let mut chunk_start = start_time;

        let mut chunk_number = 0usize;

        let mut total_raw_trades = 0usize;
        let mut total_replayed_events = 0usize;

        // ========================================================
        // PROCESS HISTORICAL CHUNKS
        // ========================================================

        while chunk_start < end_time {

            chunk_number += 1;

            let chunk_end =
                (chunk_start + chunk_duration).min(end_time);

            println!(
                "\n[BACKTEST CHUNK {}] loading | start={} | end={}",
                chunk_number,
                chunk_start,
                chunk_end,
            );

            // ====================================================
            // FETCH CHUNK
            // ====================================================

            let raw_trades =
                fetch_hyperliquid_historical_trades(
                    &symbol_refs,
                    &chunk_start.to_rfc3339(),
                    &chunk_end.to_rfc3339(),
                )?;

            let raw_trade_count = raw_trades.len();

            total_raw_trades += raw_trade_count;

            println!(
                "[BACKTEST CHUNK {}] fetched {} raw trades",
                chunk_number,
                raw_trade_count,
            );

            // ====================================================
            // BUILD CANDLES FROM FULL RAW STREAM
            // ====================================================

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

            // Close only candles that have actually completed.
            //
            // CandleManager preserves any active candles whose
            // end timestamps are beyond this chunk boundary.
            //
            // This does not reset candle state.
            bars.extend(
                candle_manager.close_through(chunk_end)
            );

            // ====================================================
            // STATEFUL DOWNSAMPLING
            // ====================================================

            let mut trades =
                downsampler.process_chunk(raw_trades);

            // Release completed buckets at the chunk boundary.
            //
            // This prevents inactive symbols from retaining
            // historical trades indefinitely.
            trades.extend(
                downsampler.flush_completed_through(chunk_end)
            );

            // ====================================================
            // BUILD CHUNK REPLAY EVENTS
            // ====================================================

            pending_events.extend(
                build_replay_events(trades, bars)
            );

            // ====================================================
            // DETERMINE SAFE REPLAY BOUNDARY
            // ====================================================

            // For zero downsampling interval, every raw trade
            // is immediately available.
            //
            // Otherwise, retain events from the time bucket
            // overlapping the chunk boundary.
            //
            // This guarantees that a later chunk cannot
            // introduce an older downsampled trade.
            let safe_replay_end =
                safe_replay_boundary(
                    chunk_end,
                    min_trade_interval,
                )?;

            // ====================================================
            // SEPARATE READY AND PENDING EVENTS
            // ====================================================

            let mut ready_events = Vec::new();
            let mut remaining_events = Vec::new();

            for event in pending_events.drain(..) {
                if event.timestamp() < safe_replay_end {
                    ready_events.push(event);
                } else {
                    remaining_events.push(event);
                }
            }

            pending_events = remaining_events;

            // ====================================================
            // SORT READY EVENTS
            // ====================================================

            sort_replay_events(&mut ready_events);

            let ready_count = ready_events.len();

            println!(
                "[BACKTEST CHUNK {}] replaying {} events | pending={}",
                chunk_number,
                ready_count,
                pending_events.len(),
            );

            // ====================================================
            // REPLAY CHUNK
            // ====================================================

            replay_historical_events(
                ready_events,
                tx_backtest_strategies,
                tx_engine,
                tx_exec,
                tx_exec_evt,
                market_state,
                execution_venue,
            )
            .await?;

            total_replayed_events += ready_count;

            println!(
                "[BACKTEST CHUNK {}] complete | raw_trades={} | replayed={}",
                chunk_number,
                raw_trade_count,
                ready_count,
            );

            // ====================================================
            // ADVANCE CHUNK
            // ====================================================

            chunk_start = chunk_end;

            // The raw trade vector has already been consumed.
            // The completed replay events have also been consumed.
            //
            // Only persistent candle, downsampling, and pending
            // replay state remains in memory.
        }

        // ========================================================
        // FINALIZE HISTORICAL DATA
        // ========================================================

        // Release the final retained trade for each symbol.
        let remaining_trades =
            downsampler.finish();

        // Finalize any remaining completed candles.
        let remaining_bars =
            candle_manager.close_through(end_time);

        pending_events.extend(
            build_replay_events(
                remaining_trades,
                remaining_bars,
            )
        );

        sort_replay_events(&mut pending_events);

        let final_event_count = pending_events.len();

        println!(
            "[BACKTEST] replaying {} final buffered events",
            final_event_count,
        );

        replay_historical_events(
            pending_events,
            tx_backtest_strategies,
            tx_engine,
            tx_exec,
            tx_exec_evt,
            market_state,
            execution_venue,
        )
        .await?;

        total_replayed_events += final_event_count;

        println!(
            "[BACKTEST] chunked replay complete | chunks={} | raw_trades={} | replayed_events={}",
            chunk_number,
            total_raw_trades,
            total_replayed_events,
        );
    }

    println!(
        "[BACKTEST] finished publishing Hyperliquid historical events"
    );

    Ok(())
}


// ============================================================
// SAFE REPLAY BOUNDARY
// ============================================================

// Determines the earliest possible timestamp belonging to an
// unfinished downsampling bucket.
//
// Events strictly before this timestamp are safe to replay.
//
// This keeps chronological ordering intact across chunks.
fn safe_replay_boundary(
    timestamp: DateTime<Utc>,
    interval: Duration,
) -> Result<DateTime<Utc>, Box<dyn std::error::Error>> {

    if interval.is_zero() {
        return Ok(timestamp);
    }

    let timestamp_ns =
        (timestamp.timestamp() as i128 * 1_000_000_000)
            + timestamp.timestamp_subsec_nanos() as i128;

    let interval_ns = interval.as_nanos() as i128;

    let bucket_start_ns =
        timestamp_ns.div_euclid(interval_ns) * interval_ns;

    let seconds =
        bucket_start_ns.div_euclid(1_000_000_000);

    let nanos =
        bucket_start_ns.rem_euclid(1_000_000_000);

    let seconds = i64::try_from(seconds)?;

    DateTime::<Utc>::from_timestamp(
        seconds,
        nanos as u32,
    )
    .ok_or_else(|| {
        "Failed to calculate historical replay boundary".into()
    })
}


// ============================================================
// BUILD HISTORICAL REPLAY EVENTS
// ============================================================

fn build_replay_events(
    trades: Vec<HistoricalTrade>,
    bars: Vec<Bar>,
) -> Vec<BacktestReplayEvent> {

    let mut events =
        Vec::with_capacity(trades.len() + bars.len());

    for trade in trades {
        events.push(
            BacktestReplayEvent::Trade(trade)
        );
    }

    for bar in bars {
        events.push(
            BacktestReplayEvent::Bar(bar)
        );
    }

    events
}


// ============================================================
// SORT HISTORICAL REPLAY EVENTS
// ============================================================

fn sort_replay_events(
    events: &mut [BacktestReplayEvent],
) {
    // Closed candles are processed before trades when both
    // have the same historical timestamp.
    events.sort_by(|a, b| {
        a.timestamp()
            .cmp(&b.timestamp())
            .then_with(|| {
                a.replay_priority()
                    .cmp(&b.replay_priority())
            })
    });
}


// ============================================================
// REPLAY HISTORICAL MARKET EVENTS
// ============================================================

// Preserves the existing historical event-processing sequence.
//
// Every event must complete all Engine, strategy, and execution
// synchronization barriers before the next event is processed.
async fn replay_historical_events(
    replay_events: Vec<BacktestReplayEvent>,
    tx_backtest_strategies: &[Sender<BacktestStrategyEvent>],
    tx_engine: &UnboundedSender<EngineEvent>,
    tx_exec: &UnboundedSender<ExecCommand>,
    tx_exec_evt: &UnboundedSender<ExecEvent>,
    market_state: &SharedBacktestMarketState,
    execution_venue: Venue,
) -> Result<(), Box<dyn std::error::Error>> {

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
                            timestamp_ms:
                                trade.timestamp.timestamp_millis(),
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
        // 1. UPDATE BACKTEST EXECUTION MARKET STATE
        // ========================================================

        if let AppEvent::TapeTrade(trade) = &event.event {

            let mut state = market_state.write().await;

            state.update_price(
                trade.symbol.clone(),
                trade.price,
                trade.timestamp_ms,
            );
        }


        // ========================================================
        // 2. PROCESS HISTORICAL PRICE IN ENGINE
        // ========================================================

        if let Some(engine_event) =
            map_app_to_backtest_engine_event(&event)
        {
            tx_engine
                .send(engine_event)
                .map_err(|_| {
                    "Engine event channel closed"
                })?;

            wait_for_engine(tx_engine).await?;
        }


        // ========================================================
        // 3. SEND EVENT TO ALL BACKTEST STRATEGIES
        // ========================================================

        let mut acknowledgements =
            Vec::with_capacity(tx_backtest_strategies.len());

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
                .map_err(|_| {
                    "Backtest strategy channel closed"
                })?;

            acknowledgements.push(rx_done);
        }


        // ========================================================
        // 4. WAIT FOR ALL BACKTEST STRATEGIES
        // ========================================================

        for done in acknowledgements {
            done
                .await
                .map_err(|_| {
                    "Backtest strategy acknowledgement dropped"
                })?;
        }


        // ========================================================
        // 5. WAIT FOR STRATEGY ENGINE EVENTS
        // ========================================================

        wait_for_engine(tx_engine).await?;


        // ========================================================
        // 6. WAIT FOR BACKTEST EXECUTION COMMANDS
        // ========================================================

        wait_for_execution_commands(
            tx_exec,
            execution_venue,
        )
        .await?;


        // ========================================================
        // 7. WAIT FOR BACKTEST EXECUTION EVENTS
        // ========================================================

        wait_for_execution(tx_exec_evt).await?;


        // ========================================================
        // DEBUG: HISTORICAL REPLAY SPEED
        // ========================================================

        const DEBUG_REPLAY_DELAY_MS: u64 = 0;

        if DEBUG_REPLAY_DELAY_MS > 0 {

            println!(
                "[BACKTEST DEBUG] HISTORICAL STEP COMPLETE | symbol={}",
                event.symbol,
            );

            sleep(
                Duration::from_millis(DEBUG_REPLAY_DELAY_MS)
            )
            .await;
        }
    }

    Ok(())
}


// ============================================================
// ENGINE SYNCHRONIZATION
// ============================================================

/// Waits until the engine has processed every EngineEvent
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
        .map_err(|_| {
            "engine event channel closed".to_string()
        })?;

    rx_done
        .await
        .map_err(|_| {
            "engine barrier was dropped".to_string()
        })?;

    Ok(())
}


// ============================================================
// EXECUTION COMMAND SYNCHRONIZATION
// ============================================================

/// Waits until the backtest execution adapter has processed
/// every execution command queued before this point.
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
        .map_err(|_| {
            "execution command channel closed".to_string()
        })?;

    rx_done
        .await
        .map_err(|_| {
            "execution command barrier was dropped".to_string()
        })?;

    Ok(())
}


// ============================================================
// EXECUTION EVENT SYNCHRONIZATION
// ============================================================

/// Waits until the engine has processed every ExecEvent
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
        .map_err(|_| {
            "execution event channel closed".to_string()
        })?;

    rx_done
        .await
        .map_err(|_| {
            "execution barrier was dropped".to_string()
        })?;

    Ok(())
}


// ============================================================
// BACKTEST REPLAY EVENT
// ============================================================

// Internal backtest-only event used to combine historical trades
// and completed candles into one chronological replay timeline.
enum BacktestReplayEvent {
    Trade(HistoricalTrade),
    Bar(Bar),
}

impl BacktestReplayEvent {

    fn timestamp(&self) -> DateTime<Utc> {
        match self {
            Self::Trade(trade) => trade.timestamp,
            Self::Bar(bar) => bar.end,
        }
    }

    // Closed candles take priority over trades when timestamps match.
    fn replay_priority(&self) -> u8 {
        match self {
            Self::Bar(_) => 0,
            Self::Trade(_) => 1,
        }
    }
}