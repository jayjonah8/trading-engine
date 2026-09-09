// use std::time::Duration;

// use chrono::{
//     DateTime,
//     Utc,
// };

// use crate::{
//     market::{
//         candle::CandleConfig,
//         candle_manager::CandleManager,
//     },
// };

// use tokio::{
//     sync::mpsc::Sender,
//     time::sleep,
// };

// use crate::{
//     backtest::{
//         alpaca::{
//             downsample_trades,
//             fetch_historical_trades,
//             HistoricalFetchConfig,
//         },
//         types::HistoricalTrade,
//     },

//     core::events::{
//         AppEvent,
//         AppEventEnvelope,
//         Bar,
//         Trade,
//     },

// };

// use tokio::sync::{
//     mpsc::UnboundedSender,
//     oneshot,
// };

// use crate::backtest::strategy_event::BacktestStrategyEvent;

// use crate::core::{
//     engine::EngineEvent,
//     engine_shared::engine_event_mapper::map_app_to_backtest_engine_event,
// };

// // Fetches raw historical trades, builds accurate candles from the full
// // trade stream, downsamples the trades for strategy replay, then merges
// // trades and closed candles into one chronological backtest event stream.
// pub async fn run_backtest_feed(
//     tx_backtest_strategies:
//         &[Sender<BacktestStrategyEvent>],

//     tx_engine:
//         &UnboundedSender<EngineEvent>,
// ) -> Result<(), Box<dyn std::error::Error>> {
//     let fetch_config =
//         HistoricalFetchConfig::default();

//     // Uses the default candle setting timeframes and prune duration. 
//     // You can change this to a custom config if you want. 
//     let candle_config =
//         CandleConfig::default();

//     // ============================================================
//     // BACKTEST TIME RANGE
//     // ============================================================

//     // full day
//     let start =
//         "2026-08-03T13:30:00Z";

//     let end =
//         "2026-08-03T20:00:00Z";

//     // 2 hours
//     // let start =
//     // "2026-08-03T13:30:00Z";

//     // let end =
//     //     "2026-08-03T15:30:00Z";

//     // ============================================================
//     // FETCH RAW HISTORICAL TRADES
//     // ============================================================

//     // Keep the raw trades untouched here because candles need every
//     // trade to calculate accurate OHLCV and trade-count information.
//     let raw_trades =
//         fetch_historical_trades(
//             &["AAPL", "MSFT", "NVDA"],
//             start,
//             end,
//             &fetch_config,
//         )
//         .await?;

//     println!(
//         "[BACKTEST] fetched {} raw historical trades",
//         raw_trades.len()
//     );

//     // ============================================================
//     // BUILD MULTI-TIMEFRAME CANDLES FROM RAW TRADES
//     // ============================================================

//     // Candles are created BEFORE downsampling so their open, high,
//     // low, close, volume, and trade count use the full trade stream.
//     let mut candle_manager =
//         CandleManager::new(
//             &candle_config.timeframes
//         );

//     // Every raw trade updates all configured candle timeframes.
//     // This happens BEFORE trade downsampling so OHLCV remains accurate.
//     let mut bars = Vec::new();

//     for trade in &raw_trades {
//         bars.extend(
//             candle_manager.on_trade(
//                 &trade.symbol,
//                 trade.timestamp,
//                 trade.price,
//                 trade.size,
//             )
//         );
//     }


//     // ============================================================
//     // CLOSE FINAL COMPLETED CANDLES
//     // ============================================================

//     let end_time =
//         DateTime::parse_from_rfc3339(
//             end
//         )?
//         .with_timezone(&Utc);

//     bars.extend(
//         candle_manager
//             .close_through(
//                 end_time
//             )
//     );

//     // Print total candles built across all configured timeframes.
//     let timeframe_minutes: Vec<u64> =
//         candle_config
//             .timeframes
//             .iter()
//             .map(|timeframe| {
//                 timeframe.as_secs() / 60
//             })
//             .collect();

//     println!(
//         "[BACKTEST] built {} historical candles | timeframes={:?} minutes",
//         bars.len(),
//         timeframe_minutes,
//     );

//     // ============================================================
//     // DOWNSAMPLE STRATEGY TRADE STREAM
//     // ============================================================

//     // Only the trade stream sent to the strategy is downsampled.
//     // Candles were already built above using every raw trade.
//     let original_trade_count =
//         raw_trades.len();

//     let trades =
//         downsample_trades(
//             raw_trades,
//             fetch_config.min_trade_interval,
//         );

//     println!(
//         "[BACKTEST] downsampled | original={} | kept={} | removed={} | interval={}ms",
//         original_trade_count,
//         trades.len(),
//         original_trade_count - trades.len(),
//         fetch_config.min_trade_interval.as_millis(),
//     );

//     // ============================================================
//     // BUILD HISTORICAL REPLAY TIMELINE
//     // ============================================================

//     // Combine downsampled trades and completed candles into one
//     // event stream before sending anything to the strategy.
//     let mut replay_events =
//         Vec::with_capacity(
//             trades.len() + bars.len()
//         );

//     for trade in trades {
//         replay_events.push(
//             BacktestReplayEvent::Trade(
//                 trade
//             )
//         );
//     }

//     for bar in bars {
//         replay_events.push(
//             BacktestReplayEvent::Bar(
//                 bar
//             )
//         );
//     }

//     // ============================================================
//     // SORT REPLAY EVENTS
//     // ============================================================

//     // Sort everything by historical market time.
//     //
//     // If a candle close and trade have the exact same timestamp,
//     // process the closed candle first so the previous candle is
//     // available before processing the new interval's trade.
//     replay_events.sort_by(
//         |a, b| {
//             a.timestamp()
//                 .cmp(&b.timestamp())
//                 .then_with(|| {
//                     a.replay_priority()
//                         .cmp(
//                             &b.replay_priority()
//                         )
//                 })
//         }
//     );

//     println!(
//         "[BACKTEST] replaying {} historical market events",
//         replay_events.len()
//     );

//     println!(
//         "[BACKTEST] starting historical replay in 3 seconds..."
//     );

//     sleep(
//         Duration::from_secs(3)
//     )
//     .await;

//     // ============================================================
//     // REPLAY HISTORICAL MARKET EVENTS
//     // ============================================================

//     // Convert each internal replay event into the normal AppEvent
//     // format consumed by BaseBacktestStrategy.
//     for replay_event in replay_events {
//         let event =
//             match replay_event {
//                 BacktestReplayEvent::Trade(
//                     trade
//                 ) => {
//                     let symbol =
//                         trade.symbol.clone();

//                     AppEventEnvelope {
//                         symbol:
//                             symbol.clone(),

//                         event:
//                             AppEvent::Trade(
//                                 Trade {
//                                     symbol,

//                                     price:
//                                         trade.price,

//                                     size:
//                                         trade.size,

//                                     timestamp:
//                                         trade.timestamp
//                                             .to_rfc3339(),
//                                 }
//                             ),
//                     }
//                 }

//                 BacktestReplayEvent::Bar(
//                     bar
//                 ) => {
//                     AppEventEnvelope {
//                         symbol:
//                             bar.symbol.clone(),

//                         event:
//                             AppEvent::BarClosed(
//                                 bar
//                             ),
//                     }
//                 }
//             };

//         // ============================================================
//         // DEBUG: HISTORICAL EVENT START (DELETE LATER)
//         // ============================================================
//         // if let AppEvent::Trade(trade) = &event.event {
//         //     println!(
//         //         "\n[BACKTEST] EVENT | symbol={} | px={:.4} | time={}",
//         //         event.symbol,
//         //         trade.price,
//         //         trade.timestamp,
//         //     );
//         // }

//         // ============================================================
//         // 1. PROCESS HISTORICAL PRICE IN ENGINE
//         // ============================================================

//         // Trade events are converted into PriceTickBackTest events so
//         // existing positions can run deterministic risk checks before
//         // strategies process this same historical market event.
//         //
//         // BarClosed events do not produce a PriceTickBackTest.
//         if let Some(engine_event) =
//             map_app_to_backtest_engine_event(
//                 &event
//             )
//         {
//             tx_engine
//                 .send(
//                     engine_event
//                 )
//                 .map_err(
//                     |_| {
//                         "Engine event channel closed"
//                     }
//                 )?;

//             // Wait until the engine has completely processed the
//             // historical price before strategies see this event.
//             wait_for_engine(
//                 tx_engine
//             )
//             .await?;

//             // TEMP DEBUG: (DELETE LATER)
//             // println!(
//             //     "[BACKTEST] ENGINE PRICE DONE"
//             // );
//         }


//         // ============================================================
//         // 2. SEND EVENT TO ALL BACKTEST STRATEGIES
//         // ============================================================

//         // Every enabled strategy receives the same historical event.
//         // Each strategy must acknowledge that it has completely
//         // processed the event before replay can continue.
//         let mut acknowledgements =
//             Vec::with_capacity(
//                 tx_backtest_strategies.len()
//             );

//         for tx_strategy in
//             tx_backtest_strategies
//         {
//             let (tx_done, rx_done) =
//                 oneshot::channel();

//             tx_strategy
//                 .send(
//                     BacktestStrategyEvent {
//                         env: event.clone(),
//                         done: tx_done,
//                     }
//                 )
//                 .await
//                 .map_err(
//                     |_| {
//                         "Backtest strategy channel closed"
//                     }
//                 )?;

//             acknowledgements.push(
//                 rx_done
//             );
//         }


//         // ============================================================
//         // 3. WAIT FOR ALL BACKTEST STRATEGIES
//         // ============================================================

//         // Do not advance until every enabled strategy has completely
//         // processed this historical market event.
//         for done in acknowledgements {
//             done
//                 .await
//                 .map_err(
//                     |_| {
//                         "Backtest strategy acknowledgement dropped"
//                     }
//                 )?;
//         }

//         // TEMP DEBUG: (DELETE LATER)
//         // println!(
//         //     "[BACKTEST] STRATEGIES DONE"
//         // );


//         // ============================================================
//         // 4. WAIT FOR STRATEGY ENGINE EVENTS
//         // ============================================================

//         // Strategies may have generated OpenBackTestTrade events while
//         // processing this market event. This barrier guarantees those
//         // engine events are processed before replay advances.
//         wait_for_engine(
//             tx_engine
//         )
//         .await?;

//         // TEMP DEBUG: (DELETE LATER)
//         // println!(
//         //     "[BACKTEST] ENGINE DECISIONS DONE"
//         // );
//     }
    
//     println!(
//         "[BACKTEST] finished publishing historical events"
//     );

//     Ok(())
// }


// /// Waits until the engine has processed every EngineEvent that was
// /// queued before this synchronization point.
// async fn wait_for_engine(
//     tx_engine: &UnboundedSender<EngineEvent>,
// ) -> Result<(), String> {
//     let (tx_done, rx_done) =
//         oneshot::channel();

//     tx_engine
//         .send(
//             EngineEvent::BacktestBarrier {
//                 done: tx_done,
//             }
//         )
//         .map_err(
//             |_| "engine event channel closed".to_string()
//         )?;

//     rx_done
//         .await
//         .map_err(
//             |_| "engine barrier was dropped".to_string()
//         )?;

//     Ok(())
// }


// // ============================================================
// // BACKTEST REPLAY EVENT
// // ============================================================

// // Internal backtest-only event type used to combine historical
// // trades and completed candles into one chronological timeline.
// //
// // This type never leaves the backtest runner. Before events are
// // sent to the strategy they are converted into normal AppEvents.
// enum BacktestReplayEvent {
//     Trade(HistoricalTrade),
//     Bar(Bar),
// }

// impl BacktestReplayEvent {
//     // Returns the historical time at which this event becomes
//     // available to the strategy.
//     //
//     // Trades use their actual trade timestamp.
//     // Closed candles use the END of their candle interval.
//     fn timestamp(
//         &self,
//     ) -> DateTime<Utc> {
//         match self {
//             Self::Trade(trade) => {
//                 trade.timestamp
//             }

//             Self::Bar(bar) => {
//                 bar.end
//             }
//         }
//     }


//     // Gives events with identical timestamps a deterministic order.
//     //
//     // Closed candles are processed before trades that occur exactly
//     // at the beginning of the next candle interval.
//     fn replay_priority(
//         &self,
//     ) -> u8 {
//         match self {
//             Self::Bar(_) => 0,
//             Self::Trade(_) => 1,
//         }
//     }
// }

