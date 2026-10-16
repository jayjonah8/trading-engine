#![allow(dead_code)]

use chrono::{DateTime, Utc};

use crate::{
    core::{
        engine::{
            engine::{
                Engine,
                EngineEvent,
            },
            state::{
                LastPrice, 
                ExecutionMode
            },
        },

        execution::ExitOrderRequest,

        trades::{
            Direction,
            TradeId,
            TradeState,
        },
    },
    shared::print_log,
};


// ============================================================
// PRICE TICK / RISK FLOW
// ============================================================

// PriceTick
//     ↓
// update latest canonical market price
//     ↓
// inspect Open Trades for this symbol
//     ↓
// calculate unrealized PnL percentage
//     ↓
// calculate trade age from EVENT TIME
//     ↓
// evaluate:
//
// - Take Profit
// - Emergency Stop Loss
// - Normal Stop Loss + delay
// - Maximum Trade Duration
//
//     ↓
// collect TradeId + close reason
//     ↓
// emit EngineEvent::CloseTrade
//     ↓
// handle_close_trade()
//     ↓
// canonical Exit Order
//     ↓
// execution adapter / simulator
//
// IMPORTANT:
//
// This handler makes RISK DECISIONS only.
//
// It does NOT:
//
// - directly mutate Trade -> Closing
// - create Exit Orders itself
// - modify Position state
// - create or release RiskReservations
// - simulate fills locally
//
// All close execution goes through the same canonical CloseTrade lifecycle.
//
// This is important because the same risk logic can then be reused for:
//
// - live trading
// - simulation
// - historical backtesting
//
// without maintaining separate trade-closing implementations.


// ============================================================
// HANDLE PRICE TICK
// ============================================================

impl Engine {
    /// Processes one canonical market-price update.
    ///
    /// The latest price is stored in `last_px`, then all fully Open trades for
    /// the symbol are evaluated against their strategy-defined exit rules.
    ///
    /// Risk decisions use `ts_ms` event time rather than wall-clock time.
    /// This keeps behavior deterministic across live trading, simulation, and
    /// historical replay.
    ///
    /// When an exit condition is triggered, this handler emits a canonical
    /// `EngineEvent::CloseTrade`. It does not directly create or execute the
    /// exit order.
    pub fn handle_price_tick(
        &mut self,
        symbol: String,
        px: f64,
        ts_ms: i64,
    ) {
        // ============================================================
        // VALIDATE PRICE
        // ============================================================

        if !px.is_finite()
            || px <= 0.0
        {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE RISK] INVALID PRICE TICK | symbol={} | px={} | ts_ms={}",
                    symbol,
                    px,
                    ts_ms,
                ),
            );

            return;
        }


        // ============================================================
        // INITIALIZE BACKTEST START DATE
        // ============================================================

        if self.execution_mode == ExecutionMode::Backtest
            && self.general.starting_date.timestamp_millis() == 0
        {
            self.general.starting_date =
                DateTime::<Utc>::from_timestamp_millis(ts_ms)
                    .expect("Invalid backtest timestamp");
        }


        // ============================================================
        // UPDATE LATEST PRICE
        // ============================================================

        self.last_px.insert(
            symbol.clone(),
            LastPrice {
                px,
                ts_ms,
            },
        );

        
        // ========================================================
        // PERIODIC EQUITY SAMPLING
        // ========================================================

        // Record equity metrics for the current execution mode.
        self.sample_equity(ts_ms);


        // ============================================================
        // COLLECT CLOSE REQUESTS
        // ============================================================

        // Collect requests first rather than sending CloseTrade while
        // iterating over Trades.
        //
        // The resulting CloseTrade event will later mutate Trade / Order /
        // Risk state, so keeping this pass read-only avoids borrow conflicts
        // and keeps the risk evaluation deterministic.
        let mut trades_to_close:
            Vec<(TradeId, String)> =
            Vec::new();


        // ============================================================
        // CHECK OPEN TRADES
        // ============================================================

        for (trade_id, trade) in
            self.state.trades.iter()
        {
            // This price tick only applies to this symbol.
            if trade.symbol != symbol {
                continue;
            }


            // Only fully Open trades participate in normal TP / SL / duration
            // risk management.
            //
            // Opening:
            //     entry execution is still being established
            //
            // Closing:
            //     an exit lifecycle is already underway
            //
            // Closed:
            //     no remaining exposure
            if trade.state != TradeState::Open {
                continue;
            }


            // ========================================================
            // ENTRY PRICE
            // ========================================================

            let Some(entry_px) =
                trade.avg_entry_px
            else {
                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[ENGINE RISK] OPEN TRADE MISSING ENTRY PRICE | trade_id={} | symbol={}",
                        trade.id,
                        trade.symbol,
                    ),
                );

                // TODO:
                // Send a remote Slack/Discord critical error notification in
                // live trading. An Open Trade without an execution entry
                // price indicates inconsistent engine state.

                continue;
            };


            if !entry_px.is_finite()
                || entry_px <= 0.0
            {
                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[ENGINE RISK] INVALID ENTRY PRICE | trade_id={} | symbol={} | entry_px={}",
                        trade.id,
                        trade.symbol,
                        entry_px,
                    ),
                );

                continue;
            }


            // ========================================================
            // PNL PERCENT
            // ========================================================

            // Positive PnL percentage means the Trade is profitable.
            // Negative PnL percentage means the Trade is losing.
            //
            // Long:
            //
            // entry   = 100
            // current = 102
            // pnl     = +2%
            //
            // Short:
            //
            // entry   = 100
            // current = 98
            // pnl     = +2%
            let pnl_percent =
                match trade.direction {
                    Direction::Long => {
                        (
                            (px - entry_px)
                            / entry_px
                        ) * 100.0
                    }

                    Direction::Short => {
                        (
                            (entry_px - px)
                            / entry_px
                        ) * 100.0
                    }
                };


            // ========================================================
            // UNREALIZED PNL
            // ========================================================

            // This is currently calculated for risk/debug visibility only.
            //
            // Position-level unrealized PnL can later become part of a
            // dedicated valuation/accounting layer.
            let unrealized_pnl =
                match trade.direction {
                    Direction::Long => {
                        (px - entry_px)
                            * trade.quantity
                    }

                    Direction::Short => {
                        (entry_px - px)
                            * trade.quantity
                    }
                };


            // ========================================================
            // TRADE AGE
            // ========================================================

            // Use historical/event time instead of Instant::now().
            //
            // This makes exactly the same SL-delay and maximum-duration logic
            // usable during live trading, simulation, and backtesting.
            let age_secs =
                trade.opened_ts_ms
                    .map(|opened_ts_ms| {
                        ts_ms
                            .saturating_sub(opened_ts_ms)
                            .max(0)
                            / 1000
                    })
                    .unwrap_or(0) as u64;


            print_log(
                self.debug_mode,
                false,
                format!(
                    "[ENGINE RISK] | trade_id={} | symbol={} | px={:.4} | avg_entry_px={:.4} | pnl=${:.4} | pnl_pct={:.4}% | age={}s | ts_ms={}",
                    trade.id,
                    trade.symbol,
                    px,
                    entry_px,
                    unrealized_pnl,
                    pnl_percent,
                    age_secs,
                    ts_ms,
                ),
            );


            // ========================================================
            // TAKE PROFIT
            // ========================================================

            if let Some(take_profit_percent) =
                trade.take_profit_percent
            {
                if pnl_percent >= take_profit_percent {
                    trades_to_close.push(
                        (
                            *trade_id,
                            "TAKE_PROFIT".to_string(),
                        )
                    );

                    continue;
                }
            }


            // ========================================================
            // EMERGENCY STOP LOSS
            // ========================================================

            // Emergency stop intentionally does NOT wait for the normal
            // stop-loss delay.
            if let Some(emergency_stop_percent) =
                trade.emergency_stop_percent
            {
                if pnl_percent <= -emergency_stop_percent {
                    trades_to_close.push(
                        (
                            *trade_id,
                            "EMERGENCY_STOP_LOSS".to_string(),
                        )
                    );

                    continue;
                }
            }


            // ========================================================
            // NORMAL STOP LOSS
            // ========================================================

            if let Some(stop_loss_percent) =
                trade.stop_loss_percent
            {
                if pnl_percent <= -stop_loss_percent {
                    let stop_delay_secs =
                        trade.stop_loss_delay_secs
                            .unwrap_or(0);

                    if age_secs >= stop_delay_secs {
                        trades_to_close.push(
                            (
                                *trade_id,
                                "STOP_LOSS".to_string(),
                            )
                        );

                        continue;
                    }
                }
            }


            // ========================================================
            // MAXIMUM TRADE DURATION
            // ========================================================

            if let Some(max_duration_secs) =
                trade.max_duration_secs
            {
                if age_secs >= max_duration_secs {
                    trades_to_close.push(
                        (
                            *trade_id,
                            "TRADE_DURATION".to_string(),
                        )
                    );

                    continue;
                }
            }
        }


        // ============================================================
        // EMIT CLOSE REQUESTS
        // ============================================================

        // Risk detection and execution remain intentionally separated.
        //
        // This handler detects WHY a Trade should close.
        //
        // handle_close_trade() determines HOW the close is represented as a
        // canonical Exit Order and sends it through the execution architecture.
        for (trade_id, reason) in
            trades_to_close
        {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE RISK] CLOSE TRIGGERED | trade_id={} | symbol={} | px={} | reason={}",
                    trade_id,
                    symbol,
                    px,
                    reason,
                ),
            );


            // Keep the risk decision separate from the execution request.
            //
            // Do not call `handle_close_trade()` directly here. Instead, emit a canonical
            // `CloseTrade` event back through the engine channel so the request is processed
            // in order by the authoritative engine event loop.
            //
            // Engine-managed risk exits intentionally use a market execution request.
            // Once TP / SL / emergency-stop / duration logic decides that exposure must
            // be closed, the goal is immediate risk reduction rather than resting a limit
            // order and remaining exposed.
            //
            // This keeps the lifecycle:
            //
            // PriceTick
            //     -> risk decision
            //     -> CloseTrade {
            //            execution: ExitOrderRequest::market()
            //        }
            //     -> handle_close_trade()
            //     -> reduce-only Market Exit Order
            //     -> execution
            //
            // and preserves the same event-driven architecture across live trading,
            // simulation, and historical backtesting.
            let send_result =
                self.tx_engine.send(
                    EngineEvent::CloseTrade {
                        trade_id,
                        execution:
                            ExitOrderRequest::market(),
                        reason,
                        ts_ms,
                    }
                );


            if let Err(err) =
                send_result
            {
                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[ENGINE RISK] CLOSE EVENT SEND FAILED | trade_id={} | symbol={} | error={}",
                        trade_id,
                        symbol,
                        err,
                    ),
                );

                // TODO:
                // Send a remote Slack/Discord critical error notification in
                // live trading. Failure to enqueue a risk-triggered close
                // request means the Trade remains exposed.
            }
        }
    }
}