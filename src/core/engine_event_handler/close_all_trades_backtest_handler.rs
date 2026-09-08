use crate::core::{
    engine::EngineEvent,
    engine_state::{
        Engine,
        ExecutionMode,
        TradingPhase,
    },
    trades::TradeStatus,
};

use crate::shared::print_log;


// ============================================================
// HANDLE CLOSE ALL BACKTEST TRADES
// ============================================================

impl Engine {

    /// Handles a request to close all currently open backtest trades.
    ///
    /// Each trade is closed immediately using its latest historical market
    /// price and timestamp stored in LastPrice.
    ///
    /// Final bookkeeping flows through EngineEvent::TradeClosedBacktest.
    pub(crate) fn handle_close_all_trades_backtest(
        &mut self,
        reason: String,
    ) {

        // ============================================================
        // EXECUTION MODE GATE
        // ============================================================

        if self.execution_mode != ExecutionMode::Backtest {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE BACKTEST CLOSE ALL][GATE] REJECT | \
                    invalid execution mode={:?} | reason={}",
                    self.execution_mode,
                    reason,
                ),
            );

            return;
        }


        // ============================================================
        // COUNT TARGET TRADES
        // ============================================================

        let close_count =
            self.trades
                .live
                .values()
                .filter(|trade| trade.status == TradeStatus::Open)
                .count();


        if close_count == 0 {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE BACKTEST CLOSE ALL] NO OPEN TRADES | reason={}",
                    reason,
                ),
            );

            return;
        }


        // ============================================================
        // TRADING PHASE
        // ============================================================

        let previous_phase =
            self.trading_phase;

        self.trading_phase =
            TradingPhase::ClosingTrades;


        print_log(
            self.debug_mode,
            true,
            format!(
                "[ENGINE BACKTEST CLOSE ALL] START | \
                target_count={} | reason={}",
                close_count,
                reason,
            ),
        );


        // ============================================================
        // COLLECT TRADES
        // ============================================================

        // Collect first because TradeClosedBacktest mutates and removes
        // trades from engine state.
        //
        // LastPrice provides both the historical exit price and the
        // historical timestamp associated with that price.
        let trades_to_close:
            Vec<(String, String, bool, f64, f64, f64, i64)> =
            self.trades
                .live
                .values()
                .filter(|trade| trade.status == TradeStatus::Open)
                .filter_map(|trade| {
                    let last_price =
                        self.last_px
                            .get(&trade.symbol)
                            .copied();

                    let Some(last_price) =
                        last_price
                    else {
                        print_log(
                            self.debug_mode,
                            true,
                            format!(
                                "[ENGINE BACKTEST CLOSE ALL] SKIP | \
                                no historical last price | id={} | symbol={} | reason={}",
                                trade.id,
                                trade.symbol,
                                reason,
                            ),
                        );

                        return None;
                    };

                    Some((
                        trade.id.clone(),
                        trade.symbol.clone(),
                        trade.is_long,
                        trade.executed_entry_avg_px,
                        trade.qty,
                        last_price.px,
                        last_price.ts_ms,
                    ))
                })
                .collect();


        // ============================================================
        // CLOSE BACKTEST TRADES
        // ============================================================

        for (
            trade_id,
            symbol,
            is_long,
            entry_avg_px,
            qty,
            exit_px,
            ts_ms,
        ) in trades_to_close {

            // ========================================================
            // REALIZED PNL
            // ========================================================

            let realized_pnl =
                if is_long {
                    (exit_px - entry_avg_px) * qty.abs()
                } else {
                    (entry_avg_px - exit_px) * qty.abs()
                };


            // ========================================================
            // MARK TRADE CLOSING
            // ========================================================

            if let Some(trade) =
                self.trades.live.get_mut(
                    &trade_id
                )
            {
                if trade.status != TradeStatus::Open {
                    continue;
                }

                trade.status =
                    TradeStatus::Closing;

                trade.exit_signal_price =
                    Some(exit_px);
            }


            // ========================================================
            // BACKTEST CLOSE LOG
            // ========================================================

            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE BACKTEST CLOSE ALL] CLOSE | \
                    id={} | symbol={} | entry_avg_px={} | exit_px={} | \
                    qty={} | realized_pnl=${:.4} | ts_ms={} | reason={}",
                    trade_id,
                    symbol,
                    entry_avg_px,
                    exit_px,
                    qty,
                    realized_pnl,
                    ts_ms,
                    reason,
                ),
            );


            // ========================================================
            // BACKTEST TRADE CLOSE PATH
            // ========================================================

            self.on_event(
                EngineEvent::TradeClosedBacktest {
                    id: trade_id,
                    ts_ms,
                    realized: realized_pnl,
                    exit_px,
                }
            );
        }


        // ============================================================
        // RESTORE TRADING PHASE
        // ============================================================

        self.trading_phase =
            previous_phase;


        print_log(
            self.debug_mode,
            true,
            format!(
                "[ENGINE BACKTEST CLOSE ALL] COMPLETE | \
                target_count={} | reason={} | phase_restored={:?}",
                close_count,
                reason,
                previous_phase,
            ),
        );
    }
}