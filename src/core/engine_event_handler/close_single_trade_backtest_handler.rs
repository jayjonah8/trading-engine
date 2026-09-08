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
// HANDLE CLOSE SINGLE BACKTEST TRADE
// ============================================================

impl Engine {

    /// Handles a request to close one specific backtest trade by symbol.
    ///
    /// The trade is closed immediately using the latest historical market
    /// price and timestamp stored in LastPrice.
    ///
    /// Final bookkeeping flows through EngineEvent::TradeClosedBacktest.
    pub(crate) fn handle_close_single_trade_backtest(
        &mut self,
        symbol: String,
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
                    "[ENGINE BACKTEST CLOSE SINGLE][GATE] REJECT | \
                    symbol={} | invalid execution mode={:?} | reason={}",
                    symbol,
                    self.execution_mode,
                    reason,
                ),
            );

            return;
        }


        // ============================================================
        // GET HISTORICAL MARKET PRICE
        // ============================================================

        let Some(last_price) =
            self.last_px
                .get(&symbol)
                .copied()
        else {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE BACKTEST CLOSE SINGLE] ERROR | \
                    no historical last price | symbol={} | reason={}",
                    symbol,
                    reason,
                ),
            );

            return;
        };


        let exit_px =
            last_price.px;

        let ts_ms =
            last_price.ts_ms;


        // ============================================================
        // FIND TARGET TRADE
        // ============================================================

        // Symbol ownership prevents multiple active trades for the same
        // symbol, so there should only be one matching open trade.
        let trade_id =
            self.trades
                .live
                .values()
                .find(|trade| {
                    trade.symbol == symbol
                        && trade.status == TradeStatus::Open
                })
                .map(|trade| trade.id.clone());


        let Some(trade_id) =
            trade_id
        else {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE BACKTEST CLOSE SINGLE][GATE] REJECT | \
                    no open trade | symbol={} | reason={}",
                    symbol,
                    reason,
                ),
            );

            return;
        };


        // ============================================================
        // TRADING PHASE
        // ============================================================

        let previous_phase =
            self.trading_phase;

        self.trading_phase =
            TradingPhase::ClosingTrades;


        // ============================================================
        // GET TRADE VALUES
        // ============================================================

        // Pull the values needed before calling self.on_event().
        //
        // TradeClosedBacktest will mutate and remove the Trade, so no
        // borrow of the Trade should remain alive across that call.
        let (
            is_long,
            entry_avg_px,
            qty,
            status,
        ) =
            match self.trades.live.get(
                &trade_id
            ) {
                Some(trade) => {
                    (
                        trade.is_long,
                        trade.executed_entry_avg_px,
                        trade.qty,
                        trade.status,
                    )
                }

                None => {
                    self.trading_phase =
                        previous_phase;

                    return;
                }
            };


        if status != TradeStatus::Open {
            self.trading_phase =
                previous_phase;

            return;
        }


        // ============================================================
        // CALCULATE REALIZED PNL
        // ============================================================

        let realized_pnl =
            if is_long {
                (exit_px - entry_avg_px) * qty.abs()
            } else {
                (entry_avg_px - exit_px) * qty.abs()
            };


        // ============================================================
        // MARK TRADE CLOSING
        // ============================================================

        if let Some(trade) =
            self.trades.live.get_mut(
                &trade_id
            )
        {
            if trade.status != TradeStatus::Open {
                self.trading_phase =
                    previous_phase;

                return;
            }

            trade.status =
                TradeStatus::Closing;

            trade.exit_signal_price =
                Some(exit_px);
        }


        // ============================================================
        // BACKTEST CLOSE LOG
        // ============================================================

        print_log(
            self.debug_mode,
            true,
            format!(
                "[ENGINE BACKTEST CLOSE SINGLE] CLOSE | \
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


        // ============================================================
        // BACKTEST TRADE CLOSE PATH
        // ============================================================

        self.on_event(
            EngineEvent::TradeClosedBacktest {
                id: trade_id,
                ts_ms,
                realized: realized_pnl,
                exit_px,
            }
        );


        // ============================================================
        // RESTORE TRADING PHASE
        // ============================================================

        self.trading_phase =
            previous_phase;
    }
}