use crate::core::{
    engine_state::{
        Engine,
        ExecutionMode,
    },
    trades::TradeStatus,
};

use crate::shared::print_log;
use crate::shared::utils::format_backtest_trade_duration;


// ============================================================
// HANDLE TRADE CLOSED BACKTEST
// ============================================================

impl Engine {

    /// Finalizes a completed historical backtest Trade.
    ///
    /// Backtest trades use historical replay timestamps rather than
    /// wall-clock time and do not send remote notifications.
    pub(crate) fn handle_trade_closed_backtest(
        &mut self,
        id: String,
        ts_ms: i64,
        realized: f64,
        exit_px: f64,
    ) {

        // ============================================================
        // EXECUTION MODE GATE
        // ============================================================

        if self.execution_mode != ExecutionMode::Backtest {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE BACKTEST] TRADE CLOSED REJECT | \
                    id={} | invalid execution mode={:?}",
                    id,
                    self.execution_mode,
                ),
            );

            return;
        }


        // ============================================================
        // FINALIZE TRADE STATE
        // ============================================================

        let (
            symbol,
            strategy,
            is_long,
            qty,
            trade_amount_usd,
            executed_entry_avg_px,
            entry_signal_price,
            exit_signal_price,
            opened_ms,
        ) = {
            let Some(trade) =
                self.trades.live.get_mut(
                    &id
                )
            else {
                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[ENGINE BACKTEST] TRADE CLOSED ERROR | \
                        trade not found | id={} | realized={} | exit_px={}",
                        id,
                        realized,
                        exit_px,
                    ),
                );

                return;
            };


            let symbol =
                trade.symbol.clone();

            let strategy =
                trade.strategy.clone();

            let is_long =
                trade.is_long;

            let qty =
                trade.qty;

            let trade_amount_usd =
                trade.trade_amount_usd;

            let executed_entry_avg_px =
                trade.executed_entry_avg_px;

            let entry_signal_price =
                trade.entry_signal_price;

            let exit_signal_price =
                trade.exit_signal_price
                    .unwrap_or(exit_px);

            let opened_ms =
                trade.opened_ms;


            trade.status =
                TradeStatus::Closed;

            trade.executed_exit_avg_px =
                Some(exit_px);

            trade.exit_signal_price =
                Some(exit_signal_price);


            (
                symbol,
                strategy,
                is_long,
                qty,
                trade_amount_usd,
                executed_entry_avg_px,
                entry_signal_price,
                exit_signal_price,
                opened_ms,
            )
        };


        // ============================================================
        // REALIZED PNL ACCOUNTING
        // ============================================================

        self.general.realized_pnl += realized;


        // ============================================================
        // UPDATE SYMBOL POSITION COUNT
        // ============================================================

        if let Some(count) =
            self.open_count_by_symbol.get_mut(
                &symbol
            )
        {
            *count =
                count.saturating_sub(1);
        }


        if self.open_count_by_symbol.get(&symbol).copied() == Some(0) {
            self.open_count_by_symbol.remove(
                &symbol
            );
        }


        // ============================================================
        // RELEASE SYMBOL OWNERSHIP
        // ============================================================

        if !self.trades.has_open_trade_for_symbol(
            &symbol
        ) {
            self.owner_by_symbol.remove(
                &symbol
            );
        }


        // ============================================================
        // RECORD LAST DECISION
        // ============================================================

        self.last_decision_ts.insert(
            symbol.clone(),
            ts_ms,
        );


        // ============================================================
        // FINAL CLOSE REPORT VALUES
        // ============================================================

        let pnl_percent =
            if is_long {
                ((exit_px - executed_entry_avg_px) / executed_entry_avg_px) * 100.0
            } else {
                ((executed_entry_avg_px - exit_px) / executed_entry_avg_px) * 100.0
            };


        let trade_duration =
            format_backtest_trade_duration(
                opened_ms,
                ts_ms,
            );


        let side =
            if is_long {
                "LONG"
            } else {
                "SHORT"
            };


        // ============================================================
        // FINAL BACKTEST CLOSE LOG
        // ============================================================

        print_log(
            self.debug_mode,
            true,
            format!(
                "[ENGINE BACKTEST] TRADE CLOSED | \
                id={} | symbol={} | strategy={} | side={} | \
                qty={} | trade_amount_usd={} | \
                entry_avg_px={} | exit_avg_px={} | \
                entry_signal_px={} | exit_signal_px={} | \
                realized_pnl=${:.4} | pnl_pct={:+.4}% | \
                opened_ms={} | closed_ms={} | duration={},",
                id,
                symbol,
                strategy,
                side,
                qty.abs(),
                trade_amount_usd,
                executed_entry_avg_px,
                exit_px,
                entry_signal_price,
                exit_signal_price,
                realized,
                pnl_percent,
                opened_ms,
                ts_ms,
                trade_duration,
            ),
        );


        // ============================================================
        // REMOVE CLOSED TRADE
        // ============================================================

        self.trades.remove(
            &id
        );
    }
}