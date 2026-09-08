use crate::core::{
    engine::EngineEvent,
    engine_state::Engine,
    trades::TradeStatus,
};

use crate::shared::print_log;


impl Engine {

    /// Processes a historical market price update for an individual symbol
    /// during backtesting.
    ///
    /// Evaluates fully-open backtest Trades against their strategy-specific
    /// take-profit, stop-loss, emergency stop-loss, and duration rules.
    ///
    /// All elapsed-time calculations use the historical replay timestamp
    /// (`ts_ms`) and the Trade's historical `opened_ms`. The system clock
    /// must never be used for backtest risk decisions.
    ///
    /// Backtest trades do not create PendingClose state or send execution
    /// commands to an exchange adapter. When an exit condition is triggered,
    /// realized PnL is calculated from the historical market price and the
    /// trade is finalized through EngineEvent::TradeClosedBacktest.
    pub(crate) fn check_risk_for_backtest_symbol(
        &mut self,
        symbol: &str,
        px: f64,
        ts_ms: i64,
    ) {
        let mut trades_to_close:
            Vec<(String, String)> =
            Vec::new();


        // ============================================================
        // CHECK OPEN TRADES FOR SYMBOL
        // ============================================================

        for trade in self.trades.live.values_mut() {

            // This historical price tick only applies to trades
            // belonging to this symbol.
            if trade.symbol != symbol {
                continue;
            }


            // Only fully-open trades participate in risk management.
            //
            // Closing trades are skipped because their close lifecycle
            // has already been started.
            if trade.status != TradeStatus::Open {
                continue;
            }


            // ========================================================
            // CALCULATE PNL
            // ========================================================

            // Calculate unrealized PnL percentage using the trade's
            // historical executed entry price and current replay price.
            //
            // Long:
            //     entry = 100
            //     current = 102
            //     pnl = +2%
            //
            // Short:
            //     entry = 100
            //     current = 98
            //     pnl = +2%
            let pnl_percent =
                if trade.is_long {
                    (
                        (px - trade.executed_entry_avg_px)
                        / trade.executed_entry_avg_px
                    ) * 100.0
                } else {
                    (
                        (trade.executed_entry_avg_px - px)
                        / trade.executed_entry_avg_px
                    ) * 100.0
                };


            // Calculate unrealized dollar PnL using the actual
            // backtest trade quantity.
            let unrealized_pnl =
                if trade.is_long {
                    (px - trade.executed_entry_avg_px)
                        * trade.qty.abs()
                } else {
                    (trade.executed_entry_avg_px - px)
                        * trade.qty.abs()
                };

            trade.unrealized_pnl =
                unrealized_pnl;


            // ========================================================
            // HISTORICAL TRADE AGE
            // ========================================================

            // Backtests must use replay time rather than Instant::now()
            // or any other wall-clock measurement.
            //
            // opened_ms = historical entry timestamp
            // ts_ms     = current historical replay timestamp
            let age_ms =
                ts_ms.saturating_sub(
                    trade.opened_ms
                );

            let age_secs =
                (age_ms / 1000)
                    .max(0) as u64;


            print_log(
                self.debug_mode,
                false,
                format!(
                    "[ENGINE BACKTEST RISK] | \
                    symbol={} | id={} | \
                    px={:.4} | entry_avg_px={:.4} | \
                    pnl=${:.4} | pnl_pct={:.4}% | \
                    age={}s | ts_ms={}",
                    symbol,
                    trade.id,
                    px,
                    trade.executed_entry_avg_px,
                    unrealized_pnl,
                    pnl_percent,
                    age_secs,
                    ts_ms,
                ),
            );


            // ========================================================
            // HELPER: MARK TRADE FOR CLOSE
            // ========================================================

            let mut mark_for_close =
                |reason: &str| {
                    trades_to_close.push(
                        (
                            trade.id.clone(),
                            reason.to_string(),
                        )
                    );
                };


            // ========================================================
            // TAKE PROFIT
            // ========================================================

            if pnl_percent
                >= trade.exit_profit_percent
            {
                mark_for_close(
                    "TAKE_PROFIT"
                );

                continue;
            }


            // ========================================================
            // EMERGENCY STOP LOSS
            // ========================================================

            // Emergency stop loss intentionally ignores min_sl_delay.
            if pnl_percent
                <= -trade.emergency_exit_percent
            {
                mark_for_close(
                    "EMERGENCY_STOP_LOSS"
                );

                continue;
            }


            // ========================================================
            // NORMAL STOP LOSS
            // ========================================================

            if pnl_percent
                <= -trade.exit_loss_percent
            {
                if age_secs
                    >= trade.min_sl_delay
                {
                    mark_for_close(
                        "STOP_LOSS"
                    );

                    continue;
                }
            }


            // ========================================================
            // MAX TRADE DURATION
            // ========================================================

            if trade.enable_trade_duration {
                if let Some(
                    trade_duration_secs
                ) =
                    trade.trade_duration_secs
                {
                    if age_secs
                        >= trade_duration_secs
                    {
                        mark_for_close(
                            "TRADE_DURATION"
                        );

                        continue;
                    }
                }
            }
        }


        // ============================================================
        // CLOSE BACKTEST TRADES
        // ============================================================
        //
        // Backtest trades never communicate with an exchange adapter.
        //
        // Flow:
        //
        // Trade::Open
        //     ↓
        // historical risk condition triggers
        //     ↓
        // calculate realized PnL using historical price
        //     ↓
        // Trade::Closing
        //     ↓
        // EngineEvent::TradeClosedBacktest
        //     ↓
        // final backtest close bookkeeping
        //
        // The current historical market price is treated as the
        // simulated execution price for the backtest close.
        for (
            trade_id,
            reason,
        ) in trades_to_close
        {
            // ========================================================
            // GET OPEN BACKTEST TRADE
            // ========================================================

            // Extract the values required to close the trade before
            // calling self.on_event(), because TradeClosedBacktest may
            // mutate or remove the Trade.
            let (
                is_long,
                entry_avg_px,
                qty,
                trade_symbol,
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
                            trade.symbol.clone(),
                            trade.status,
                        )
                    }

                    None => {
                        continue;
                    }
                };


            if status
                != TradeStatus::Open
            {
                continue;
            }


            // ========================================================
            // CALCULATE REALIZED PNL
            // ========================================================

            // The current historical replay price is used as the
            // backtest exit execution price.
            //
            // Long:
            //     realized = (exit - entry) * qty
            //
            // Short:
            //     realized = (entry - exit) * qty
            let realized_pnl =
                if is_long {
                    (
                        px
                        - entry_avg_px
                    ) * qty.abs()
                } else {
                    (
                        entry_avg_px
                        - px
                    ) * qty.abs()
                };


            // ========================================================
            // MARK TRADE CLOSING
            // ========================================================

            // Mark the Trade as Closing before finalizing it so another
            // historical price event cannot begin another close lifecycle.
            if let Some(trade) =
                self.trades.live.get_mut(
                    &trade_id
                )
            {
                trade.status =
                    TradeStatus::Closing;

                trade.exit_signal_price =
                    Some(px);
            }


            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE BACKTEST RISK] CLOSE | \
                    id={} | symbol={} | \
                    entry_avg_px={} | exit_px={} | \
                    qty={} | realized_pnl=${:.4} | \
                    ts_ms={} | reason={}",
                    trade_id,
                    trade_symbol,
                    entry_avg_px,
                    px,
                    qty,
                    realized_pnl,
                    ts_ms,
                    reason,
                ),
            );


            // ========================================================
            // FINALIZE BACKTEST TRADE
            // ========================================================

            self.on_event(
                EngineEvent::TradeClosedBacktest {
                    id: trade_id,
                    ts_ms,
                    realized: realized_pnl,
                    exit_px: px,
                }
            );
        }
    }
}