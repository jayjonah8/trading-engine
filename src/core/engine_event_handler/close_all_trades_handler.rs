use std::collections::HashSet;

use chrono::Utc;

use crate::core::{
    engine::EngineEvent,
    engine_state::{
        Engine,
        ExecutionMode,
        TradingPhase,
    },
    exec::ExecCommand,
    trades::{
        PendingClose,
        TradeStatus,
    },
};

use crate::notifications::{
    send_remote_message,
    env_helpers::{
        get_slack_error_channel_id,
        get_discord_error_channel_id,
        get_slack_trades_channel_id,
        get_discord_trades_channel_id,
    },
};

use crate::shared::utils::{
    get_bot_name,
    get_local_timestamp,
};

use crate::shared::print_log;


// ============================================================
// HANDLE CLOSE ALL LIVE / SIM TRADES
// ============================================================

impl Engine {

    /// Handles a request to close all currently open live or simulated trades.
    ///
    /// Live trades create PendingClose state and send ExecCommand::Close
    /// through the execution adapter.
    ///
    /// Simulated trades are closed locally using the latest known market price
    /// and then flow through EngineEvent::TradeClosed for shared bookkeeping.
    ///
    /// The engine temporarily enters TradingPhase::ClosingTrades while the
    /// close requests are being created.
    pub(crate) fn handle_close_all_trades(&mut self, reason: String) {

        // ============================================================
        // EXECUTION MODE GATE
        // ============================================================

        if !matches!(
            self.execution_mode,
            ExecutionMode::Live | ExecutionMode::Simulation
        ) {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE CLOSE ALL][GATE] REJECT | \
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
                .filter(|trade| {
                    trade.status == TradeStatus::Open
                        && match self.execution_mode {
                            ExecutionMode::Live => !trade.is_simulated,
                            ExecutionMode::Simulation => trade.is_simulated,
                            ExecutionMode::Backtest => false,
                        }
                })
                .count();


        // Nothing needs to be closed.
        if close_count == 0 {
            print_log(
                self.debug_mode,
                false,
                format!(
                    "[ENGINE CLOSE ALL] NO OPEN TRADES | mode={:?} | reason={}",
                    self.execution_mode,
                    reason,
                ),
            );

            return;
        }


        // ============================================================
        // TRADING PHASE GATE
        // ============================================================

        let previous_phase =
            self.trading_phase;

        self.trading_phase =
            TradingPhase::ClosingTrades;


        // ============================================================
        // CLOSE ALL NOTIFICATION
        // ============================================================

        let mode_tag =
            match self.execution_mode {
                ExecutionMode::Live => "🔻 LIVE",
                ExecutionMode::Simulation => "🔻 🧪 SIM",
                ExecutionMode::Backtest => "BACKTEST",
            };


        let message = format!(
            "🧹 CLOSE ALL TRADES REQUESTED – `{}`\n\
            Mode: `{}`\n\
            Target Open Trades: `{}`\n\
            Reason: `{}`\n\
            Trading Phase: `ClosingTrades`\n\
            Time: `{}`",
            get_bot_name(),
            mode_tag,
            close_count,
            reason,
            get_local_timestamp(),
        );


        send_remote_message(
            &self.cfg,
            message,
            Some(get_slack_trades_channel_id()),
            Some(get_discord_trades_channel_id()),
        );


        print_log(
            self.debug_mode,
            true,
            format!(
                "[ENGINE CLOSE ALL] START | \
                mode={} | count={} | reason={}",
                mode_tag,
                close_count,
                reason,
            ),
        );


        // ============================================================
        // EXECUTION MODE
        // ============================================================

        match self.execution_mode {

            // ========================================================
            // LIVE CLOSE ALL
            // ========================================================

            ExecutionMode::Live => {

                // Collect first so we do not hold an iterator borrow over
                // self.trades while modifying engine state below.
                let trades_to_close:
                    Vec<(String, String, f64)> =
                    self.trades
                        .live
                        .values()
                        .filter(|trade| {
                            trade.status == TradeStatus::Open
                                && !trade.is_simulated
                        })
                        .map(|trade| {
                            let signal_price =
                                self.last_px
                                    .get(&trade.symbol)
                                    .map(|last_price| last_price.px)
                                    .unwrap_or(trade.executed_entry_avg_px);

                            (
                                trade.id.clone(),
                                trade.symbol.clone(),
                                signal_price,
                            )
                        })
                        .collect();


                for (
                    trade_id,
                    symbol,
                    signal_price,
                ) in trades_to_close {

                    // ====================================================
                    // CREATE CLOSE ID
                    // ====================================================

                    let ts_ms =
                        Utc::now()
                            .timestamp_millis();

                    let close_cid =
                        format!(
                            "closeall-live-{}-{}",
                            trade_id,
                            ts_ms,
                        );


                    // ====================================================
                    // GET TRADE
                    // ====================================================

                    let Some(trade) =
                        self.trades.live.get_mut(
                            &trade_id
                        )
                    else {
                        continue;
                    };


                    // Trade may have changed state since collection.
                    if trade.status != TradeStatus::Open {
                        continue;
                    }

                    if trade.is_simulated {
                        continue;
                    }


                    // ====================================================
                    // CREATE PENDING CLOSE
                    // ====================================================

                    self.pending_closes.insert(
                        close_cid.clone(),
                        PendingClose {
                            cid: close_cid.clone(),
                            oid: None,
                            trade_id: trade.id.clone(),
                            symbol: symbol.clone(),
                            requested_qty: None,
                            filled_qty: 0.0,
                            avg_fill_px: None,
                            signal_price,
                            execution_ids: HashSet::new(),
                            ts_ms,
                        },
                    );


                    // ====================================================
                    // MARK TRADE CLOSING
                    // ====================================================

                    trade.status =
                        TradeStatus::Closing;

                    trade.close_cid =
                        Some(
                            close_cid.clone()
                        );


                    print_log(
                        self.debug_mode,
                        true,
                        format!(
                            "[ENGINE CLOSE ALL] LIVE CLOSE REQUEST | \
                            id={} | symbol={} | cid={} | signal_price={} | reason={}",
                            trade_id,
                            symbol,
                            close_cid,
                            signal_price,
                            reason,
                        ),
                    );


                    // ====================================================
                    // SEND CLOSE COMMAND
                    // ====================================================

                    let send_result =
                        self.tx_exec.send(
                            ExecCommand::Close {
                                cid: close_cid.clone(),
                                symbol: symbol.clone(),
                                qty: None,
                            }
                        );


                    // ====================================================
                    // HANDLE LOCAL SEND FAILURE
                    // ====================================================

                    if let Err(error) =
                        send_result
                    {
                        // Adapter never received the close command, so undo
                        // the local close state and return the Trade to Open.
                        self.pending_closes.remove(
                            &close_cid
                        );

                        trade.status =
                            TradeStatus::Open;

                        trade.close_cid =
                            None;


                        let message = format!(
                            "🚨 EXECUTION ERROR – `{}`\n\
                            Failed to send Close-All ExecCommand::Close\n\
                            Symbol: `{}`\n\
                            Trade ID: `{}`\n\
                            CID: `{}`\n\
                            Reason: `{}`\n\
                            Error: `{}`\n\
                            Time: `{}`",
                            get_bot_name(),
                            symbol,
                            trade_id,
                            close_cid,
                            reason,
                            error,
                            get_local_timestamp(),
                        );


                        send_remote_message(
                            &self.cfg,
                            message,
                            Some(get_slack_error_channel_id()),
                            Some(get_discord_error_channel_id()),
                        );


                        print_log(
                            self.debug_mode,
                            true,
                            format!(
                                "[ENGINE CLOSE ALL] LIVE SEND ERROR | \
                                id={} | symbol={} | cid={} | reason={} | error={}",
                                trade_id,
                                symbol,
                                close_cid,
                                reason,
                                error,
                            ),
                        );

                        continue;
                    }
                }
            }


            // ========================================================
            // SIMULATION CLOSE ALL
            // ========================================================

            ExecutionMode::Simulation => {

                // Collect everything needed before calling self.on_event().
                // TradeClosed may mutate and remove trades from self.trades.
                let trades_to_close:
                    Vec<(String, String, bool, f64, f64, f64)> =
                    self.trades
                        .live
                        .values()
                        .filter(|trade| {
                            trade.status == TradeStatus::Open
                                && trade.is_simulated
                        })
                        .map(|trade| {
                            let exit_px =
                                self.last_px
                                    .get(&trade.symbol)
                                    .map(|last_price| last_price.px)
                                    .unwrap_or(trade.executed_entry_avg_px);

                            (
                                trade.id.clone(),
                                trade.symbol.clone(),
                                trade.is_long,
                                trade.executed_entry_avg_px,
                                trade.qty,
                                exit_px,
                            )
                        })
                        .collect();


                for (
                    trade_id,
                    symbol,
                    is_long,
                    entry_avg_px,
                    qty,
                    exit_px,
                ) in trades_to_close {

                    // ====================================================
                    // CALCULATE REALIZED PNL
                    // ====================================================

                    let realized_pnl =
                        if is_long {
                            (
                                exit_px
                                - entry_avg_px
                            ) * qty.abs()
                        } else {
                            (
                                entry_avg_px
                                - exit_px
                            ) * qty.abs()
                        };


                    // ====================================================
                    // CREATE CLOSE VALUES
                    // ====================================================

                    let ts_ms =
                        Utc::now()
                            .timestamp_millis();


                    let close_cid =
                        format!(
                            "closeall-sim-{}-{}",
                            trade_id,
                            ts_ms,
                        );


                    // ====================================================
                    // MARK TRADE CLOSING
                    // ====================================================

                    if let Some(trade) =
                        self.trades.live.get_mut(
                            &trade_id
                        )
                    {
                        if trade.status != TradeStatus::Open
                            || !trade.is_simulated
                        {
                            continue;
                        }

                        trade.status =
                            TradeStatus::Closing;

                        trade.close_cid =
                            Some(
                                close_cid.clone()
                            );

                        trade.exit_signal_price =
                            Some(exit_px);
                    }


                    print_log(
                        self.debug_mode,
                        true,
                        format!(
                            "[ENGINE CLOSE ALL] SIM CLOSE | \
                            id={} | symbol={} | \
                            entry_avg_px={} | exit_px={} | \
                            qty={} | realized_pnl=${:.4} | reason={}",
                            trade_id,
                            symbol,
                            entry_avg_px,
                            exit_px,
                            qty,
                            realized_pnl,
                            reason,
                        ),
                    );


                    // ====================================================
                    // SHARED TRADE CLOSE PATH
                    // ====================================================

                    self.on_event(
                        EngineEvent::TradeClosed {
                            id: trade_id,
                            ts_ms,
                            realized: realized_pnl,
                            exit_px,
                        }
                    );
                }
            }


            // ========================================================
            // BACKTEST
            // ========================================================

            // Backtest closes use the dedicated CloseAllBackTestTrades
            // event and handler.
            ExecutionMode::Backtest => {
                self.trading_phase =
                    previous_phase;

                return;
            }
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
                "[ENGINE CLOSE ALL] REQUESTS COMPLETE | \
                mode={} | target_count={} | reason={} | phase_restored={:?}",
                mode_tag,
                close_count,
                reason,
                previous_phase,
            ),
        );
    }

}