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
// HANDLE CLOSE SINGLE LIVE / SIM TRADE
// ============================================================

impl Engine {

    /// Handles a request to close one specific live or simulated trade by symbol.
    ///
    /// Live trades create a PendingClose and send ExecCommand::Close to the
    /// execution adapter.
    ///
    /// Simulated trades are closed locally using the latest market price and
    /// then flow through EngineEvent::TradeClosed for shared bookkeeping.
    ///
    /// TradingPhase is temporarily changed to ClosingTrades while the close
    /// request is being created so new trade intents cannot enter during the
    /// close operation.
    pub(crate) fn handle_close_single_trade(
        &mut self,
        symbol: String,
        reason: String,
    ) {
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
                    "[ENGINE CLOSE SINGLE][GATE] REJECT | \
                    symbol={} | invalid execution mode={:?}",
                    symbol,
                    self.execution_mode,
                ),
            );

            return;
        }


        // ============================================================
        // GET CURRENT MARKET PRICE
        // ============================================================

        let Some(exit_px) =
            self.last_px
                .get(&symbol)
                .map(|last_price| last_price.px)
        else {
            let message = format!(
                "🚨 CLOSE SINGLE TRADE ERROR – `{}`\n\
                Cannot close trade because no current market price exists\n\
                Symbol: `{}`\n\
                Reason: `{}`\n\
                Time: `{}`",
                get_bot_name(),
                symbol,
                reason,
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
                    "[ENGINE CLOSE SINGLE] ERROR | \
                    no last_px | symbol={} | reason={}",
                    symbol,
                    reason,
                ),
            );

            return;
        };


        // ============================================================
        // FIND TARGET TRADE
        // ============================================================

        // There should only be one active trade per symbol because symbol
        // ownership prevents duplicate positions.
        //
        // The execution mode determines whether we are looking for the live
        // or simulated version of the position.
        let trade_id =
            self.trades
                .live
                .values()
                .find(|trade| {
                    trade.symbol == symbol
                        && trade.status == TradeStatus::Open
                        && match self.execution_mode {
                            ExecutionMode::Live => !trade.is_simulated,
                            ExecutionMode::Simulation => trade.is_simulated,
                            ExecutionMode::Backtest => false,
                        }
                })
                .map(|trade| trade.id.clone());


        let Some(trade_id) =
            trade_id
        else {
            print_log(
                self.debug_mode,
                false,
                format!(
                    "[ENGINE CLOSE SINGLE][GATE] REJECT | \
                    no open trade | symbol={} | mode={:?} | reason={}",
                    symbol,
                    self.execution_mode,
                    reason,
                ),
            );

            return;
        };


        // ============================================================
        // CLOSE TIMESTAMP
        // ============================================================

        let ts_ms =
            Utc::now()
                .timestamp_millis();


        // ============================================================
        // TRADING PHASE GATE
        // ============================================================

        let previous_phase =
            self.trading_phase;

        self.trading_phase =
            TradingPhase::ClosingTrades;


        // ============================================================
        // EXECUTION MODE
        // ============================================================

        match self.execution_mode {

            // ========================================================
            // LIVE CLOSE
            // ========================================================

            ExecutionMode::Live => {

                let close_cid =
                    format!(
                        "manual-close-{}-{}",
                        trade_id,
                        ts_ms,
                    );


                // ====================================================
                // CREATE PENDING CLOSE
                // ====================================================

                let Some(trade) =
                    self.trades.live.get_mut(
                        &trade_id
                    )
                else {
                    self.trading_phase =
                        previous_phase;

                    return;
                };


                // Protect against another close lifecycle having started
                // between locating the trade and reaching this point.
                if trade.status
                    != TradeStatus::Open
                {
                    self.trading_phase =
                        previous_phase;

                    return;
                }


                if trade.is_simulated {
                    self.trading_phase =
                        previous_phase;

                    return;
                }


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
                        signal_price: exit_px,
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
                // LOCAL SEND FAILURE
                // ====================================================

                if let Err(error) =
                    send_result
                {
                    self.pending_closes.remove(
                        &close_cid
                    );

                    trade.status =
                        TradeStatus::Open;

                    trade.close_cid =
                        None;


                    let message = format!(
                        "🚨 EXECUTION ERROR - IN CLOSE SINGLE TRADE – `{}`\n\
                        Failed to send forced ExecCommand::Close\n\
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
                            "[ENGINE CLOSE SINGLE] LIVE SEND ERROR | \
                            id={} | symbol={} | cid={} | reason={} | error={}",
                            trade_id,
                            symbol,
                            close_cid,
                            reason,
                            error,
                        ),
                    );


                    self.trading_phase =
                        previous_phase;

                    return;
                }


                // ====================================================
                // LIVE CLOSE REQUEST NOTIFICATION
                // ====================================================

                let message = format!(
                    "🔻 LIVE TRADE CLOSE REQUESTED – `{}`\n\
                    Symbol: `{}`\n\
                    Trade ID: `{}`\n\
                    Signal Price: `${:.6}`\n\
                    Reason: `{}`\n\
                    CID: `{}`\n\
                    Time: `{}`",
                    get_bot_name(),
                    symbol,
                    trade_id,
                    exit_px,
                    reason,
                    close_cid,
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
                        "[ENGINE CLOSE SINGLE] LIVE CLOSE REQUEST | \
                        id={} | symbol={} | cid={} | exit_px={} | reason={}",
                        trade_id,
                        symbol,
                        close_cid,
                        exit_px,
                        reason,
                    ),
                );
            }


            // ========================================================
            // SIMULATION CLOSE
            // ========================================================

            ExecutionMode::Simulation => {

                // Pull the values we need before calling self.on_event().
                //
                // TradeClosed may mutate and remove the Trade, so no borrow
                // of the Trade should remain alive across that call.
                let (
                    is_simulated,
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
                                trade.is_simulated,
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


                if !is_simulated
                    || status != TradeStatus::Open
                {
                    self.trading_phase =
                        previous_phase;

                    return;
                }


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
                // MARK SIM TRADE CLOSING
                // ====================================================

                let close_cid =
                    format!(
                        "manual-close-sim-{}-{}",
                        trade_id,
                        ts_ms,
                    );


                if let Some(trade) =
                    self.trades.live.get_mut(
                        &trade_id
                    )
                {
                    trade.status =
                        TradeStatus::Closing;

                    trade.close_cid =
                        Some(
                            close_cid.clone()
                        );

                    trade.exit_signal_price =
                        Some(exit_px);
                }


                // ====================================================
                // SIM CLOSE REQUEST NOTIFICATION
                // ====================================================

                let message = format!(
                    "🔻 🧪 SIM TRADE CLOSE REQUESTED – `{}`\n\
                    Symbol: `{}`\n\
                    Trade ID: `{}`\n\
                    Exit Price: `${:.6}`\n\
                    Reason: `{}`\n\
                    CID: `{}`\n\
                    Time: `{}`",
                    get_bot_name(),
                    symbol,
                    trade_id,
                    exit_px,
                    reason,
                    close_cid,
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
                        "[ENGINE CLOSE SINGLE] SIM CLOSE | \
                        id={} | symbol={} | entry_avg_px={} | \
                        exit_px={} | qty={} | realized_pnl=${:.4} | reason={}",
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

            ExecutionMode::Backtest => {
                print_log(
                    self.debug_mode,
                    false,
                    format!(
                        "[ENGINE CLOSE SINGLE][GATE] REJECT | \
                        symbol={} | mode=Backtest | reason={}",
                        symbol,
                        reason,
                    ),
                );

                self.trading_phase = previous_phase;

                return;
            }

        }

        // ============================================================
        // RESTORE TRADING PHASE
        // ============================================================

        self.trading_phase =
            previous_phase;
    }

}