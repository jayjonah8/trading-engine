use std::collections::HashSet;
use std::time::Instant;

use crate::core::{
    engine::{
        EngineEvent,
    },
    engine_state::Engine,
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
    },
};

use crate::shared::utils::{
    get_bot_name,
    get_local_timestamp,
};

use crate::shared::print_log;


impl Engine {

    /// Processes a live market price update for an individual symbol.
    ///
    /// This function evaluates fully-open live and simulated Trades.
    ///
    /// PendingOpen orders are intentionally ignored because their final
    /// quantity and average entry price are not known until the opening
    /// execution is complete.
    ///
    /// Each completed Trade already contains the strategy-specific exit
    /// rules that were copied from the original TradeIntent through
    /// PendingOpen:
    ///
    /// - take-profit percentage
    /// - normal stop-loss percentage
    /// - emergency stop-loss percentage
    /// - normal stop-loss delay
    /// - optional maximum trade duration
    ///
    /// PnL percentage is calculated from the Trade's final average entry
    /// price (`avg_px`) against the latest market price.
    ///
    /// Positive PnL percentage means the trade is profitable.
    /// Negative PnL percentage means the trade is losing.
    ///
    /// Live trades send ExecCommand::Close to the exchange adapter and wait
    /// for the exchange's final CloseFill.
    ///
    /// Simulated trades bypass the exchange adapter and immediately produce
    /// EngineEvent::TradeClosed using the current simulated market price.
    ///
    /// Both paths eventually use TradeClosed for shared close bookkeeping.
    pub(crate) fn check_risk_for_symbol(
        &mut self,
        symbol: &str,
        px: f64,
        ts_ms: i64,
    ) {
        let now =
            Instant::now();

        let mut live_trades_to_close:
            Vec<(String, String)> =
            Vec::new();

        let mut sim_trades_to_close:
            Vec<(String, String)> =
            Vec::new();


        // ============================================================
        // CHECK OPEN TRADES FOR SYMBOL
        // ============================================================

        for trade in self.trades.live.values_mut() {

            // This price tick only applies to trades for this symbol.
            if trade.symbol != symbol {
                continue;
            }


            // Only fully-open trades participate in normal risk management.
            //
            // PendingOpen is handled separately and is not yet a Trade.
            // Closing trades are skipped because a close is already in-flight.
            if trade.status != TradeStatus::Open {
                continue;
            }


            // ========================================================
            // CALCULATE PNL
            // ========================================================

            // Calculate unrealized PnL percentage from the trade's final
            // average entry fill price.
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

            // Approximate unrealized dollar PnL using the actual filled
            // quantity and average entry price.
            //
            // qty is signed:
            //     long  = positive
            //     short = negative
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
            // TRADE AGE
            // ========================================================

            // entry_time is monotonic and therefore preferred for elapsed
            // duration checks such as SL delays and maximum trade duration.
            let age_secs =
                trade
                    .entry_time
                    .map(
                        |entry_time| {
                            now
                                .duration_since(entry_time)
                                .as_secs()
                        }
                    )
                    .unwrap_or(0);

            print_log(
                self.debug_mode,
                false,
                format!(
                    "[ENGINE RISK] | \
                    symbol={} | id={} | \
                    px={:.4} | avg_px={:.4} | \
                    pnl=${:.4} | pnl_pct={:.4}% | \
                    age={}s | sim={} | ts_ms={}",
                    symbol,
                    trade.id,
                    px,
                    trade.executed_entry_avg_px,
                    unrealized_pnl,
                    pnl_percent,
                    age_secs,
                    trade.is_simulated,
                    ts_ms,
                ),
            );


            // ========================================================
            // HELPER: MARK TRADE FOR CLOSE
            // ========================================================

            let mut mark_for_close =
                |reason: &str| {
                    if trade.is_simulated {
                        sim_trades_to_close.push(
                            (
                                trade.id.clone(),
                                reason.to_string(),
                            )
                        );
                    } else {
                        live_trades_to_close.push(
                            (
                                trade.id.clone(),
                                reason.to_string(),
                            )
                        );
                    }
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

            // Emergency SL intentionally does not wait for min_sl_delay.
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
        // REQUEST LIVE CLOSES
        // ============================================================
        //
        // Live trades must go through the execution adapter.
        //
        // Flow:
        //
        // Trade::Open
        //     ↓
        // create PendingClose
        //     ↓
        // Trade::Closing
        //     ↓
        // ExecCommand::Close
        //     ↓
        // Alpaca REST
        //     ↓
        // trade_updates
        //     ↓
        // ClosePartialFill / CloseFill
        //     ↓
        // EngineEvent::TradeClosed
        //
        // PendingClose is created BEFORE ExecCommand::Close is sent so
        // engine-side state already exists if Alpaca responds immediately.
        for (
            trade_id,
            reason,
        ) in live_trades_to_close
        {
            // ========================================================
            // GET OPEN TRADE
            // ========================================================

            let Some(trade) =
                self.trades.live.get_mut(
                    &trade_id
                )
            else {
                continue;
            };

            // Only an Open trade may begin a new close lifecycle.
            if trade.status
                != TradeStatus::Open
            {
                continue;
            }

            // Simulation trades should never enter the live close path.
            if trade.is_simulated {
                continue;
            }


            // ========================================================
            // CREATE CLOSE ID
            // ========================================================

            let close_cid =
                format!(
                    "close-{}-{}",
                    trade.id,
                    ts_ms,
                );

            let trade_symbol =
                trade.symbol.clone();


            // ========================================================
            // CREATE PENDING CLOSE
            // ========================================================

            // Create engine-side pending state BEFORE sending the command
            // to the exchange adapter.
            //
            // This mirrors the opening lifecycle:
            //
            // PendingOpen exists before ExecCommand::Place.
            // PendingClose exists before ExecCommand::Close.
            //
            // requested_qty = None means close the entire live position.
            self.pending_closes.insert(
                close_cid.clone(),
                PendingClose {
                    cid: close_cid.clone(),
                    oid: None,
                    trade_id: trade.id.clone(),
                    symbol: trade_symbol.clone(),
                    requested_qty: None,
                    filled_qty: 0.0,
                    avg_fill_px: None,
                    signal_price: px,
                    execution_ids: HashSet::new(),
                    ts_ms,
                },
            );


            // ========================================================
            // MARK TRADE CLOSING
            // ========================================================

            // The actual position still exists at Alpaca.
            //
            // Closing prevents later price ticks from generating another
            // close request while this close order is working.
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
                    "[ENGINE RISK] LIVE CLOSE REQUEST | \
                    id={} | symbol={} | cid={} | reason={}",
                    trade_id,
                    trade_symbol,
                    close_cid,
                    reason,
                ),
            );


            // ========================================================
            // SEND CLOSE COMMAND
            // ========================================================

            let send_result =
                self.tx_exec.send(
                    ExecCommand::Close {
                        cid: close_cid.clone(),
                        symbol: trade_symbol.clone(),
                        qty: None,
                    }
                );


            // ========================================================
            // HANDLE LOCAL SEND FAILURE
            // ========================================================

            if let Err(error) =
                send_result
            {
                // The adapter never received the close command, so undo
                // the local PendingClose and restore the Trade to Open.
                //
                // This is different from an Alpaca REST/exchange error.
                // Exchange-side errors arrive later through ExecEvent::Error.
                self.pending_closes.remove(
                    &close_cid
                );

                trade.status =
                    TradeStatus::Open;

                trade.close_cid =
                    None;

                let message = format!(
                    "🚨 EXECUTION ERROR – `{}`\n\
                    Failed to send ExecCommand::Close\n\
                    Symbol: `{}`\n\
                    Trade ID: `{}`\n\
                    CID: `{}`\n\
                    Error: `{}`\n\
                    Time: `{}`",
                    get_bot_name(),
                    trade_symbol,
                    trade_id,
                    close_cid,
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
                        "[ENGINE RISK] LIVE CLOSE SEND ERROR | \
                        id={} | symbol={} | cid={} | error={}",
                        trade_id,
                        trade_symbol,
                        close_cid,
                        error,
                    ),
                );

                continue;
            }
        }


        // ============================================================
        // CLOSE SIMULATED TRADES LOCALLY
        // ============================================================
        //
        // Simulation uses exactly the same risk decision logic as Live,
        // but it does not create PendingClose or call the Alpaca adapter.
        //
        // Instead:
        //
        // Trade::Open
        //     ↓
        // risk condition triggers
        //     ↓
        // calculate realized PnL using current simulated price
        //     ↓
        // EngineEvent::TradeClosed
        //
        // TradeClosed then performs the same shared close bookkeeping
        // used after a real Alpaca CloseFill.
        for (
            trade_id,
            reason,
        ) in sim_trades_to_close
        {
            // Pull everything required from the Trade first.
            //
            // self.on_event(TradeClosed) will later mutate/remove the trade,
            // so we should not keep a borrow alive across that call.
            let (
                is_simulated,
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
                            trade.is_simulated,
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


            if !is_simulated {
                continue;
            }

            if status
                != TradeStatus::Open
            {
                continue;
            }


            // ========================================================
            // CALCULATE SIMULATED REALIZED PNL
            // ========================================================

            // Simulation treats the current market price as the exit fill.
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

            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE RISK] SIM CLOSE | \
                    id={} | symbol={} | \
                    entry_avg_px={} | exit_px={} | \
                    qty={} | realized_pnl=${:.4} | reason={}",
                    trade_id,
                    trade_symbol,
                    entry_avg_px,
                    px,
                    qty,
                    realized_pnl,
                    reason,
                ),
            );


            // ========================================================
            // SHARED TRADE CLOSE PATH
            // ========================================================

            // Simulation bypasses Alpaca entirely and goes directly into
            // the same final engine bookkeeping used by live CloseFill.
            self.on_event(
                EngineEvent::TradeClosed {
                    id: trade_id,
                    ts_ms,
                    realized: realized_pnl,
                    exit_px: px,
                }
            );
        }
    }

}