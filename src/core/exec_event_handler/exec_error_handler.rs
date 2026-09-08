use crate::core::{
    engine_state::Engine,
    exec::ExecEvent,
    trades::TradeStatus,
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

    /// Handles an execution or order error reported by the live exchange adapter.
    ///
    /// For now this handler is intentionally diagnostic-only.
    ///
    /// It does not remove PendingOpen / PendingClose state or roll back Trades.
    /// Instead, it captures as much engine state as possible so an important
    /// remote alert can explain what was happening when the adapter reported
    /// the failure.
    ///
    /// More advanced recovery / reconciliation can be added later.
    pub(crate) fn handle_exec_error(
        &mut self,
        event: ExecEvent,
    ) {
        let ExecEvent::Error {
            cid,
            symbol,
            message,
        } = event
        else {
            return;
        };


        // ============================================================
        // INSPECT ENGINE STATE
        // ============================================================

        let pending_open =
            self.pending_opens.get(
                &cid
            );

        let pending_close =
            self.pending_closes.get(
                &cid
            );


        // Count only active Trades for this symbol.
        //
        // Closing Trades still represent real exchange exposure until
        // Alpaca confirms the final close fill, so they are included.
        let open_trades_for_symbol =
            self.trades
                .live
                .values()
                .filter(
                    |trade| {
                        trade.symbol == symbol
                            && (
                                trade.status == TradeStatus::Open
                                || trade.status == TradeStatus::Closing
                            )
                    }
                )
                .count();


        let symbol_owner =
            self.owner_by_symbol
                .get(&symbol)
                .cloned();


        let symbol_open_count =
            self.open_count_by_symbol
                .get(&symbol)
                .copied()
                .unwrap_or(0);


        // ============================================================
        // BUILD ERROR CONTEXT
        // ============================================================

        let pending_open_info =
            match pending_open {
                Some(pending) => {
                    format!(
                        "YES | \
                        oid={:?} | strategy={} | \
                        requested_qty={} | filled_qty={} | avg_fill_px={:?}",
                        pending.oid,
                        pending.strategy,
                        pending.requested_qty,
                        pending.filled_qty,
                        pending.avg_fill_px,
                    )
                }

                None => {
                    "NO".to_string()
                }
            };


        let pending_close_info =
            match pending_close {
                Some(pending) => {
                    format!(
                        "YES | \
                        trade_id={} | oid={:?} | \
                        requested_qty={:?} | filled_qty={} | avg_fill_px={:?}",
                        pending.trade_id,
                        pending.oid,
                        pending.requested_qty,
                        pending.filled_qty,
                        pending.avg_fill_px,
                    )
                }

                None => {
                    "NO".to_string()
                }
            };


        // ============================================================
        // LOCAL ERROR LOG
        // ============================================================

        print_log(
            self.debug_mode,
            true,
            format!(
                "[ENGINE] EXEC ERROR | \
                cid={} | symbol={} | message={} | \
                pending_open=[{}] | \
                pending_close=[{}] | \
                open_trades_for_symbol={} | \
                open_count={} | \
                owner={:?}",
                cid,
                symbol,
                message,
                pending_open_info,
                pending_close_info,
                open_trades_for_symbol,
                symbol_open_count,
                symbol_owner,
            ),
        );


        // ============================================================
        // REMOTE EXECUTION ERROR ALERT
        // ============================================================

        let remote_message = format!(
            "🚨 EXECUTION ERROR – `{}`\n\
            Symbol: `{}`\n\
            CID: `{}`\n\
            Error: `{}`\n\
            Pending Open: `{}`\n\
            Pending Close: `{}`\n\
            Open Trades For Symbol: `{}`\n\
            Engine Open Count: `{}`\n\
            Symbol Owner: `{}`\n\
            Time: `{}`",
            get_bot_name(),
            symbol,
            cid,
            message,
            pending_open_info,
            pending_close_info,
            open_trades_for_symbol,
            symbol_open_count,
            symbol_owner
                .as_deref()
                .unwrap_or("None"),
            get_local_timestamp(),
        );

        send_remote_message(
            &self.cfg,
            remote_message,
            Some(get_slack_error_channel_id()),
            Some(get_discord_error_channel_id()),
        );


        // ============================================================
        // ERROR RECOVERY
        // ============================================================

        // @todo:
        // Do not mutate execution state here yet. for now errors are just
        // sent via send_remote_message for manual intervention.
        //
        // Later we can decide how different failures should be recovered:
        //
        //     PendingOpen with zero fills
        //         -> remove pending open
        //         -> possibly release symbol ownership
        //
        //     PendingClose with zero fills
        //         -> remove pending close
        //         -> restore TradeStatus::Open
        //
        //     PendingClose with partial fills
        //         -> reconcile the remaining Trade quantity and realized PnL
        //
        // For now the state is intentionally preserved so the full execution
        // context remains available for manual inspection.
    }

}