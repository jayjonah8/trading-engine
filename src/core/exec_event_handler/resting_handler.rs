use crate::core::{
    engine_state::Engine,
    exec::ExecEvent,
    trades::TradeStatus,
};

use crate::notifications::{
    send_remote_message,
    env_helpers::{
        get_slack_trades_channel_id,
        get_discord_trades_channel_id,
    },
};

use crate::shared::utils::{
    get_bot_name,
    get_local_timestamp,
};

use crate::shared::print_log;


impl Engine {

    /// Handles a live order that has been accepted but is currently resting.
    ///
    /// The order remains pending because no final execution has occurred yet.
    ///
    /// For now this handler is diagnostic-only. It records enough information
    /// to support a future remote warning if an order remains resting longer
    /// than expected.
    pub(crate) fn handle_exec_resting(
        &mut self,
        event: ExecEvent,
    ) {
        let ExecEvent::Resting {
            cid,
            symbol,
            oid,
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
        // BUILD RESTING CONTEXT
        // ============================================================

        let pending_open_info =
            match pending_open {
                Some(pending) => {
                    format!(
                        "YES | \
                        strategy={} | requested_qty={} | \
                        filled_qty={} | avg_fill_px={:?}",
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
                        trade_id={} | requested_qty={:?} | \
                        filled_qty={} | avg_fill_px={:?}",
                        pending.trade_id,
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
        // LOCAL RESTING LOG
        // ============================================================

        print_log(
            self.debug_mode,
            true,
            format!(
                "[ENGINE] EXEC RESTING | \
                cid={} | symbol={} | oid={} | \
                pending_open=[{}] | \
                pending_close=[{}] | \
                open_trades_for_symbol={} | \
                open_count={} | \
                owner={:?}",
                cid,
                symbol,
                oid,
                pending_open_info,
                pending_close_info,
                open_trades_for_symbol,
                symbol_open_count,
                symbol_owner,
            ),
        );


        // ============================================================
        // REMOTE RESTING ORDER MESSAGE
        // ============================================================

        let remote_message = format!(
            "⏳ ORDER RESTING – `{}`\n\
            Symbol: `{}`\n\
            CID: `{}`\n\
            OID: `{}`\n\
            Pending Open: `{}`\n\
            Pending Close: `{}`\n\
            Open Trades For Symbol: `{}`\n\
            Engine Open Count: `{}`\n\
            Symbol Owner: `{}`\n\
            Time: `{}`",
            get_bot_name(),
            symbol,
            cid,
            oid,
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
            Some(get_slack_trades_channel_id()),
            Some(get_discord_trades_channel_id()),
        );
    }

}