use crate::core::{
    engine::EngineEvent,
    engine_state::Engine,
    exec::ExecEvent,
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

    /// Handles the final fill for a live closing order.
    ///
    /// Alpaca provides two views of the final close execution:
    ///
    /// - `fill_px` / `fill_sz` describe THIS final individual execution.
    /// - `avg_px` / `total_sz` describe the ENTIRE close order.
    ///
    /// Example:
    ///
    ///     previous partial close = 0.886983632 shares
    ///     final fill_sz          = 155 shares
    ///
    ///     total_sz = 155.886983632
    ///     avg_px   = 321.63
    ///
    /// The cumulative `total_sz` and `avg_px` therefore represent the
    /// authoritative final result of the complete closing order.
    ///
    /// Flow:
    ///
    /// PendingClose
    ///     ↓
    /// final cumulative close fill
    ///     ↓
    /// calculate realized PnL
    ///     ↓
    /// EngineEvent::TradeClosed
    ///     ↓
    /// shared live/simulation close bookkeeping
    pub(crate) fn handle_close_fill(
        &mut self,
        event: ExecEvent,
    ) {
        let ExecEvent::CloseFill {
            cid,
            symbol,
            fill_px,
            fill_sz,
            avg_px,
            total_sz,
            oid,
            execution_id,
        } = event
        else {
            return;
        };


        // ============================================================
        // UPDATE FINAL PENDING CLOSE STATE
        // ============================================================

        let Some(pending) =
            self.pending_closes.get_mut(&cid)
        else {
            let message = format!(
                "🚨 CLOSE FILL ERROR – `{}`\n\
                No PendingClose found\n\
                Symbol: `{}`\n\
                CID: `{}`\n\
                OID: `{}`\n\
                Time: `{}`",
                get_bot_name(),
                symbol,
                cid,
                oid,
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
                    "[ENGINE] EXEC CLOSE FILL ERROR | \
                    no PendingClose found | cid={} | symbol={} | oid={}",
                    cid,
                    symbol,
                    oid,
                ),
            );

            return;
        };


        // Each Alpaca execution has its own execution_id.
        //
        // Ignore this event if the exact execution was already processed.
        if !pending.execution_ids.insert(
            execution_id.clone()
        ) {
            print_log(
                self.debug_mode,
                false,
                format!(
                    "[ENGINE] EXEC CLOSE FILL DUPLICATE | \
                    cid={} | symbol={} | execution_id={}",
                    cid,
                    symbol,
                    execution_id,
                ),
            );

            return;
        }


        pending.oid =
            Some(oid.clone());


        // `total_sz` is Alpaca's cumulative quantity closed across the
        // entire order, including every previous partial fill.
        //
        // Therefore assign it directly rather than adding it.
        pending.filled_qty =
            total_sz;


        // `avg_px` is Alpaca's cumulative average exit price across
        // every execution belonging to this close order.
        pending.avg_fill_px =
            Some(avg_px);


        print_log(
            self.debug_mode,
            true,
            format!(
                "[ENGINE] EXEC CLOSE FILL | \
                cid={} | symbol={} | \
                fill_px={} | fill_sz={} | \
                avg_px={} | total_sz={} | \
                oid={} | execution_id={}",
                cid,
                symbol,
                fill_px,
                fill_sz,
                avg_px,
                total_sz,
                oid,
                execution_id,
            ),
        );


        // ============================================================
        // REMOVE COMPLETED PENDING CLOSE
        // ============================================================

        // The final fill completes this close order.
        //
        // Keep ownership of PendingClose so we retain the associated
        // trade_id while removing it from the active pending-close map.
        let Some(pending) =
            self.pending_closes.remove(
                &cid
            )
        else {
            return;
        };


        // ============================================================
        // FIND TRADE BEING CLOSED
        // ============================================================

        let Some(trade) =
            self.trades.live.get_mut(
                &pending.trade_id
            )
        else {
            let message = format!(
                "🚨 CLOSE FILL ERROR – `{}`\n\
                Trade not found in engine state\n\
                Symbol: `{}`\n\
                Trade ID: `{}`\n\
                CID: `{}`\n\
                OID: `{}`\n\
                Time: `{}`",
                get_bot_name(),
                symbol,
                pending.trade_id,
                cid,
                oid,
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
                    "[ENGINE] EXEC CLOSE FILL ERROR | \
                    Trade not found | trade_id={} | cid={} | symbol={}",
                    pending.trade_id,
                    cid,
                    symbol,
                ),
            );

            return;
        };


        // ============================================================
        // CALCULATE FINAL REALIZED PNL
        // ============================================================

        // Use Alpaca's FINAL cumulative close average and cumulative
        // closed quantity, not the final individual fill_px / fill_sz.
        //
        // Long:
        //
        //     realized = (exit - entry) * quantity
        //
        // Short:
        //
        //     realized = (entry - exit) * quantity
        let realized_pnl =
            if trade.is_long {
                (
                    avg_px
                    - trade.executed_entry_avg_px
                ) * total_sz
            } else {
                (
                    trade.executed_entry_avg_px
                    - avg_px
                ) * total_sz
            };


        let trade_id =
            trade.id.clone();


        // ============================================================
        // STORE FINAL CLOSE ORDER ID
        // ============================================================

        // Preserve Alpaca's closing order ID on the Trade so the shared
        // TradeClosed handler can use it for final logs / remote messages
        // before the Trade is removed from memory.
        trade.close_oid =
            Some(
                oid.clone()
            );


        // The mutable Trade borrow should end before calling on_event(),
        // because TradeClosed will mutate this same Trade.
        let _ =
            trade;


        // ============================================================
        // SHARED TRADE CLOSE PATH
        // ============================================================

        let ts_ms =
            chrono::Utc::now()
                .timestamp_millis();


        // Realized PnL accumulation belongs inside TradeClosed so LIVE
        // and SIMULATION use the same accounting path.
        self.on_event(
            EngineEvent::TradeClosed {
                id: trade_id,
                ts_ms,
                realized: realized_pnl,
                exit_px: avg_px,
            }
        );
    }

}