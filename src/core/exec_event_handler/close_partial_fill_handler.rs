use crate::core::{
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

    /// Handles an individual partial fill for a live closing order.
    ///
    /// Updates the matching PendingClose with Alpaca's authoritative
    /// cumulative close state.
    ///
    /// Alpaca provides:
    ///
    /// - `fill_px` / `fill_sz` = THIS individual execution.
    /// - `avg_px` / `total_sz` = cumulative state of the ENTIRE close order.
    ///
    /// Example:
    ///
    ///     first execution:
    ///
    ///         fill_sz  = 0.886983632
    ///         total_sz = 0.886983632
    ///
    ///     final execution:
    ///
    ///         fill_sz  = 155
    ///         total_sz = 155.886983632
    ///
    /// The Trade itself remains unchanged while the close order is still
    /// active. PendingClose tracks how much of the position Alpaca has
    /// actually closed.
    pub(crate) fn handle_close_partial_fill(
        &mut self,
        event: ExecEvent,
    ) {
        let ExecEvent::ClosePartialFill {
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
        // FIND PENDING CLOSE
        // ============================================================

        let Some(pending) =
            self.pending_closes.get_mut(
                &cid
            )
       else {
            let remote_message = format!(
                "🚨 CLOSE PARTIAL FILL ERROR – `{}`\n\
                Received a close partial fill with no matching PendingClose\n\
                Symbol: `{}`\n\
                CID: `{}`\n\
                OID: `{}`\n\
                Error: `No PendingClose found`\n\
                Time: `{}`",
                get_bot_name(),
                symbol,
                cid,
                oid,
                get_local_timestamp(),
            );

            send_remote_message(
                &self.cfg,
                remote_message,
                Some(get_slack_error_channel_id()),
                Some(get_discord_error_channel_id()),
            );

            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] EXEC CLOSE PARTIAL FILL ERROR | \
                    no PendingClose found | cid={} | symbol={} | oid={}",
                    cid,
                    symbol,
                    oid,
                ),
            );

            return;
        };


        // ============================================================
        // DEDUPE EXECUTION
        // ============================================================

        // Every Alpaca execution receives its own execution_id.
        //
        // WebSocket reconnects or duplicate delivery must not cause the
        // same execution to be processed more than once.
        if !pending.execution_ids.insert(
            execution_id.clone()
        ) {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] EXEC CLOSE PARTIAL FILL DUPLICATE | \
                    cid={} | symbol={} | execution_id={}",
                    cid,
                    symbol,
                    execution_id,
                ),
            );

            return;
        }


        // ============================================================
        // UPDATE CUMULATIVE CLOSE STATE
        // ============================================================

        pending.oid =
            Some(
                oid.clone()
            );


        // `total_sz` already includes every previous execution for this
        // close order.
        //
        // Therefore assign it directly. Never do:
        //
        //     pending.filled_qty += total_sz
        //
        // because that would repeatedly add cumulative quantities.
        pending.filled_qty =
            total_sz;


        // Alpaca's filled_avg_price is also cumulative across all fills
        // belonging to this close order.
        pending.avg_fill_px =
            Some(
                avg_px
            );


        // Keep the Trade relationship before ending the PendingClose
        // mutable borrow.
        let trade_id =
            pending.trade_id.clone();


        // ============================================================
        // RECORD CLOSE ORDER ID ON TRADE
        // ============================================================

        // The Trade remains in TradeStatus::Closing while partial fills
        // continue.
        //
        // Preserve the closing Alpaca order ID for diagnostics and future
        // remote alerts. The final CloseFill will set this again before
        // TradeClosed performs shared finalization.
        if let Some(trade) =
            self.trades.live.get_mut(
                &trade_id
            )
        {
            trade.close_oid =
                Some(
                    oid.clone()
                );
        } else {
            let remote_message = format!(
                "🚨 CLOSE PARTIAL FILL ERROR – `{}`\n\
                PendingClose references a Trade that no longer exists\n\
                Symbol: `{}`\n\
                Trade ID: `{}`\n\
                CID: `{}`\n\
                OID: `{}`\n\
                Error: `Trade not found in engine state`\n\
                Time: `{}`",
                get_bot_name(),
                symbol,
                trade_id,
                cid,
                oid,
                get_local_timestamp(),
            );

            send_remote_message(
                &self.cfg,
                remote_message,
                Some(get_slack_error_channel_id()),
                Some(get_discord_error_channel_id()),
            );

            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] EXEC CLOSE PARTIAL FILL ERROR | \
                    Trade not found | trade_id={} | cid={} | symbol={}",
                    trade_id,
                    cid,
                    symbol,
                ),
            );
        }


        // ============================================================
        // PARTIAL CLOSE LOG
        // ============================================================

        print_log(
            self.debug_mode,
            true,
            format!(
                "[ENGINE] EXEC CLOSE PARTIAL FILL | \
                cid={} | trade_id={} | symbol={} | \
                fill_px={} | fill_sz={} | \
                avg_px={} | total_sz={} | \
                oid={} | execution_id={}",
                cid,
                trade_id,
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
        // PARTIAL CLOSE ACCOUNTING
        // ============================================================

        // If we later want realized PnL to be recognized immediately on
        // every individual partial execution, `fill_px` and `fill_sz` are
        // the correct values to use here.
        //
        // For now we intentionally leave the original Trade quantity and
        // realized PnL unchanged while the order remains partially filled.
        //
        // PendingClose contains the authoritative partial-close state:
        //
        //     pending.filled_qty
        //     pending.avg_fill_px
        //
        // When Alpaca sends the final CloseFill, its cumulative `total_sz`
        // and `avg_px` represent the complete close and are used to calculate
        // final realized PnL before TradeClosed performs shared bookkeeping.
        //
        // IMPORTANT:
        // A future canceled/rejected close AFTER partial fills will need
        // special handling because some quantity has actually been closed
        // while the remainder is still open.
    }

}