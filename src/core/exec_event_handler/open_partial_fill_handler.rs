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

    // fill_px / fill_sz = this one execution
    // avg_px / total_sz = whole order so far
    //
    // That’s why you don’t use fill_px and fill_sz to update PendingOpen state here; 
    // Alpaca already gives you the cumulative state directly. You’re just logging 
    // the individual execution details.
    

    /// Handles an individual partial execution for a live opening order.
    ///
    /// Alpaca provides two different views of the fill in each `partial_fill` event:
    ///
    /// - `fill_px` / `fill_sz` describe THIS individual execution.
    /// - `avg_px` / `total_sz` describe the ENTIRE order filled so far.
    ///
    /// Example:
    ///
    /// A previous execution filled 38 shares, then the next execution fills 2:
    ///
    ///     fill_sz  = 2
    ///     fill_px  = 320.75
    ///
    ///     total_sz = 40
    ///     avg_px   = 320.731
    ///
    /// The engine stores `total_sz` and `avg_px` on PendingOpen because those are
    /// Alpaca's authoritative cumulative values for the position filled so far.
    /// `fill_sz` and `fill_px` remain useful for identifying/logging the individual
    /// execution but must not be added again to the cumulative values.
    ///
    /// Flow:
    /// partial_fill → deduplicate execution → replace cumulative pending fill state
    ///
    /// PendingOpen remains active until the final OpenFill arrives. The final event
    /// provides the completed cumulative quantity and average entry price that are
    /// then used to create the Trade.
    pub(crate) fn handle_open_partial_fill(
        &mut self,
        event: ExecEvent,
    ) {
        let ExecEvent::OpenPartialFill {
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
        // FIND PENDING OPEN
        // ============================================================

        let Some(pending) =
            self.pending_opens.get_mut(&cid)
        else {
            let remote_message = format!(
                "🚨 OPEN PARTIAL FILL ERROR – `{}`\n\
                Received an open partial fill with no matching PendingOpen\n\
                Symbol: `{}`\n\
                CID: `{}`\n\
                OID: `{}`\n\
                Error: `No PendingOpen found`\n\
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
                    "[ENGINE] EXEC OPEN PARTIAL FILL ERROR | \
                    no PendingOpen found | cid={} | symbol={} | oid={}",
                    cid,
                    symbol,
                    oid,
                ),
            );

            return;
        };


        // ============================================================
        // DEDUPLICATE EXECUTION
        // ============================================================

        // Each Alpaca execution has its own execution_id.
        //
        // Multiple partial fills belong to the same order, so the order oid is
        // not sufficient for deduplication. Ignore only an execution_id that
        // has already been processed.
        if !pending.execution_ids.insert(
            execution_id.clone()
        ) {
            print_log(
                self.debug_mode,
                false,
                format!(
                    "[ENGINE] EXEC OPEN PARTIAL FILL DUPLICATE | \
                    cid={} | symbol={} | execution_id={}",
                    cid,
                    symbol,
                    execution_id,
                ),
            );

            return;
        }


        // ============================================================
        // UPDATE CUMULATIVE FILL STATE
        // ============================================================

        // Store the exchange order ID here as well as during ACK handling.
        //
        // WebSocket and REST events are independent, so this keeps PendingOpen
        // correct even if the execution event reaches the engine before its ACK.
        pending.oid =
            Some(oid.clone());


        // `fill_sz` is only THIS execution.
        //
        // `total_sz` is Alpaca's cumulative filled quantity for the whole order so far.
        // Therefore replace `pending.filled_qty` with total_sz rather than adding to it.
        //
        // Example:
        //
        // previous total = 38
        // current fill_sz = 2
        // current total_sz = 40
        //
        // Correct:
        //     filled_qty = 40
        //
        // Wrong:
        //     filled_qty += 40
        pending.filled_qty =
            total_sz;


        // `avg_px` is also cumulative. Alpaca has already calculated the weighted
        // average price across every execution belonging to this order so far.
        //
        // We therefore store Alpaca's value directly instead of recalculating it from
        // the individual `fill_px` and `fill_sz`.
        pending.avg_fill_px =
            Some(avg_px);


        print_log(
            self.debug_mode,
            true,
            format!(
                "[ENGINE] EXEC OPEN PARTIAL FILL | \
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
    }

}