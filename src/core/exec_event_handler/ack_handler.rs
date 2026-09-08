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

    /// Handles an acknowledgement that a live exchange order was successfully submitted.
    ///
    /// The ACK is produced after the adapter's REST request succeeds and Alpaca returns
    /// an exchange order ID (`oid`). Before sending this event, the adapter stores the
    /// `oid → cid` correlation so future WebSocket trade updates can be routed back to
    /// the correct engine order.
    ///
    /// This handler attaches that `oid` to the matching PendingOpen or PendingClose.
    /// The pending state already exists before the REST request, so the ACK does not
    /// create an order or Trade and does not mean that any quantity has been filled.
    ///
    /// Flow:
    /// REST succeeds → adapter stores oid correlation → ACK → attach oid to pending state
    ///
    /// If no pending state exists for the cid, the engine and exchange may be out of
    /// sync and the condition should be treated as an important execution error.
    ///
    /// ACKs do not require execution-id deduplication because they are not fills, and
    /// assigning the same exchange order ID again would not change execution state.
    pub(crate) fn handle_exec_ack(
        &mut self,
        event: ExecEvent,
    ) {
        let ExecEvent::Ack {
            cid,
            symbol,
            oid,
        } = event
        else {
            return;
        };


        // ============================================================
        // ATTACH EXCHANGE ORDER ID
        // ============================================================

        // The REST request succeeded, so attach the Alpaca order ID
        // to whichever pending request owns this cid.
        if let Some(pending) =
            self.pending_opens.get_mut(&cid)
        {
            pending.oid =
                Some(oid.clone());
        } else if let Some(pending) =
            self.pending_closes.get_mut(&cid)
        {
            pending.oid =
                Some(oid.clone());
        } else {
            let remote_message = format!(
                "🚨 EXEC ACK ERROR – `{}`\n\
                Alpaca accepted an order with no matching pending engine state\n\
                Symbol: `{}`\n\
                CID: `{}`\n\
                OID: `{}`\n\
                Error: `No PendingOpen or PendingClose found`\n\
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
                    "[ENGINE] EXEC ACK ERROR | \
                    no pending order found | cid={} | symbol={} | oid={}",
                    cid,
                    symbol,
                    oid,
                ),
            );

            return;
        }

        print_log(
            self.debug_mode,
            true,
            format!(
                "[ENGINE] EXEC ACK | \
                cid={} | symbol={} | oid={}",
                cid,
                symbol,
                oid,
            ),
        );
    }

}