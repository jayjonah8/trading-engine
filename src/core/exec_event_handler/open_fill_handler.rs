use std::time::{
    Instant,
    SystemTime,
};

use crate::core::{
    engine_state::Engine,
    exec::ExecEvent,
    trades::{
        Trade,
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


impl Engine {

    /// Handles the final exchange fill for a live opening order.
    ///
    /// Finalizes the PendingOpen with Alpaca's authoritative cumulative quantity
    /// and average fill price, then removes it from pending state and uses the
    /// returned PendingOpen data to construct the permanent Trade.
    ///
    /// Flow:
    /// PendingOpen → final fill state → remove from pending → Trade::Open
    ///
    /// The completed Trade keeps the existing symbol ownership and begins normal
    /// trade management using the final exchange quantity and entry price.
    pub(crate) fn handle_open_fill(
        &mut self,
        event: ExecEvent,
    ) {
        let ExecEvent::OpenFill {
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
        // UPDATE FINAL PENDING STATE
        // ============================================================

        let Some(pending) =
            self.pending_opens.get_mut(&cid)
        else {
            let message = format!(
                "🚨 OPEN FILL ERROR – `{}`\n\
                Symbol: {}\n\
                Error: No PendingOpen found for final exchange fill\n\
                CID: {}\n\
                OID: {}\n\
                Execution ID: {}\n\
                Time: {}",
                get_bot_name(),
                symbol,
                cid,
                oid,
                execution_id,
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
                    "[ENGINE] EXEC OPEN FILL ERROR | \
                    no PendingOpen found | cid={} | symbol={} | oid={}",
                    cid,
                    symbol,
                    oid,
                ),
            );

            return;
        };

        // Prevent the same final execution from being processed twice.
        if !pending.execution_ids.insert(
            execution_id.clone()
        ) {
            print_log(
                self.debug_mode,
                false,
                format!(
                    "[ENGINE] EXEC OPEN FILL DUPLICATE | \
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

        // Alpaca's order.filled_qty is cumulative, so assign it directly.
        //
        // Any previous partial fills are already included in total_sz.
        pending.filled_qty =
            total_sz;

        // The final cumulative average becomes the authoritative
        // entry price for the completed trade.
        pending.avg_fill_px =
            Some(avg_px);

        print_log(
            self.debug_mode,
            true,
            format!(
                "[ENGINE] EXEC OPEN FILL | \
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
        // REMOVE COMPLETED PENDING OPEN
        // ============================================================

        // Remove the completed PendingOpen from the map and take ownership of it.
        //
        // `remove()` returns the full PendingOpen, so all of its configuration and
        // strategy values remain available below while it is converted into Trade.
        //
        // The stored execution_ids are also removed here. This is safe because the
        // adapter removes its order correlation after the final fill, preventing that
        // completed order from normally producing another correlated execution event.
        let Some(pending) =
            self.pending_opens.remove(&cid)
        else {
            return;
        };


        // ============================================================
        // CREATE LIVE TRADE
        // ============================================================

        let trade_id =
            format!(
                "{}-{}-{}",
                pending.strategy,
                pending.symbol,
                pending.ts_ms,
            );

        let signed_qty =
            if pending.is_long {
                total_sz
            } else {
                -total_sz
            };

        let trade =
            Trade {
                id: trade_id.clone(),
                symbol: pending.symbol.clone(),
                is_long: pending.is_long,
                qty: signed_qty,
                executed_entry_avg_px: avg_px,
                executed_exit_avg_px: None,
                entry_signal_price: pending.signal_price,
                exit_signal_price: None,
                oid: oid.clone(),
                cid: cid.clone(),
                strategy: pending.strategy.clone(),
                opened_ms: chrono::Utc::now().timestamp_millis(),
                unrealized_pnl: 0.0,
                status: TradeStatus::Open,
                trade_amount_usd: pending.trade_amount_usd,
                exit_profit_percent: pending.exit_profit_percent,
                exit_loss_percent: pending.exit_loss_percent,
                emergency_exit_percent: pending.emergency_exit_percent,
                min_sl_delay: pending.min_sl_delay,
                enable_trade_duration: pending.enable_trade_duration,
                trade_duration_secs: pending.trade_duration_secs,
                entry_time: Some(Instant::now()),
                entry_wall_time: Some(SystemTime::now()),
                is_simulated: false,
                close_cid: None,
                close_oid: None,
            };


        // ============================================================
        // STORE LIVE TRADE
        // ============================================================

        self.trades.upsert(
            trade
        );

        *self
            .open_count_by_symbol
            .entry(
                pending.symbol.clone()
            )
            .or_insert(0) += 1;

        // owner_by_symbol intentionally remains reserved because the same
        // strategy still owns the symbol after PendingOpen becomes Trade.


        // ============================================================
        // LIVE TRADE OPENED NOTIFICATION
        // ============================================================

        let message = format!(
            "✅ LIVE TRADE OPENED – `{}`\n\
            Symbol: `{}`\n\
            Side: `{}`\n\
            Size: `{:.6}`\n\
            USD: `${:.2}`\n\
            Notional: `${:.2}`\n\
            Executed Price: `${:.6}`\n\
            Signal Price: `${:.6}`\n\
            Max Duration: `{}`\n\
            Strategy: `{}`\n\
            CID: `{}`\n\
            OID: `{}`\n\
            Time: `{}`",
            get_bot_name(),
            pending.symbol,
            if pending.is_long {
                "LONG"
            } else {
                "SHORT"
            },
            signed_qty.abs(),
            pending.trade_amount_usd,
            signed_qty.abs() * avg_px,
            avg_px,
            pending.signal_price,
            pending.trade_duration_secs
                .map(|secs| format!("{}s", secs))
                .unwrap_or_else(|| "None".to_string()),
            pending.strategy,
            cid,
            oid,
            get_local_timestamp(),
        );

        send_remote_message(
            &self.cfg,
            message,
            Some(get_slack_trades_channel_id()),
            Some(get_discord_trades_channel_id()),
        );


        // ============================================================
        // LOG LIVE TRADE
        // ============================================================

        print_log(
            self.debug_mode,
            true,
            format!(
                "[ENGINE] LIVE TRADE OPENED | \
                id={} | symbol={} | side={} | \
                qty={} | avg_px={} | notional=${:.2} | \
                cid={} | oid={}",
                trade_id,
                pending.symbol,
                if pending.is_long {
                    "LONG"
                } else {
                    "SHORT"
                },
                signed_qty,
                avg_px,
                pending.trade_amount_usd,
                cid,
                oid,
            ),
        );
    }

}