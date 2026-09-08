use crate::core::{
    engine_state::Engine,
    trades::TradeStatus,
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
    format_trade_duration,
};

use crate::shared::print_log;


impl Engine {

    /// Finalizes a completed live or simulated Trade.
    ///
    /// This is the shared close bookkeeping path used by both execution modes.
    ///
    /// Live:
    ///
    ///     CloseFill
    ///         ↓
    ///     TradeClosed
    ///
    /// Simulation:
    ///
    ///     simulated risk close
    ///         ↓
    ///     TradeClosed
    ///
    /// Exchange-specific work should happen before this function. By the time
    /// TradeClosed is emitted, the engine already knows the final exit price
    /// and realized PnL.
    pub(crate) fn handle_trade_closed(
        &mut self,
        id: String,
        ts_ms: i64,
        realized: f64,
        exit_px: f64,
    ) {
        // ============================================================
        // FINALIZE TRADE STATE
        // ============================================================

        let (
            symbol,
            strategy,
            is_simulated,
            is_long,
            qty,
            trade_amount_usd,
            executed_entry_avg_px,
            executed_exit_avg_px,
            entry_signal_price,
            exit_signal_price,
            entry_wall_time,
            open_oid,
            close_cid,
            close_oid,
        ) = {
            let Some(trade) =
                self.trades.live.get_mut(
                    &id
                )
            else {
                let message = format!(
                    "🚨 TRADE CLOSED ERROR – `{}`\n\
                    Trade not found in engine state\n\
                    Trade ID: `{}`\n\
                    Realized PnL: `${:.2}`\n\
                    Exit Price: `${:.6}`\n\
                    Time: `{}`",
                    get_bot_name(),
                    id,
                    realized,
                    exit_px,
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
                        "[ENGINE] TRADE CLOSED ERROR | \
                        Trade not found | id={} | realized={} | exit_px={}",
                        id,
                        realized,
                        exit_px,
                    ),
                );

                return;
            };

            let symbol =
                trade.symbol.clone();

            let strategy =
                trade.strategy.clone();

            let is_simulated =
                trade.is_simulated;

            let is_long =
                trade.is_long;

            let qty =
                trade.qty;

            let trade_amount_usd =
                trade.trade_amount_usd;

            let executed_entry_avg_px =
                trade.executed_entry_avg_px;

            let executed_exit_avg_px =
                trade.executed_exit_avg_px;

            let entry_signal_price =
                trade.entry_signal_price;

            let exit_signal_price =
                trade.exit_signal_price;

            let entry_wall_time =
                trade.entry_wall_time;

            let open_oid =
                trade.oid.clone();

            let close_cid =
                trade.close_cid.clone();

            let close_oid =
                trade.close_oid.clone();


            trade.status =
                TradeStatus::Closed;


            (
                symbol,
                strategy,
                is_simulated,
                is_long,
                qty,
                trade_amount_usd,
                executed_entry_avg_px,
                executed_exit_avg_px,
                entry_signal_price,
                exit_signal_price,
                entry_wall_time,
                open_oid,
                close_cid,
                close_oid,
            )
        };


        // ============================================================
        // RESOLVE FINAL EXIT VALUES
        // ============================================================

        // LIVE exit values are populated by close_fill_handler.rs.
        // SIM bypasses the exchange, so exit_px is used as both the
        // executed exit price and exit signal price when needed.
        let executed_exit_avg_px =
            executed_exit_avg_px
                .unwrap_or(exit_px);

        let exit_signal_price =
            exit_signal_price
                .unwrap_or(exit_px);


        // ============================================================
        // REALIZED PNL ACCOUNTING
        // ============================================================

        if is_simulated {
            self.general.sim_realized_pnl += realized;
        } else {
            self.general.realized_pnl += realized;
        }


        // ============================================================
        // UPDATE SYMBOL POSITION COUNT
        // ============================================================

        if let Some(count) =
            self.open_count_by_symbol.get_mut(
                &symbol
            )
        {
            *count =
                count.saturating_sub(1);
        }


        if self
            .open_count_by_symbol
            .get(&symbol)
            .copied()
            == Some(0)
        {
            self.open_count_by_symbol.remove(
                &symbol
            );
        }


        // ============================================================
        // RELEASE SYMBOL OWNERSHIP
        // ============================================================

        if !self.trades.has_open_trade_for_symbol(
            &symbol
        ) {
            self.owner_by_symbol.remove(
                &symbol
            );
        }


        // ============================================================
        // RECORD LAST DECISION
        // ============================================================

        self.last_decision_ts.insert(
            symbol.clone(),
            ts_ms,
        );


        // ============================================================
        // FINAL CLOSE REPORT VALUES
        // ============================================================

        let pnl_percent =
            if is_long {
                (
                    (
                        executed_exit_avg_px
                        - executed_entry_avg_px
                    )
                    / executed_entry_avg_px
                ) * 100.0
            } else {
                (
                    (
                        executed_entry_avg_px
                        - executed_exit_avg_px
                    )
                    / executed_entry_avg_px
                ) * 100.0
            };


        let trade_duration =
            format_trade_duration(
                entry_wall_time
            );


        let mode_tag =
            if is_simulated {
                "SIM"
            } else {
                "LIVE"
            };


        let side =
            if is_long {
                "LONG"
            } else {
                "SHORT"
            };


        // ============================================================
        // FINAL CLOSE LOG
        // ============================================================

        print_log(
            self.debug_mode,
            true,
            format!(
                "[ENGINE] {} TRADE CLOSED | \
                id={} | symbol={} | strategy={} | side={} | \
                qty={} | entry_avg_px={} | exit_avg_px={} | \
                entry_signal_px={} | exit_signal_px={} | \
                realized_pnl=${:.4} | pnl_pct={:+.4}% | \
                duration={} | open_oid={} | close_oid={}",
                mode_tag,
                id,
                symbol,
                strategy,
                side,
                qty.abs(),
                executed_entry_avg_px,
                executed_exit_avg_px,
                entry_signal_price,
                exit_signal_price,
                realized,
                pnl_percent,
                trade_duration,
                open_oid,
                close_oid
                    .as_deref()
                    .unwrap_or("N/A"),
            ),
        );


        // ============================================================
        // REMOTE TRADE CLOSE MESSAGE
        // ============================================================

        let close_cid_display =
            close_cid
                .as_deref()
                .unwrap_or("N/A");

        let close_oid_display =
            close_oid
                .as_deref()
                .unwrap_or("N/A");

        let title =
            if is_simulated {
                "🔻 🧪 SIM TRADE CLOSED"
            } else {
                "🔻 LIVE TRADE CLOSED"
            };


        let message = format!(
            "{} – `{}`\n\
            Symbol: `{}`\n\
            Side: `{}`\n\
            Size: `{:.6}`\n\
            USD: `${:.2}`\n\
            Notional: `${:.2}`\n\
            Executed Entry Price: `${:.6}`\n\
            Executed Exit Price: `${:.6}`\n\
            Trade PnL: `${:.4}` ({:+.2}%)\n\
            Trade Duration: `{}`\n\
            Entry Signal Price: `${:.6}`\n\
            Exit Signal Price: `${:.6}`\n\
            Strategy: `{}`\n\
            CID: `{}`\n\
            OID: `{}`\n\
            Time: `{}`",
            title,
            get_bot_name(),
            symbol,
            side,
            qty.abs(),
            trade_amount_usd,
            qty.abs() * executed_entry_avg_px,
            executed_entry_avg_px,
            executed_exit_avg_px,
            realized,
            pnl_percent,
            trade_duration,
            entry_signal_price,
            exit_signal_price,
            strategy,
            close_cid_display,
            close_oid_display,
            get_local_timestamp(),
        );


        send_remote_message(
            &self.cfg,
            message,
            Some(get_slack_trades_channel_id()),
            Some(get_discord_trades_channel_id()),
        );


        // ============================================================
        // REMOVE CLOSED TRADE
        // ============================================================

        self.trades.remove(
            &id
        );
    }
}