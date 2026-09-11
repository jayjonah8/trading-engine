#![allow(dead_code)]

use serde_json::Value;

use tokio::sync::{
    mpsc::{
        self,
        UnboundedReceiver,
        UnboundedSender,
    },
    watch,
};

use crate::{
    core::{
        config::Config,

        execution::{
            ExecCommand,
            ExecEvent,
        },
    },

    exchange::alpaca::{
        trade_updates_ws::run_alpaca_trade_updates_ws,

        types::{
            AlpacaConfig,
            TradeUpdatesConnectionState,
        },

        utils::get_alpaca_config,
    },

    notifications::{
        env_helpers::{
            get_discord_error_channel_id,
            get_slack_error_channel_id,
        },
        send_remote_message,
    },

    shared::utils::{
        get_bot_name,
        get_local_timestamp,
    },
};


// ============================================================
// RUN ALPACA EXCHANGE ADAPTER
// ============================================================

/// Runs the Alpaca live execution adapter.
///
/// This adapter is the boundary between the venue-independent engine and
/// Alpaca's trading APIs.
///
/// The adapter owns execution orchestration while lower-level modules own
/// specific Alpaca responsibilities:
///
/// - `orders.rs` performs Alpaca REST order operations.
/// - `trade_updates_ws.rs` owns the execution WebSocket lifecycle.
/// - `trade_update_mapper.rs` will translate Alpaca updates into `ExecEvent`s.
/// - `command_handler.rs` will translate `ExecCommand`s into Alpaca actions.
///
/// The execution `trade_updates` WebSocket is independent from Alpaca's
/// market-data WebSocket under `data/live/alpaca`.
///
/// TODO:
/// Wire the finalized canonical `ExecCommand` and `ExecEvent` order lifecycle
/// once the new engine order implementation is ready.
///
/// TODO:
/// Wire raw Alpaca `trade_updates` through `trade_update_mapper.rs` once the
/// canonical fill/order event model is finalized.
pub async fn run_alpaca_exchange_adapter(
    cfg: Config,
    mut rx_cmd: UnboundedReceiver<ExecCommand>,
    tx_evt: UnboundedSender<ExecEvent>,
) {

    // ============================================================
    // ALPACA CONFIG
    // ============================================================

    let alpaca_config =
        match get_alpaca_config() {
            Ok(config) => config,

            Err(error) => {
                eprintln!(
                    "[ALPACA ADAPTER] failed to load config | error={}",
                    error
                );

                let remote_message =
                    format!(
                        "🚨 ALPACA ADAPTER ERROR – `{}`\n\
                        Failed to load Alpaca configuration\n\
                        Adapter: `AlpacaExchangeAdapter`\n\
                        Error: `{}`\n\
                        Time: `{}`",
                        get_bot_name(),
                        error,
                        get_local_timestamp(),
                    );

                send_remote_message(
                    &cfg,
                    remote_message,
                    Some(get_slack_error_channel_id()),
                    Some(get_discord_error_channel_id()),
                );

                return;
            }
        };


    println!(
        "[ALPACA ADAPTER] started | mode={}",
        if alpaca_config.paper_trading {
            "PAPER"
        } else {
            "LIVE"
        }
    );


    // ============================================================
    // RAW TRADE UPDATES CHANNEL
    // ============================================================

    // trade_updates_ws.rs owns WebSocket transport and publishes raw Alpaca
    // execution messages into this channel.
    //
    // The adapter will eventually send these messages through
    // trade_update_mapper.rs to produce canonical ExecEvents.
    let (
        tx_trade_updates,
        mut rx_trade_updates,
    ) =
        mpsc::unbounded_channel::<Value>();


    // ============================================================
    // TRADE UPDATES CONNECTION STATE
    // ============================================================

    // Explicitly tracks execution-monitoring availability.
    //
    // This restores an important safety property from the older adapter:
    // new live orders should not be submitted while Alpaca trade_updates
    // monitoring is unavailable.
    let (
        tx_trade_updates_state,
        rx_trade_updates_state,
    ) =
        watch::channel(
            TradeUpdatesConnectionState::Disconnected
        );


    // ============================================================
    // START TRADE UPDATES WEBSOCKET
    // ============================================================

    let ws_cfg =
        cfg.clone();

    let ws_alpaca_config =
        alpaca_config.clone();


    tokio::spawn(
        async move {
            if let Err(error) =
                run_alpaca_trade_updates_ws(
                    ws_cfg,
                    ws_alpaca_config,
                    tx_trade_updates,
                    tx_trade_updates_state,
                )
                .await
            {
                eprintln!(
                    "[ALPACA ADAPTER] trade_updates supervisor stopped | error={}",
                    error
                );
            }
        }
    );


    // ============================================================
    // ADAPTER EVENT LOOP
    // ============================================================

    loop {
        tokio::select! {

            // ====================================================
            // ENGINE COMMAND
            // ====================================================

            command =
                rx_cmd.recv() =>
            {
                let Some(command) =
                    command
                else {
                    println!(
                        "[ALPACA ADAPTER] command channel closed"
                    );

                    break;
                };


                handle_engine_command(
                    command,
                    &alpaca_config,
                    &rx_trade_updates_state,
                    &tx_evt,
                )
                .await;
            }


            // ====================================================
            // ALPACA TRADE UPDATE
            // ====================================================

            update =
                rx_trade_updates.recv() =>
            {
                let Some(update) =
                    update
                else {
                    eprintln!(
                        "[ALPACA ADAPTER] trade_updates channel closed"
                    );

                    break;
                };


                handle_trade_update(
                    update,
                    &tx_evt,
                );
            }
        }
    }


    // ============================================================
    // ADAPTER SHUTDOWN
    // ============================================================

    println!(
        "[ALPACA ADAPTER] shutting down"
    );
}


// ============================================================
// HANDLE ENGINE COMMAND
// ============================================================

/// Temporary command-handler boundary for the new engine.
///
/// TODO:
/// Replace this boilerplate with the finalized Alpaca command handler once
/// the canonical engine Order -> ExecCommand execution path is implemented.
///
/// Before submitting any live order, the final implementation must verify
/// that `trade_updates` execution monitoring is connected.
async fn handle_engine_command(
    command: ExecCommand,
    _alpaca_config: &AlpacaConfig,
    trade_updates_state: &watch::Receiver<TradeUpdatesConnectionState>,
    _tx_evt: &UnboundedSender<ExecEvent>,
) {
    // ============================================================
    // REQUIRE EXECUTION MONITORING
    // ============================================================

    if *trade_updates_state.borrow()
        != TradeUpdatesConnectionState::Connected
    {
        eprintln!(
            "[ALPACA ADAPTER] execution command blocked | trade_updates unavailable"
        );

        // TODO:
        // Emit the finalized canonical adapter-unavailable / rejected
        // ExecEvent once that engine event shape has been completed.
        //
        // Never submit a new live Alpaca order while execution monitoring
        // is unavailable because the order could fill without the engine
        // receiving its execution update.

        return;
    }


    // ============================================================
    // TODO: FINAL COMMAND HANDLING
    // ============================================================

    // TODO:
    // Forward the canonical command into command_handler.rs once the new
    // ExecCommand order model is finalized.

    let _ = command;
}


// ============================================================
// HANDLE TRADE UPDATE
// ============================================================

/// Temporary raw Alpaca execution-update boundary.
///
/// TODO:
/// Forward this message into `trade_update_mapper.rs` once the canonical
/// ExecEvent fill/order lifecycle has been finalized.
fn handle_trade_update(
    update: Value,
    _tx_evt: &UnboundedSender<ExecEvent>,
) {
    // Intentionally empty for now.
    //
    // The old bot's proven partial-fill, final-fill, resting, canceled,
    // rejected, expired, execution-ID, cumulative quantity, and average-price
    // handling will be migrated into trade_update_mapper.rs.

    let _ = update;
}