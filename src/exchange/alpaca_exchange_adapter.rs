use std::{
    collections::HashMap,
    time::Duration,
};

use futures_util::StreamExt;

use tokio::sync::mpsc::{
    UnboundedReceiver,
    UnboundedSender,
};

use crate::core::config::Config;

use crate::{
    core::exec::{
        ExecCommand,
        ExecEvent,
        Direction,
    },

    exchange::{
        alpaca_execution_ws::{
            OrderCorrelation,
            OrderKind,
            connect_alpaca_execution_ws,
            handle_trade_update,
            websocket_message_to_json,
        },

        alpaca_orders::{
            close_position,
            place_market_order,
        },

        alpaca_utils::{
            get_alpaca_config,
        },
    },
};

use crate::notifications::{
    send_remote_message,
    env_helpers::{
        get_slack_error_channel_id,
        get_slack_reconnect_error_channel_id,
        get_discord_error_channel_id,
        get_discord_reconnect_error_channel_id,
    },
};

use crate::shared::utils::{
    get_bot_name,
    get_local_timestamp,
};


/// Runs the Alpaca execution adapter.
///
/// The adapter connects the engine's execution commands to Alpaca's REST
/// trading API and translates Alpaca trade_updates WebSocket messages back
/// into generic ExecEvents.
///
/// The trade_updates WebSocket automatically reconnects after unexpected
/// connection loss using exponential backoff. Every reconnect performs a
/// fresh authentication and trade_updates subscription before new engine
/// commands are processed.
///
/// The Alpaca order correlation map is intentionally preserved across
/// reconnects so execution updates for already-submitted orders can still be
/// matched after the WebSocket connection returns.
///
/// New execution commands are not processed while the WebSocket is
/// disconnected. They remain queued in the engine-to-adapter channel until
/// execution monitoring has been restored.
pub async fn run_exchange_adapter(
    cfg: Config,
    mut rx_cmd: UnboundedReceiver<ExecCommand>,
    tx_evt: UnboundedSender<ExecEvent>,
) {

    // ============================================================
    // ALPACA CONFIG
    // ============================================================

    let config =
        match get_alpaca_config() {
            Ok(config) => {
                config
            }

           Err(error) => {
                let message = format!(
                    "🚨 ALPACA ADAPTER ERROR – `{}`\n\
                    Failed to load Alpaca config\n\
                    Adapter: `AlpacaExchangeAdapter`\n\
                    Error: `{}`\n\
                    Time: `{}`",
                    get_bot_name(),
                    error,
                    get_local_timestamp(),
                );

                send_remote_message(
                    &cfg,
                    message,
                    Some(get_slack_error_channel_id()),
                    Some(get_discord_error_channel_id()),
                );

                eprintln!(
                    "[ALPACA ADAPTER] Failed to load Alpaca config: {}",
                    error
                );

                return;
            }
        };


    println!(
        "[ADAPTER] Alpaca execution adapter started. mode={}",
        if config.paper_trading {
            "PAPER"
        } else {
            "LIVE"
        }
    );


    // ============================================================
    // ORDER CORRELATION
    // ============================================================

    // Alpaca order ID -> engine correlation information.
    //
    // IMPORTANT:
    // This map remains OUTSIDE the WebSocket reconnect loop.
    //
    // If the trade_updates socket disconnects while an order is still
    // pending, reconnecting must not erase our ability to recognize that
    // order when another execution update arrives.
    let mut order_map:
        HashMap<String, OrderCorrelation> =
        HashMap::new();


    // ============================================================
    // RECONNECT CONFIG
    // ============================================================

    let initial_backoff_secs: u64 =
        2;

    let max_backoff_secs: u64 =
        30;

    let mut backoff_secs =
        initial_backoff_secs;

    let mut has_connected_once =
        false;


    // ============================================================
    // EXECUTION WEBSOCKET SUPERVISOR
    // ============================================================

    'supervisor: loop {

        // ========================================================
        // CONNECT / AUTHENTICATE / SUBSCRIBE
        // ========================================================

        println!(
            "[ADAPTER] Connecting Alpaca trade_updates WebSocket..."
        );


        let mut ws_stream =
            match connect_alpaca_execution_ws(
                &config
            )
            .await
            {
                Ok(ws_stream) => {
                    ws_stream
                }

                Err(error) => {
                    eprintln!(
                        "[ADAPTER] Alpaca trade_updates connection failed | error={} | retry={}s",
                        error,
                        backoff_secs
                    );

                    let message = format!(
                        "🚨 ALPACA ADAPTER ERROR – `{}`\n\
                        WebSocket connection failed\n\
                        Adapter: `AlpacaExchangeAdapter`\n\
                        Stream: `trade_updates`\n\
                        Error: `{}`\n\
                        Retry: `{}s`\n\
                        Time: `{}`",
                        get_bot_name(),
                        error,
                        backoff_secs,
                        get_local_timestamp(),
                    );

                    send_remote_message(
                        &cfg,
                        message,
                        Some(get_slack_reconnect_error_channel_id()),
                        Some(get_discord_reconnect_error_channel_id()),
                    );

                    tokio::time::sleep(
                        Duration::from_secs(
                            backoff_secs
                        )
                    )
                    .await;


                    backoff_secs =
                        (
                            backoff_secs * 2
                        )
                        .min(
                            max_backoff_secs
                        );


                    continue;
                }
            };


        // ========================================================
        // CONNECTION ESTABLISHED
        // ========================================================

        if has_connected_once {
            println!(
                "[ADAPTER] Alpaca trade_updates WebSocket reconnected."
            );

            let remote_message = format!(
                "✅ ALPACA ADAPTER RECONNECTED – `{}`\n\
                Execution WebSocket connection restored\n\
                Adapter: `AlpacaExchangeAdapter`\n\
                Stream: `trade_updates`\n\
                Status: `Connected and subscribed`\n\
                Time: `{}`",
                get_bot_name(),
                get_local_timestamp(),
            );

            send_remote_message(
                &cfg,
                remote_message,
                Some(get_slack_reconnect_error_channel_id()),
                Some(get_discord_reconnect_error_channel_id()),
            );
        } else {
            println!(
                "[ADAPTER] Alpaca trade_updates WebSocket connected and subscribed."
            );

            has_connected_once =
                true;
        }


        backoff_secs =
            initial_backoff_secs;


        // ========================================================
        // ADAPTER EVENT LOOP
        // ========================================================

        loop {
            tokio::select! {

                // ====================================================
                // ENGINE COMMAND
                // ====================================================

                command =
                    rx_cmd.recv() =>
                {
                    let Some(cmd) =
                        command
                    else {
                        break 'supervisor;
                    };


                    match cmd {

                        // ============================================
                        // PLACE ORDER
                        // ============================================

                        ExecCommand::Place {
                            cid,
                            symbol,
                            side,
                            notional,
                        } => {
                            let alpaca_side =
                                match side {
                                    Direction::Long => {
                                        "buy"
                                    }

                                    Direction::Short => {
                                        "sell"
                                    }
                                };


                            match place_market_order(
                                &config,
                                &cid,
                                &symbol,
                                alpaca_side,
                                notional,
                            )
                            .await
                            {
                                Ok(order_id) => {
                                    // Store correlation immediately after Alpaca
                                    // returns the order ID so future execution
                                    // updates can be mapped back to this command.
                                    order_map.insert(
                                        order_id.clone(),
                                        OrderCorrelation {
                                            cid:
                                                cid.clone(),

                                            symbol:
                                                symbol.clone(),

                                            kind:
                                                OrderKind::Open,
                                        },
                                    );


                                    // No adapter log here because on_exec_event()
                                    // already logs the acknowledgement.
                                    let _ =
                                        tx_evt.send(
                                            ExecEvent::Ack {
                                                cid,

                                                symbol,

                                                oid:
                                                    order_id,
                                            }
                                        );
                                }


                                Err(error) => {
                                    // No adapter error print / remote alert here.
                                    //
                                    // ExecEvent::Error is handled centrally by
                                    // Engine::on_exec_event(), which owns logging
                                    // and the correlated execution-error alert.
                                    let _ =
                                        tx_evt.send(
                                            ExecEvent::Error {
                                                cid,

                                                symbol,

                                                message:
                                                    error.to_string(),
                                            }
                                        );
                                }
                            }
                        }


                        // ============================================
                        // CLOSE POSITION
                        // ============================================

                        ExecCommand::Close {
                            cid,
                            symbol,
                            qty,
                        } => {
                            match close_position(
                                &config,
                                &symbol,
                                qty,
                            )
                            .await
                            {
                                Ok(order_id) => {
                                    order_map.insert(
                                        order_id.clone(),
                                        OrderCorrelation {
                                            cid:
                                                cid.clone(),

                                            symbol:
                                                symbol.clone(),

                                            kind:
                                                OrderKind::Close,
                                        },
                                    );


                                    // No adapter log here because on_exec_event()
                                    // already logs the acknowledgement.
                                    let _ =
                                        tx_evt.send(
                                            ExecEvent::Ack {
                                                cid,

                                                symbol,

                                                oid:
                                                    order_id,
                                            }
                                        );
                                }


                                Err(error) => {
                                    // Engine::on_exec_event() owns execution-error
                                    // logging and remote-alert handling.
                                    let _ =
                                        tx_evt.send(
                                            ExecEvent::Error {
                                                cid,

                                                symbol,

                                                message:
                                                    error.to_string(),
                                            }
                                        );
                                }
                            }
                        }
                    }
                }


                // ====================================================
                // ALPACA TRADE UPDATE
                // ====================================================

                message =
                    ws_stream.next() =>
                {
                    let Some(message) =
                        message
                    else {
                        eprintln!(
                            "[ADAPTER] Alpaca trade_updates WebSocket disconnected."
                        );

                        break;
                    };


                    match message {
                        Ok(message) => {
                            let value =
                                match websocket_message_to_json(
                                    message
                                ) {
                                    Ok(
                                        Some(value)
                                    ) => {
                                        value
                                    }


                                    Ok(
                                        None
                                    ) => {
                                        continue;
                                    }


                                    Err(error) => {
                                        // A malformed execution message is important,
                                        // but it does not necessarily mean the socket
                                        // itself is dead.
                                        //
                                        // Keep listening while reporting the failure.
                                        eprintln!(
                                            "[ADAPTER] Failed to process trade_updates message: {}",
                                            error
                                        );

                                        let remote_message = format!(
                                            "🚨 ALPACA ADAPTER ERROR – `{}`\n\
                                            Failed to process trade_updates message\n\
                                            Adapter: `AlpacaExchangeAdapter`\n\
                                            Stream: `trade_updates`\n\
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

                                        continue;
                                    }
                                };


                            handle_trade_update(
                                &cfg,
                                value,
                                &mut order_map,
                                &tx_evt,
                            );
                        }


                        Err(error) => {
                            eprintln!(
                                "[ADAPTER] Alpaca trade_updates WebSocket error: {}",
                                error
                            );

                            let remote_message = format!(
                                "🚨 ALPACA ADAPTER WEBSOCKET ERROR – `{}`\n\
                                WebSocket stream error\n\
                                Adapter: `AlpacaExchangeAdapter`\n\
                                Stream: `trade_updates`\n\
                                Error: `{}`\n\
                                Time: `{}`",
                                get_bot_name(),
                                error,
                                get_local_timestamp(),
                            );

                            send_remote_message(
                                &cfg,
                                remote_message,
                                Some(get_slack_reconnect_error_channel_id()),
                                Some(get_discord_reconnect_error_channel_id()),
                            );

                            break;
                        }
                    }
                }
            }
        }


        // ========================================================
        // EXECUTION WEBSOCKET LOST
        // ========================================================

        // Reaching this point means the trade_updates stream ended or
        // encountered a transport failure.
        //
        // Existing order correlations are intentionally preserved.
        //
        // New ExecCommands remain queued while this reconnect delay and
        // connection startup are occurring. This avoids intentionally
        // submitting new orders while fill monitoring is unavailable.
        eprintln!(
            "[ADAPTER] Alpaca trade_updates disconnected | reconnecting in {}s",
            backoff_secs
        );


        let remote_message = format!(
            "🔌 ALPACA ADAPTER DISCONNECTED – `{}`\n\
            Execution WebSocket connection lost\n\
            Adapter: `AlpacaExchangeAdapter`\n\
            Stream: `trade_updates`\n\
            Action: `Reconnecting`\n\
            Retry: `{}s`\n\
            Warning: `Orders may fill while execution monitoring is disconnected`\n\
            Time: `{}`",
            get_bot_name(),
            backoff_secs,
            get_local_timestamp(),
        );

        send_remote_message(
            &cfg,
            remote_message,
            Some(get_slack_reconnect_error_channel_id()),
            Some(get_discord_reconnect_error_channel_id()),
        );


        tokio::time::sleep(
            Duration::from_secs(
                backoff_secs
            )
        )
        .await;


        backoff_secs =
            (
                backoff_secs * 2
            )
            .min(
                max_backoff_secs
            );
    }


    // ============================================================
    // ADAPTER SHUTDOWN
    // ============================================================

    println!(
        "[ADAPTER] Alpaca execution adapter shutting down."
    );
}


// ============================================================================
// ALPACA EXECUTION ADAPTER ARCHITECTURE
// ============================================================================
//
// This adapter is the boundary between the trading engine and Alpaca.
//
// The ENGINE owns authoritative trading state:
//
//     - PendingOpen / PendingClose
//     - Trade state
//     - realized / unrealized PnL
//     - execution deduplication
//     - position accounting
//
// The ADAPTER owns Alpaca communication:
//
//     - submit orders through REST
//     - close positions through REST
//     - maintain the trade_updates WebSocket
//     - reconnect / re-authenticate / re-subscribe after disconnects
//     - translate Alpaca execution messages into ExecEvent values
//
// The adapter does not decide whether a trade is open or closed.
// It reports Alpaca execution state to the engine, and the engine applies it.
//
// ============================================================================
// ORDER FLOW
// ============================================================================
//
// Open:
//
//     Engine creates PendingOpen
//              |
//              v
//       ExecCommand::Place
//              |
//              v
//         Alpaca REST
//              |
//              v
//        ExecEvent::Ack
//              |
//              v
//      trade_updates WS
//              |
//       partial_fill / fill
//              |
//              v
//     OpenPartialFill / OpenFill
//              |
//              v
//            Engine
//
//
// Close:
//
//     Trade::Open
//          |
//          v
//     Trade::Closing + PendingClose
//          |
//          v
//     ExecCommand::Close
//          |
//          v
//       Alpaca REST
//          |
//          v
//     ExecEvent::Ack
//          |
//          v
//     trade_updates WS
//          |
//       partial_fill / fill
//          |
//          v
//     ClosePartialFill / CloseFill
//          |
//          v
//            Engine
//
// ============================================================================
// REST ACK IS NOT A FILL
// ============================================================================
//
// A successful REST response means Alpaca accepted / created the order.
//
// It does NOT mean the order executed.
//
// Ack provides:
//
//     cid = engine client order ID
//     oid = Alpaca order ID
//
// Actual execution state always comes from trade_updates.
//
// ============================================================================
// NOTIONAL OPEN ORDERS
// ============================================================================
//
// Live Alpaca entries are submitted using dollar notional.
//
// Example:
//
//     trade_amount_usd = $10,000
//
// Alpaca determines the actual fractional quantity from execution prices.
//
// Therefore:
//
//     requested_qty = estimate only
//     filled_qty    = authoritative executed quantity
//     avg_fill_px   = authoritative average execution price
//
// ============================================================================
// INDIVIDUAL VS CUMULATIVE FILL DATA
// ============================================================================
//
// Alpaca provides both individual execution values:
//
//     fill_px = data.price
//     fill_sz = data.qty
//
// and cumulative order values:
//
//     avg_px   = data.order.filled_avg_price
//     total_sz = data.order.filled_qty
//
// Example:
//
//     EVENT               fill_sz         total_sz
//     ------------------------------------------------
//     partial_fill        38               38
//     partial_fill         2               40
//     partial_fill        74              114
//     fill                22              136
//
// Because total_sz is already cumulative, the engine must use:
//
//     pending.filled_qty = total_sz;
//
// NOT:
//
//     pending.filled_qty += total_sz;
//
// ============================================================================
// EXECUTION ID / DUPLICATE PROTECTION
// ============================================================================
//
// Every fill contains an execution_id.
//
// PendingOpen and PendingClose keep:
//
//     HashSet<String> execution_ids
//
// If the same execution_id is received again, the engine ignores it.
//
// This protects execution accounting from duplicate WebSocket events and is
// especially important around reconnects.
//
// ============================================================================
// ORDER CORRELATION
// ============================================================================
//
// The adapter temporarily maintains:
//
//     HashMap<String, OrderCorrelation>
//
// keyed by Alpaca order ID:
//
//     oid -> {
//         cid,
//         symbol,
//         kind: Open | Close,
//     }
//
// This map only translates Alpaca order updates into the correct ExecEvent.
//
// It is NOT authoritative trading state.
//
// IMPORTANT:
// The correlation map survives trade_updates WebSocket reconnects so pending
// Alpaca orders can still be recognized after the socket reconnects.
//
// Final fills and terminal order failures remove their correlation entry.
//
// ============================================================================
// TRADE_UPDATES WEBSOCKET RECONNECT
// ============================================================================
//
// The execution WebSocket automatically reconnects after:
//
//     - transport errors
//     - unexpected disconnects
//     - Alpaca closing the socket
//
// Reconnect flow:
//
//     disconnect
//         |
//         v
//     exponential backoff
//         |
//         v
//     new WebSocket connection
//         |
//         v
//     authenticate
//         |
//         v
//     subscribe trade_updates
//         |
//         v
//     resume adapter event loop
//
// New ExecCommands are not processed while the execution WebSocket is down.
// They remain queued until execution monitoring has been restored.
//
// Existing order correlation state is preserved across reconnects.
//
// ============================================================================
// RECONNECT LIMITATION
// ============================================================================
//
// Reconnecting restores future trade_updates, but it does not guarantee that
// every execution that occurred while the socket was disconnected will be
// replayed.
//
// Example:
//
//     order submitted
//          |
//     WebSocket disconnects
//          |
//     order fills while offline
//          |
//     WebSocket reconnects
//
// Complete protection against this case requires later REST reconciliation
// against Alpaca orders / positions.
//
// For now, WebSocket disconnects should be surfaced through an important
// remote Discord/Slack alert so rare missed-fill cases can be inspected.
//
// ============================================================================
// ERROR / LOGGING OWNERSHIP
// ============================================================================
//
// Order-specific failures that can become:
//
//     ExecEvent::Error
//
// should normally be forwarded to Engine::on_exec_event().
//
// The engine owns the main execution-error log and remote alert.
//
// The adapter should directly log / remotely alert infrastructure failures
// that cannot reasonably reach the engine, including:
//
//     - invalid Alpaca configuration
//     - WebSocket connection failure
//     - authentication failure
//     - subscription failure
//     - WebSocket disconnect / transport failure
//     - unknown Alpaca order correlation
//
// This avoids duplicate logs and duplicate remote alerts.
//
// ============================================================================
// CORE RULE
// ============================================================================
//
// REST tells us:
//
//     "Alpaca accepted the order."
//
// trade_updates tells us:
//
//     "This is what actually executed."
//
// The engine decides:
//
//     "This is now the authoritative trade state."
//
// ============================================================================



