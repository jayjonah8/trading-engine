#![allow(dead_code)]

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
            Venue,
        },
    },

    exchange::alpaca::{
        commands::{
            handle_cancel_order,
            handle_replace_order,
            handle_submit_order,
        },

        correlation::AlpacaOrderCorrelation,

        execution_ws::{
            start_execution_ws,
            wait_until_execution_ws_ready,
            ExecutionWsHealth,
        },

        mapper::handle_trade_update,

        submission_result::AlpacaSubmissionResult,

        types::AlpacaConfig,

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
        print_log,
    },
};


// ============================================================
// RUN ALPACA EXCHANGE ADAPTER
// ============================================================

/// Runs the Alpaca live execution adapter.
///
/// The adapter is the authoritative owner of Alpaca-specific execution
/// orchestration.
///
/// Responsibilities:
///
/// - owns Alpaca order correlation
/// - owns execution WebSocket orchestration
/// - routes canonical ExecCommands into Alpaca command handlers
/// - receives private REST submission results
/// - records Alpaca venue order IDs in adapter correlation
/// - routes raw Alpaca trade_updates into the Alpaca mapper
/// - emits canonical ExecEvents back to the Engine
///
/// Correlation intentionally remains adapter-local.
///
/// Spawned REST tasks never mutate `AlpacaOrderCorrelation` directly.
/// Instead, successful submissions return their Alpaca venue order ID through
/// `AlpacaSubmissionResult`. The adapter receives that result inside this
/// event loop and performs the correlation mutation itself.
///
/// This preserves single ownership without requiring:
///
/// - Arc
/// - Mutex
/// - shared mutable correlation state
///
/// The Engine remains authoritative for canonical:
///
/// - Orders
/// - Fills
/// - Positions
/// - Trades
/// - Risk
pub async fn run_alpaca_adapter(
    cfg: Config,
    mut rx_cmd: UnboundedReceiver<ExecCommand>,
    tx_evt: UnboundedSender<ExecEvent>,
) {
    let debug_mode = true;


    // ============================================================
    // ALPACA CONFIG
    // ============================================================

    let alpaca_config = match get_alpaca_config() {
        Ok(config) => config,

        Err(error) => {
            eprintln!(
                "[ALPACA ADAPTER] failed to load config | error={}",
                error
            );

            let remote_message = format!(
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
        if alpaca_config.paper_trading { "PAPER" } else { "LIVE" }
    );


    // ============================================================
    // ORDER CORRELATION
    // ============================================================

    // The adapter exclusively owns Alpaca correlation state.
    //
    // Initial correlation:
    //
    //     OrderId <-> ClientOrderId
    //
    // is registered synchronously before the REST task is spawned.
    //
    // Once Alpaca accepts the REST request and assigns its own order ID,
    // the submission-result channel extends the correlation to:
    //
    //     OrderId
    //        ↕
    //     ClientOrderId
    //        ↕
    //     Alpaca venue_order_id
    //
    // All mutations happen inside this adapter task.
    let mut correlation = AlpacaOrderCorrelation::new();


    // ============================================================
    // SUBMISSION RESULT CHANNEL
    // ============================================================

    // REST submission handlers execute in spawned tasks so network latency
    // never blocks the adapter's main event loop.
    //
    // Those tasks cannot mutate adapter-owned correlation directly.
    //
    // Successful market/limit submissions therefore send:
    //
    //     AlpacaSubmissionResult {
    //         order_id,
    //         venue_order_id,
    //     }
    //
    // back to this task.
    //
    // This is an internal Alpaca adapter channel. It is NOT part of the
    // canonical Engine execution interface.
    let (
        tx_submission_result,
        mut rx_submission_result,
    ) = mpsc::unbounded_channel::<AlpacaSubmissionResult>();


    // ============================================================
    // START EXECUTION WEBSOCKET
    // ============================================================

    let mut execution_ws = start_execution_ws(
        cfg.clone(),
        alpaca_config.clone(),
    );


    // ============================================================
    // WAIT FOR EXECUTION MONITORING
    // ============================================================

    // Do not allow startup to proceed until Alpaca has confirmed:
    //
    // - WebSocket connection
    // - authentication
    // - trade_updates subscription
    //
    // This prevents the adapter from accepting new live orders before
    // execution monitoring is available.
    if let Err(error) = wait_until_execution_ws_ready(
        &mut execution_ws.health_rx
    )
    .await
    {
        eprintln!(
            "[ALPACA ADAPTER] execution websocket readiness failed | error={}",
            error
        );

        return;
    }


    println!(
        "[ALPACA ADAPTER] execution websocket ready | stream=trade_updates"
    );


    // ============================================================
    // ADAPTER EVENT LOOP
    // ============================================================

    // This loop is the single owner of mutable Alpaca correlation state.
    //
    // Three independent event sources are multiplexed here:
    //
    // 1. Engine ExecCommands
    // 2. Alpaca trade_updates
    // 3. REST submission results
    //
    // Because correlation mutations occur here rather than inside spawned
    // tasks, no synchronization primitive is required.
    loop {
        tokio::select! {
            // ====================================================
            // ENGINE COMMAND
            // ====================================================

            command = rx_cmd.recv() => {
                let Some(command) = command else {
                    println!("[ALPACA ADAPTER] command channel closed");
                    break;
                };

                handle_engine_command(
                    command,
                    &alpaca_config,
                    &execution_ws.health_rx,
                    &mut correlation,
                    &tx_evt,
                    &tx_submission_result,
                    debug_mode,
                );
            }


            // ====================================================
            // ALPACA TRADE UPDATE
            // ====================================================

            update = execution_ws.raw_rx.recv() => {
                let Some(update) = update else {
                    eprintln!("[ALPACA ADAPTER] trade_updates channel closed");
                    break;
                };

                handle_trade_update(
                    update,
                    &correlation,
                    &tx_evt,
                    debug_mode,
                );
            }


            // ====================================================
            // REST SUBMISSION RESULT
            // ====================================================

            submission_result = rx_submission_result.recv() => {
                let Some(submission_result) = submission_result else {
                    eprintln!("[ALPACA ADAPTER] submission result channel closed");
                    break;
                };

                handle_submission_result(
                    submission_result,
                    &mut correlation,
                    debug_mode,
                );
            }
        }
    }


    // ============================================================
    // ADAPTER SHUTDOWN
    // ============================================================

    println!("[ALPACA ADAPTER] shutting down");
}


// ============================================================
// HANDLE ENGINE COMMAND
// ============================================================

/// Routes canonical Engine execution commands into Alpaca-specific handlers.
///
/// Submission and replacement are blocked whenever Alpaca execution
/// monitoring is unavailable.
///
/// Cancellation intentionally remains available during a WebSocket outage
/// because canceling outstanding exposure is risk-reducing.
fn handle_engine_command(
    command: ExecCommand,
    alpaca_config: &AlpacaConfig,
    execution_health: &watch::Receiver<ExecutionWsHealth>,
    correlation: &mut AlpacaOrderCorrelation,
    tx_evt: &UnboundedSender<ExecEvent>,
    tx_submission_result: &UnboundedSender<AlpacaSubmissionResult>,
    debug_mode: bool,
) {
    match command {
        // ========================================================
        // SUBMIT ORDER
        // ========================================================

        ExecCommand::SubmitOrder { order } => {
            if order.venue != Venue::Alpaca {
                let _ = tx_evt.send(ExecEvent::Error {
                    order_id: Some(order.id),
                    message: format!("Alpaca adapter received SubmitOrder for venue {:?}", order.venue),
                    ts_ms: chrono::Utc::now().timestamp_millis(),
                });

                return;
            }


            // A new order must not be submitted while trade_updates is down.
            //
            // Otherwise Alpaca could execute the order while the Engine has
            // no live execution-monitoring path.
            if !execution_health.borrow().is_ready() {
                let _ = tx_evt.send(ExecEvent::Error {
                    order_id: Some(order.id),
                    message: "Alpaca SubmitOrder blocked because trade_updates execution monitoring is unavailable".to_string(),
                    ts_ms: chrono::Utc::now().timestamp_millis(),
                });

                return;
            }


            // Register the Engine-generated ClientOrderId BEFORE spawning the
            // REST request.
            //
            // Alpaca may publish a trade_update extremely quickly. Registering
            // first guarantees that an update containing client_order_id can
            // already be resolved back to the canonical OrderId.
            correlation.insert(
                order.id,
                order.client_order_id,
            );


            let alpaca_config = alpaca_config.clone();
            let tx_evt = tx_evt.clone();
            let tx_submission_result = tx_submission_result.clone();


            // REST networking is intentionally moved outside the authoritative
            // adapter loop.
            //
            // The spawned task reports:
            //
            // - canonical acceptance/rejection/error through tx_evt
            // - Alpaca venue-order correlation through tx_submission_result
            tokio::spawn(async move {
                handle_submit_order(
                    &alpaca_config,
                    &tx_evt,
                    &tx_submission_result,
                    order,
                    debug_mode,
                )
                .await;
            });
        }


        // ========================================================
        // CANCEL ORDER
        // ========================================================

        ExecCommand::CancelOrder { venue, order_id } => {
            if venue != Venue::Alpaca {
                let _ = tx_evt.send(ExecEvent::Error {
                    order_id: Some(order_id),
                    message: format!("Alpaca adapter received CancelOrder for venue {:?}", venue),
                    ts_ms: chrono::Utc::now().timestamp_millis(),
                });

                return;
            }


            // Cancellation intentionally remains available while the
            // execution WebSocket is unhealthy because cancellation reduces
            // outstanding execution risk.
            //
            // Alpaca cancellation requires the venue order ID, which should
            // have been attached to correlation after REST acceptance.
            let Some(order_ref) = correlation.get_by_order_id(order_id).cloned() else {
                let _ = tx_evt.send(ExecEvent::Error {
                    order_id: Some(order_id),
                    message: format!("Alpaca cancel correlation missing for order {}", order_id),
                    ts_ms: chrono::Utc::now().timestamp_millis(),
                });

                return;
            };


            let alpaca_config = alpaca_config.clone();
            let tx_evt = tx_evt.clone();


            tokio::spawn(async move {
                handle_cancel_order(
                    &alpaca_config,
                    &tx_evt,
                    order_id,
                    order_ref,
                    debug_mode,
                )
                .await;
            });
        }


        // ========================================================
        // REPLACE ORDER
        // ========================================================

        ExecCommand::ReplaceOrder { venue, order_id, changes } => {
            if venue != Venue::Alpaca {
                let _ = tx_evt.send(ExecEvent::Error {
                    order_id: Some(order_id),
                    message: format!("Alpaca adapter received ReplaceOrder for venue {:?}", venue),
                    ts_ms: chrono::Utc::now().timestamp_millis(),
                });

                return;
            }


            // Replacing an order can change live execution behavior, so keep
            // it blocked while execution monitoring is unavailable.
            if !execution_health.borrow().is_ready() {
                let _ = tx_evt.send(ExecEvent::Error {
                    order_id: Some(order_id),
                    message: "Alpaca ReplaceOrder blocked because trade_updates execution monitoring is unavailable".to_string(),
                    ts_ms: chrono::Utc::now().timestamp_millis(),
                });

                return;
            }


            let alpaca_config = alpaca_config.clone();
            let tx_evt = tx_evt.clone();


            tokio::spawn(async move {
                handle_replace_order(
                    &alpaca_config,
                    &tx_evt,
                    order_id,
                    changes,
                    debug_mode,
                )
                .await;
            });
        }


        // ================================================
        // BACKTEST SYNCHRONIZATION (LIVE ADAPTER)
        // ================================================

        ExecCommand::BacktestBarrier {
            venue: _,
            done,
        } => {
            // Backtest barriers should never reach live adapters.
            // Drop the acknowledgment sender without signaling completion.
            //
            // This ensures an incorrectly routed barrier fails
            // instead of falsely reporting successful synchronization.
            drop(done);

            print_log(
                debug_mode,
                true,
                "[ALPACA ADAPTER] unexpected backtest barrier received",
            );
        }
    }
}


// ============================================================
// HANDLE SUBMISSION RESULT
// ============================================================

/// Applies the Alpaca venue order ID learned by a spawned REST submission
/// task to adapter-owned correlation.
///
/// Before REST acceptance:
///
///     OrderId <-> ClientOrderId
///
/// After REST acceptance:
///
///     OrderId <-> ClientOrderId
///         │
///         └── venue_order_id
///
/// This mutation deliberately happens inside the adapter task rather than
/// inside the spawned HTTP task.
fn handle_submission_result(
    result: AlpacaSubmissionResult,
    correlation: &mut AlpacaOrderCorrelation,
    debug_mode: bool,
) {
    let updated = correlation.set_venue_order_id(
        result.order_id,
        result.venue_order_id.clone(),
    );


    if updated {
        if debug_mode {
            println!(
                "[ALPACA ADAPTER] CORRELATION UPDATED | order_id={} | venue_order_id={}",
                result.order_id,
                result.venue_order_id,
            );
        }

        return;
    }


    eprintln!(
        "[ALPACA ADAPTER] CORRELATION UPDATE FAILED | order_id={} | venue_order_id={}",
        result.order_id,
        result.venue_order_id,
    );
}