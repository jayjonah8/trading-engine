#![allow(dead_code)]

use std::sync::Arc;

use chrono::Utc;
use ibapi::Client;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use crate::{
    core::execution::{ExecCommand, ExecEvent},
    exchange::ibkr::{
        commands::{
            cancel_order::handle_cancel_order,
            submit_order::handle_submit_order,
        },
        correlation::IbkrOrderCorrelation,
        execution_ws::runtime::start_execution_ws,
        mapper::order_update::handle_order_update,
        submission_result::IbkrSubmissionResult,
    },
    shared::print_log,
};


//                          Engine
//                            │
//                       ExecCommand
//                            │
//                            ▼
//                     run_ibkr_adapter
//                            │
//              ┌─────────────┼─────────────┐
//              │             │             │
//              ▼             ▼             ▼
//           Submit         Cancel      execution_ws
//              │             │             │
//              └────── Arc<Client> ─────────┘
//                            │
//                            ▼
//                      TWS / Gateway
//                            │
//                            ▼
//                           IBKR
//
// IBKR updates
//      │
//      ▼
// order_update_stream
//      │
//      ▼
// handle_order_update
//      │
//      ▼
// ExecEvent
//      │
//      ▼
// Engine


// ============================================================
// RUN IBKR ADAPTER
// ============================================================

pub async fn run_ibkr_adapter(
    address: String,
    client_id: i32,
    mut rx_cmd: UnboundedReceiver<ExecCommand>,
    tx_evt: UnboundedSender<ExecEvent>,
    debug_mode: bool,
) {
    // ============================================================
    // CONNECT
    // ============================================================

    let client = match Client::connect(&address, client_id).await {
        Ok(client) => Arc::new(client),

        Err(error) => {
            let _ = tx_evt.send(ExecEvent::Error {
                order_id: None,
                message: format!("IBKR connection failed: {:?}", error),
                ts_ms: Utc::now().timestamp_millis(),
            });

            return;
        }
    };

    print_log(
        debug_mode,
        true,
        format!("[IBKR ADAPTER] CONNECTED | address={} | client_id={}", address, client_id),
    );


    // ============================================================
    // EXECUTION MONITORING
    // ============================================================

    let mut execution_ws = start_execution_ws(Arc::clone(&client), debug_mode);


    // ============================================================
    // SUBMISSION RESULTS
    // ============================================================

    let (tx_submission_result, mut rx_submission_result) =
        mpsc::unbounded_channel::<IbkrSubmissionResult>();


    // ============================================================
    // CORRELATION
    // ============================================================

    let mut correlation = IbkrOrderCorrelation::new();


    // ============================================================
    // ADAPTER EVENT LOOP
    // ============================================================

    loop {
        tokio::select! {
            // ----------------------------------------------------
            // ENGINE COMMAND
            // ----------------------------------------------------

            command = rx_cmd.recv() => {
                let Some(command) = command else {
                    break;
                };

                match command {
                    ExecCommand::SubmitOrder { order } => {
                        let client = Arc::clone(&client);
                        let tx_evt = tx_evt.clone();
                        let tx_submission_result = tx_submission_result.clone();

                        tokio::spawn(async move {
                            handle_submit_order(
                                &client,
                                &tx_evt,
                                &tx_submission_result,
                                order,
                                debug_mode,
                            ).await;
                        });
                    }


                    // ------------------------------------------------
                    // CANCEL ORDER
                    // ------------------------------------------------

                    ExecCommand::CancelOrder { venue: _, order_id } => {
                        let Some(order_ref) = correlation.get_by_order_id(order_id).copied() else {
                            let _ = tx_evt.send(ExecEvent::Error {
                                order_id: Some(order_id),
                                message: "IBKR cancellation failed: order correlation not found".to_string(),
                                ts_ms: Utc::now().timestamp_millis(),
                            });

                            continue;
                        };

                        let client = Arc::clone(&client);
                        let tx_evt = tx_evt.clone();

                        tokio::spawn(async move {
                            handle_cancel_order(
                                &client,
                                &tx_evt,
                                order_id,
                                order_ref.ibkr_order_id,
                                debug_mode,
                            ).await;
                        });
                    }


                    // ------------------------------------------------
                    // REPLACE ORDER
                    // ------------------------------------------------

                    ExecCommand::ReplaceOrder { venue: _, order_id, .. } => {
                        print_log(
                            debug_mode,
                            true,
                            format!("[IBKR ADAPTER] REPLACE NOT IMPLEMENTED | order_id={}", order_id),
                        );
                    }
                }
            }


            // ----------------------------------------------------
            // SUBMISSION RESULT
            // ----------------------------------------------------

            result = rx_submission_result.recv() => {
                if let Some(result) = result {
                    correlation.insert(result.order_id, result.ibkr_order_id);

                    print_log(
                        debug_mode,
                        true,
                        format!(
                            "[IBKR ADAPTER] ORDER CORRELATED | order_id={} | ibkr_order_id={}",
                            result.order_id, result.ibkr_order_id,
                        ),
                    );
                }
            }


            // ----------------------------------------------------
            // IBKR ORDER UPDATE
            // ----------------------------------------------------

            item = execution_ws.raw_rx.recv() => {
                let Some(item) = item else {
                    break;
                };

                handle_order_update(
                    item,
                    &correlation,
                    &tx_evt,
                    debug_mode,
                );
            }
        }
    }


    // ============================================================
    // SHUTDOWN
    // ============================================================

    client.disconnect().await;

    print_log(
        debug_mode,
        true,
        "[IBKR ADAPTER] DISCONNECTED".to_string(),
    );
}

