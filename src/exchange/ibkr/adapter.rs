#![allow(dead_code)]

use std::{
    collections::HashMap,
    sync::Arc,
};

use chrono::Utc;
use ibapi::{
    orders::OrderUpdate,
    register_timezone_alias,
    subscriptions::SubscriptionItem,
    Client,
};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use crate::{
    core::execution::{ExecCommand, ExecEvent, Order},
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


// ============================================================================
// IBKR ADAPTER FLOW
// ============================================================================
//
//                              Engine
//                                │
//                           ExecCommand
//                                │
//                                ▼
//                         run_ibkr_adapter
//                                │
//                 ┌──────────────┼──────────────┐
//                 │              │              │
//                 ▼              ▼              ▼
//          Submit Queue        Cancel       execution_ws
//                 │              │              │
//                 ▼              │              │
//       Submission Worker        │              │
//                 │              │              │
//                 └─────── Arc<Client> ─────────┘
//                                │
//                                ▼
//                          TWS / Gateway
//                                │
//                                ▼
//                               IBKR
//
// ORDER SUBMISSION:
//
// IBKR submissions are intentionally serialized through one submission
// worker. The engine remains fully concurrent and any number of orders may
// remain active at IBKR simultaneously.
//
// Only the actual submit() operations are serialized:
//
//     Order A ─┐
//     Order B ─┼─> submission queue -> submit A -> submit B -> submit C
//     Order C ─┘
//
// This prevents concurrent IBKR submissions from racing native order-ID
// allocation while leaving cancellation and execution processing concurrent.
//
// IBKR asynchronous updates:
//
//      IBKR
//        │
//        ▼
// order_update_stream
//        │
//        ▼
//     raw_rx
//        │
//        ▼
//  correlation exists?
//        │
//      ┌─┴─┐
//     YES  NO
//      │    │
//      │    ▼
//      │  pending_updates
//      │    │
//      │    │ wait for IbkrSubmissionResult
//      │    ▼
//      │  correlation.insert(...)
//      │    │
//      │    ▼
//      │  replay buffered updates
//      │    │
//      └────┘
//        │
//        ▼
// handle_order_update
//        │
//        ▼
//    ExecEvent
//        │
//        ▼
//      Engine
//
// WHY BUFFERING EXISTS:
//
// `submit().await` and the IBKR order-update stream are asynchronous.
//
// A fast IBKR update can theoretically reach this event loop before the
// submission worker's `IbkrSubmissionResult` has been processed:
//
//     ExecutionData(ibkr_order_id=7)
//                  ↓
//         correlation not present yet
//                  ↓
//             BUFFER UPDATE
//
// Later:
//
//     IbkrSubmissionResult {
//         order_id: OrderId(...),
//         ibkr_order_id: 7,
//     }
//                  ↓
//         correlation.insert(...)
//                  ↓
//        replay buffered update
//                  ↓
//        canonical Fill preserved
//
// Without this buffer, a fast fill could be discarded simply because the
// two asynchronous channels were scheduled in an unfavorable order.
//
// ============================================================================


// ============================================================
// RUN IBKR ADAPTER
// ============================================================

pub async fn run_ibkr_adapter(
    address: String,
    client_id: i32,
    timezone_name: String,
    timezone_alias: String,
    mut rx_cmd: UnboundedReceiver<ExecCommand>,
    tx_evt: UnboundedSender<ExecEvent>,
    debug_mode: bool,
) {
    // ============================================================
    // TIMEZONE
    // ============================================================

    // TWS / IB Gateway may report a timezone name that is not directly
    // recognized by the timezone parser used by the IBKR SDK.
    //
    // The alias must be registered BEFORE Client::connect() because the
    // connection handshake itself can contain the configured TWS timezone.
    //
    // Keep this configurable rather than hardcoding a particular timezone.
    //
    // Current paper TWS example:
    //
    //     timezone_name  = "Hawaii-Aleutian Standard Time"
    //     timezone_alias = "Pacific/Honolulu"
    //
    // TODO:
    // Move these values into canonical IBKR configuration once exchange
    // configuration is centralized.
    register_timezone_alias(&timezone_name, &timezone_alias);


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
        format!(
            "[IBKR ADAPTER] CONNECTED | address={} | client_id={} | timezone={} -> {}",
            address, client_id, timezone_name, timezone_alias,
        ),
    );


    // ============================================================
    // EXECUTION MONITORING
    // ============================================================

    // The execution WS uses the same persistent IBKR Client as command
    // submission/cancellation. It forwards raw IBKR order updates back into
    // this authoritative adapter loop.
    let mut execution_ws = start_execution_ws(Arc::clone(&client), debug_mode);


    // ============================================================
    // SUBMISSION RESULTS
    // ============================================================

    // After IBKR's submit() returns its native order ID, the submission
    // worker sends an IbkrSubmissionResult here so this adapter can establish:
    //
    //     canonical OrderId <-> IBKR order ID
    //
    // Correlation remains owned exclusively by this adapter loop.
    let (tx_submission_result, mut rx_submission_result) =
        mpsc::unbounded_channel::<IbkrSubmissionResult>();


    // ============================================================
    // SERIALIZED ORDER SUBMISSION
    // ============================================================

    // IBKR order submission is intentionally processed by exactly one worker.
    //
    // This does NOT limit the number of simultaneously active IBKR orders.
    // It only prevents multiple submit().await calls from racing each other
    // while IBKR native order IDs are allocated.
    //
    // Cancellation and execution processing remain independent/concurrent.
    let (tx_submit, mut rx_submit) = mpsc::unbounded_channel::<Order>();

    let submit_client = Arc::clone(&client);
    let submit_tx_evt = tx_evt.clone();
    let submit_tx_submission_result = tx_submission_result.clone();

    tokio::spawn(async move {
        while let Some(order) = rx_submit.recv().await {
            handle_submit_order(
                &submit_client,
                &submit_tx_evt,
                &submit_tx_submission_result,
                order,
                debug_mode,
            ).await;
        }

        print_log(
            debug_mode,
            true,
            "[IBKR ADAPTER] SUBMISSION WORKER STOPPED".to_string(),
        );
    });


    // ============================================================
    // CORRELATION
    // ============================================================

    let mut correlation = IbkrOrderCorrelation::new();


    // ============================================================
    // PENDING UNCORRELATED UPDATES
    // ============================================================

    // Temporary holding area for IBKR updates that arrive before their
    // IbkrSubmissionResult has established canonical correlation.
    //
    // Key:
    //     IBKR native order ID
    //
    // Value:
    //     Raw updates received for that order, preserved in arrival order.
    //
    // Once correlation is established, all buffered updates for that native
    // order ID are replayed through handle_order_update().
    //
    // WARNING:
    // IBKR may also report orders not created by this engine process, such as
    // manual/external orders. Those orders will never receive an
    // IbkrSubmissionResult from this adapter.
    //
    // TODO:
    // Before production hardening, add a bounded/expiry policy so permanently
    // uncorrelated external updates cannot remain in memory indefinitely.
    let mut pending_updates: HashMap<i32, Vec<SubscriptionItem<OrderUpdate>>> = HashMap::new();


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
                    // ------------------------------------------------
                    // SUBMIT ORDER
                    // ------------------------------------------------

                    ExecCommand::SubmitOrder { order } => {
                        let order_id = order.id;

                        // Queue the order for the single IBKR submission worker.
                        //
                        // This keeps the authoritative adapter loop responsive
                        // while guaranteeing submit() calls are serialized.
                        if tx_submit.send(order).is_err() {
                            let _ = tx_evt.send(ExecEvent::Error {
                                order_id: Some(order_id),
                                message: "IBKR submission worker closed".to_string(),
                                ts_ms: Utc::now().timestamp_millis(),
                            });
                        }
                    }


                    // ------------------------------------------------
                    // CANCEL ORDER
                    // ------------------------------------------------

                    ExecCommand::CancelOrder { venue: _, order_id } => {
                        // Cancellation requires IBKR's native order ID.
                        // Resolve it from adapter-owned correlation first.
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
                        // TODO:
                        // Implement IBKR order replacement only after the
                        // primary submit/cancel/fill lifecycle is hardened.
                        print_log(
                            debug_mode,
                            true,
                            format!(
                                "[IBKR ADAPTER] REPLACE NOT IMPLEMENTED | order_id={}",
                                order_id,
                            ),
                        );
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
                            "[IBKR ADAPTER] unexpected backtest barrier received",
                        );
                    }
                }
            }


            // ----------------------------------------------------
            // SUBMISSION RESULT / ESTABLISH CORRELATION
            // ----------------------------------------------------

            result = rx_submission_result.recv() => {
                if let Some(result) = result {
                    // ------------------------------------------------
                    // 1. ESTABLISH CORRELATION
                    // ------------------------------------------------

                    correlation.insert(result.order_id, result.ibkr_order_id);

                    print_log(
                        debug_mode,
                        true,
                        format!(
                            "[IBKR ADAPTER] ORDER CORRELATED | order_id={} | ibkr_order_id={}",
                            result.order_id, result.ibkr_order_id,
                        ),
                    );


                    // ------------------------------------------------
                    // 2. REPLAY EARLY IBKR UPDATES
                    // ------------------------------------------------

                    // If IBKR was faster than the submission-result channel,
                    // updates for this native order ID are waiting here.
                    //
                    // Removing the Vec also guarantees each buffered update is
                    // replayed only once from this buffer.
                    if let Some(updates) = pending_updates.remove(&result.ibkr_order_id) {
                        print_log(
                            debug_mode,
                            true,
                            format!(
                                "[IBKR ADAPTER] REPLAYING BUFFERED UPDATES | order_id={} | ibkr_order_id={} | count={}",
                                result.order_id,
                                result.ibkr_order_id,
                                updates.len(),
                            ),
                        );

                        for item in updates {
                            handle_order_update(
                                item,
                                &correlation,
                                &tx_evt,
                                debug_mode,
                            );
                        }
                    }
                }
            }


            // ----------------------------------------------------
            // IBKR ORDER UPDATE
            // ----------------------------------------------------

            item = execution_ws.raw_rx.recv() => {
                let Some(item) = item else {
                    break;
                };


                // ------------------------------------------------
                // 1. EXTRACT NATIVE ORDER ID WHEN AVAILABLE
                // ------------------------------------------------

                if let Some(ibkr_order_id) = get_ibkr_order_id(&item) {
                    // ------------------------------------------------
                    // 2. CORRELATION NOT READY -> BUFFER
                    // ------------------------------------------------

                    if correlation.get_by_ibkr_order_id(ibkr_order_id).is_none() {
                        print_log(
                            debug_mode,
                            true,
                            format!(
                                "[IBKR ADAPTER] BUFFERING UNCORRELATED UPDATE | ibkr_order_id={}",
                                ibkr_order_id,
                            ),
                        );

                        // Preserve arrival order for this IBKR order.
                        //
                        // The update will be replayed when the corresponding
                        // IbkrSubmissionResult establishes correlation.
                        pending_updates.entry(ibkr_order_id).or_default().push(item);

                        continue;
                    }
                }


                // ------------------------------------------------
                // 3. CORRELATED UPDATE -> MAP IMMEDIATELY
                // ------------------------------------------------

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


// ============================================================
// GET IBKR ORDER ID
// ============================================================

/// Extracts the native IBKR order ID from an order-specific stream item.
///
/// This is used only by the adapter to determine whether canonical
/// correlation exists before sending the item into the mapper.
///
/// CommissionReport does not directly expose the order ID in the currently
/// used mapping path, so it passes directly to the mapper.
fn get_ibkr_order_id(
    item: &SubscriptionItem<OrderUpdate>,
) -> Option<i32> {
    match item {
        SubscriptionItem::Data(OrderUpdate::OpenOrder(data)) => Some(data.order_id),
        SubscriptionItem::Data(OrderUpdate::OrderStatus(status)) => Some(status.order_id),
        SubscriptionItem::Data(OrderUpdate::ExecutionData(data)) => Some(data.execution.order_id),
        SubscriptionItem::Data(OrderUpdate::CommissionReport(_)) => None,
        SubscriptionItem::Notice(notice) => notice.request_id,
    }
}




