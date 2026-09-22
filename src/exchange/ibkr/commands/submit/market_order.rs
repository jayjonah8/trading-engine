#![allow(dead_code)]

use chrono::Utc;
use ibapi::{contracts::Contract, Client};
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    core::execution::{ExecEvent, Order, Side},

    exchange::ibkr::submission_result::IbkrSubmissionResult,

    shared::print_log,
};


// ============================================================
// HANDLE MARKET ORDER
// ============================================================

/// Submits a canonical market order to IBKR.
///
/// IBKR execution behavior is represented by:
///
///     symbol + side + quantity
///
/// IBKR returns its order ID after the request is successfully sent to TWS.
///
/// That IBKR order ID is returned to the authoritative adapter event loop
/// through `IbkrSubmissionResult`, allowing the adapter to maintain:
///
///     Engine OrderId <-> IBKR order ID
///
/// without sharing mutable correlation state with this submission task.
///
/// IMPORTANT:
///
/// Successful `submit()` means the order was sent to TWS. It does not prove
/// broker acceptance, routing, working state, or execution.
///
/// Those facts arrive asynchronously through the IBKR order update stream.
pub async fn handle_market_order(
    client: &Client,
    tx_evt: &UnboundedSender<ExecEvent>,
    tx_submission_result: &UnboundedSender<IbkrSubmissionResult>,
    order: Order,
    debug_mode: bool,
) {
    // ============================================================
    // VALIDATION
    // ============================================================

    if order.post_only {
        let _ = tx_evt.send(ExecEvent::Error {
            order_id: Some(order.id),
            message: "IBKR market orders cannot be post_only".to_string(),
            ts_ms: Utc::now().timestamp_millis(),
        });

        return;
    }

    if !order.quantity.is_finite() || order.quantity <= 0.0 {
        let _ = tx_evt.send(ExecEvent::Error {
            order_id: Some(order.id),
            message: format!("Invalid IBKR market order quantity: {}", order.quantity),
            ts_ms: Utc::now().timestamp_millis(),
        });

        return;
    }


    // ============================================================
    // CONTRACT
    // ============================================================

    let contract = Contract::stock(&order.symbol).build();


    // ============================================================
    // SUBMIT
    // ============================================================

    print_log(
        debug_mode,
        true,
        format!(
            "[IBKR ADAPTER] MARKET SUBMIT | order_id={} | client_order_id={} | purpose={:?} | symbol={} | side={:?} | qty={} | tif={:?} | reduce_only={}",
            order.id,
            order.client_order_id,
            order.purpose,
            order.symbol,
            order.side,
            order.quantity,
            order.time_in_force,
            order.reduce_only,
        ),
    );

    let result = match order.side {
        Side::Buy => client.order(&contract).buy(order.quantity).market().submit().await,
        Side::Sell => client.order(&contract).sell(order.quantity).market().submit().await,
    };


    // ============================================================
    // SUBMISSION RESULT
    // ============================================================

    match result {
        Ok(ibkr_order_id) => {
            let ibkr_order_id = ibkr_order_id.0;

            // ----------------------------------------------------
            // RETURN IBKR ORDER ID TO ADAPTER
            // ----------------------------------------------------

            // This submission task does not own or mutate
            // IbkrOrderCorrelation.
            //
            // Return IBKR's assigned order ID to the authoritative adapter
            // event loop. The adapter will perform:
            //
            //     correlation.insert(order.id, ibkr_order_id)
            //
            // This preserves single ownership of correlation state.
            if tx_submission_result.send(IbkrSubmissionResult {
                order_id: order.id,
                ibkr_order_id,
            }).is_err() {
                print_log(
                    debug_mode,
                    true,
                    format!(
                        "[IBKR ADAPTER] MARKET CORRELATION RESULT FAILED | order_id={} | ibkr_order_id={}",
                        order.id,
                        ibkr_order_id,
                    ),
                );

                let _ = tx_evt.send(ExecEvent::Error {
                    order_id: Some(order.id),
                    message: "IBKR submission result channel closed".to_string(),
                    ts_ms: Utc::now().timestamp_millis(),
                });

                return;
            }


            // ----------------------------------------------------
            // SENT TO TWS
            // ----------------------------------------------------

            // Do NOT emit ExecEvent::OrderAccepted here.
            //
            // IBKR returning an order ID only confirms that the request was
            // successfully submitted to TWS.
            //
            // The IBKR order update stream remains authoritative for:
            //
            // - acceptance
            // - working state
            // - fills
            // - cancellation
            // - rejection
            print_log(
                debug_mode,
                true,
                format!(
                    "[IBKR ADAPTER] MARKET SENT | order_id={} | symbol={} | ibkr_order_id={}",
                    order.id,
                    order.symbol,
                    ibkr_order_id,
                ),
            );
        }


        // ========================================================
        // SUBMISSION ERROR
        // ========================================================

        Err(error) => {
            print_log(
                debug_mode,
                true,
                format!(
                    "[IBKR ADAPTER] MARKET SUBMIT ERROR | order_id={} | symbol={} | error={:?}",
                    order.id,
                    order.symbol,
                    error,
                ),
            );

            let _ = tx_evt.send(ExecEvent::Error {
                order_id: Some(order.id),
                message: format!("IBKR market order submission failed: {:?}", error),
                ts_ms: Utc::now().timestamp_millis(),
            });
        }
    }
}