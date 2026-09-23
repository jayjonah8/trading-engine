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
// HANDLE LIMIT ORDER
// ============================================================

/// Submits a canonical limit order to IBKR.
///
/// A successful `submit()` returns IBKR's order ID but does not prove the
/// order is working or filled. Authoritative lifecycle state arrives through
/// the IBKR order-update stream.
pub async fn handle_limit_order(
    client: &Client,
    tx_evt: &UnboundedSender<ExecEvent>,
    tx_submission_result: &UnboundedSender<IbkrSubmissionResult>,
    order: Order,
    debug_mode: bool,
) {
    // ============================================================
    // LIMIT PRICE
    // ============================================================

    let Some(limit_px) = order.limit_px else {
        let _ = tx_evt.send(ExecEvent::Error {
            order_id: Some(order.id),
            message: "IBKR limit order missing limit_px".to_string(),
            ts_ms: Utc::now().timestamp_millis(),
        });

        return;
    };


    // ============================================================
    // VALIDATION
    // ============================================================

    if !limit_px.is_finite() || limit_px <= 0.0 {
        let _ = tx_evt.send(ExecEvent::Error {
            order_id: Some(order.id),
            message: format!("Invalid IBKR limit price: {}", limit_px),
            ts_ms: Utc::now().timestamp_millis(),
        });

        return;
    }

    if !order.quantity.is_finite() || order.quantity <= 0.0 {
        let _ = tx_evt.send(ExecEvent::Error {
            order_id: Some(order.id),
            message: format!("Invalid IBKR limit order quantity: {}", order.quantity),
            ts_ms: Utc::now().timestamp_millis(),
        });

        return;
    }

    if order.post_only {
        let _ = tx_evt.send(ExecEvent::Error {
            order_id: Some(order.id),
            message: "IBKR post_only limit orders are not implemented".to_string(),
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
            "[IBKR ADAPTER] LIMIT SUBMIT | order_id={} | client_order_id={} | purpose={:?} | symbol={} | side={:?} | qty={} | limit_px={} | tif={:?} | reduce_only={} | post_only={}",
            order.id,
            order.client_order_id,
            order.purpose,
            order.symbol,
            order.side,
            order.quantity,
            limit_px,
            order.time_in_force,
            order.reduce_only,
            order.post_only,
        ),
    );

    let result = match order.side {
        Side::Buy => client.order(&contract).buy(order.quantity).limit(limit_px).submit().await,
        Side::Sell => client.order(&contract).sell(order.quantity).limit(limit_px).submit().await,
    };


    // ============================================================
    // SUBMISSION RESULT
    // ============================================================

    match result {
        Ok(ibkr_order_id) => {
            let ibkr_order_id = ibkr_order_id.0;

            if tx_submission_result.send(IbkrSubmissionResult {
                order_id: order.id,
                ibkr_order_id,
            }).is_err() {
                print_log(
                    debug_mode,
                    true,
                    format!(
                        "[IBKR ADAPTER] LIMIT CORRELATION RESULT FAILED | order_id={} | ibkr_order_id={}",
                        order.id, ibkr_order_id,
                    ),
                );

                let _ = tx_evt.send(ExecEvent::Error {
                    order_id: Some(order.id),
                    message: "IBKR submission result channel closed".to_string(),
                    ts_ms: Utc::now().timestamp_millis(),
                });

                return;
            }

            print_log(
                debug_mode,
                true,
                format!(
                    "[IBKR ADAPTER] LIMIT SENT | order_id={} | symbol={} | ibkr_order_id={}",
                    order.id, order.symbol, ibkr_order_id,
                ),
            );
        }

        Err(error) => {
            print_log(
                debug_mode,
                true,
                format!(
                    "[IBKR ADAPTER] LIMIT SUBMIT ERROR | order_id={} | symbol={} | error={:?}",
                    order.id, order.symbol, error,
                ),
            );

            let _ = tx_evt.send(ExecEvent::Error {
                order_id: Some(order.id),
                message: format!("IBKR limit order submission failed: {:?}", error),
                ts_ms: Utc::now().timestamp_millis(),
            });
        }
    }
}