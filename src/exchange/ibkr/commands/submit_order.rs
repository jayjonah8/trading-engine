#![allow(dead_code)]

use ibapi::Client;
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    core::execution::{ExecEvent, Order, OrderType},
    exchange::ibkr::{
        commands::submit::{
            handle_limit_order,
            handle_market_order,
            handle_stop_limit_order,
            handle_stop_market_order,
        },
        submission_result::IbkrSubmissionResult,
    },
};


// ============================================================
// HANDLE SUBMIT ORDER
// ============================================================

/// Routes a canonical Engine `Order` to the correct IBKR
/// order-type-specific submit handler.
///
/// Each handler translates the canonical order into the corresponding IBKR
/// request and submits it through the shared TWS / IB Gateway client.
///
/// A successful `submit()` only means TWS returned an IBKR order ID.
/// Authoritative lifecycle state comes from the order-update stream.
///
/// The submission result is returned to the adapter event loop so it can
/// establish canonical OrderId ↔ IBKR order ID correlation.
pub async fn handle_submit_order(
    client: &Client,
    tx_evt: &UnboundedSender<ExecEvent>,
    tx_submission_result: &UnboundedSender<IbkrSubmissionResult>,
    order: Order,
    debug_mode: bool,
) {
    match order.order_type {
        OrderType::Market => {
            handle_market_order(client, tx_evt, tx_submission_result, order, debug_mode).await;
        }

        OrderType::Limit => {
            handle_limit_order(client, tx_evt, tx_submission_result, order, debug_mode).await;
        }

        OrderType::StopMarket => {
            handle_stop_market_order(client, tx_evt, tx_submission_result, order, debug_mode).await;
        }

        OrderType::StopLimit => {
            handle_stop_limit_order(client, tx_evt, tx_submission_result, order, debug_mode).await;
        }
    }
}