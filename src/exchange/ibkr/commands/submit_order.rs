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
/// The canonical Order remains venue-independent. Each submit handler owns
/// the IBKR-specific translation into an `ibapi` order request.
///
/// Submission flow:
///
/// Engine
///   │
///   ▼
/// ExecCommand::SubmitOrder
///   │
///   ▼
/// adapter.rs
///   │
///   ▼
/// submit_order.rs
///   │
///   ├── Market ─────► market_order.rs
///   ├── Limit ──────► limit_order.rs
///   ├── StopMarket ─► stop_market_order.rs
///   └── StopLimit ──► stop_limit_order.rs
///                         │
///                         ▼
///                    IBKR / TWS
///
/// `submit()` returning an IBKR order ID proves that the request was sent to
/// TWS, but does not prove that IBKR accepted, routed, or filled the order.
///
/// The returned IBKR order ID is sent back to the authoritative adapter event
/// loop through `IbkrSubmissionResult` so adapter-local correlation can be
/// updated without shared mutable state.
///
/// Authoritative lifecycle events are handled separately through the IBKR
/// order update stream.
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
            handle_stop_market_order(client, tx_evt, order, debug_mode).await;
        }

        OrderType::StopLimit => {
            handle_stop_limit_order(client, tx_evt, order, debug_mode).await;
        }
    }
}