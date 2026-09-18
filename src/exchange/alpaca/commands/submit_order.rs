#![allow(dead_code)]

use tokio::sync::mpsc::UnboundedSender;

use crate::{
    core::execution::{
        ExecEvent,
        Order,
        OrderType,
    },

    exchange::alpaca::{
        commands::submit::{
            handle_limit_order,
            handle_market_order,
            handle_stop_limit_order,
            handle_stop_market_order,
        },

        submission_result::AlpacaSubmissionResult,
        types::AlpacaConfig,
    },
};


// ============================================================
// HANDLE SUBMIT ORDER
// ============================================================

/// Routes a canonical Engine `Order` to the correct Alpaca
/// order-type-specific submit handler.
///
/// Each handler owns its Alpaca-specific REST translation and immediate
/// submission-response handling.
///
/// Market and limit submissions also receive `tx_submission_result`.
///
/// This private adapter channel exists because Alpaca assigns its own venue
/// order ID only after the REST submission succeeds. The spawned REST task
/// must return that ID to the adapter so the adapter can update its private
/// `AlpacaOrderCorrelation`.
///
/// The REST task never receives mutable access to correlation state.
///
/// Submission flow:
///
/// ```text
/// Engine
///   │
///   ▼
/// ExecCommand::SubmitOrder
///   │
///   ▼
/// adapter.rs
///   │
///   ├── register OrderId <-> ClientOrderId
///   │
///   └── spawn REST submission
///           │
///           ▼
///      submit_order.rs
///           │
///           ├── Market ─────► market_order.rs
///           │
///           ├── Limit ──────► limit_order.rs
///           │
///           ├── StopMarket ─► stop_market_order.rs
///           │
///           └── StopLimit ──► stop_limit_order.rs
///                               │
///                               ▼
///                         Alpaca REST API
///                               │
///                               ▼
///                       venue_order_id assigned
///                               │
///                    ┌──────────┴──────────┐
///                    │                     │
///                    ▼                     ▼
///       AlpacaSubmissionResult    ExecEvent::OrderAccepted
///                    │                     │
///                    ▼                     ▼
///               adapter.rs              Engine
///                    │
///                    ▼
///       correlation.set_venue_order_id(...)
/// ```
///
/// This gives the adapter two separate information paths:
///
/// - `ExecEvent::OrderAccepted` reports Alpaca acceptance to the Engine.
/// - `AlpacaSubmissionResult` updates private adapter correlation state.
///
/// Actual executions do NOT come from the REST submission response.
///
/// Alpaca `trade_updates` remains authoritative for:
///
/// - working lifecycle updates
/// - individual executions / fills
/// - cancellations
/// - expirations
/// - rejections
///
/// Authoritative Order, Trade, Position, FillLedger, and RiskState mutations
/// remain exclusively inside the Engine.
pub async fn handle_submit_order(
    alpaca_config: &AlpacaConfig,
    tx_evt: &UnboundedSender<ExecEvent>,
    tx_submission_result: &UnboundedSender<AlpacaSubmissionResult>,
    order: Order,
    debug_mode: bool,
) {
    match order.order_type {
        // ========================================================
        // MARKET
        // ========================================================

        OrderType::Market => {
            handle_market_order(
                alpaca_config,
                tx_evt,
                tx_submission_result,
                order,
                debug_mode,
            )
            .await;
        }


        // ========================================================
        // LIMIT
        // ========================================================

        OrderType::Limit => {
            handle_limit_order(
                alpaca_config,
                tx_evt,
                tx_submission_result,
                order,
                debug_mode,
            )
            .await;
        }


        // ========================================================
        // STOP MARKET
        // ========================================================

        // Stop-market submission is intentionally still boilerplate.
        //
        // Once implemented, it can receive the same submission-result
        // channel and participate in the same correlation architecture.
        OrderType::StopMarket => {
            handle_stop_market_order(
                alpaca_config,
                tx_evt,
                order,
                debug_mode,
            )
            .await;
        }


        // ========================================================
        // STOP LIMIT
        // ========================================================

        // Stop-limit submission is intentionally still boilerplate.
        //
        // Once implemented, it can receive the same submission-result
        // channel and participate in the same correlation architecture.
        OrderType::StopLimit => {
            handle_stop_limit_order(
                alpaca_config,
                tx_evt,
                order,
                debug_mode,
            )
            .await;
        }
    }
}