#![allow(dead_code)]

use crate::core::execution::OrderId;


// ============================================================
// ALPACA SUBMISSION RESULT
// ============================================================

/// Internal message returned from an Alpaca REST submission task to the
/// authoritative Alpaca adapter event loop.
///
/// This is separate from `ExecEvent::OrderAccepted`.
///
/// `ExecEvent::OrderAccepted` reports venue acceptance to the Engine.
///
/// `AlpacaSubmissionResult` allows the adapter to attach Alpaca's assigned
/// venue order ID to its private correlation state without sharing mutable
/// correlation through Arc<Mutex<...>>.
#[derive(Debug, Clone)]
pub struct AlpacaSubmissionResult {
    pub order_id: OrderId,
    pub venue_order_id: String,
}