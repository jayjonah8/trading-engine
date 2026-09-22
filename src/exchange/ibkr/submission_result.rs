#![allow(dead_code)]

use crate::core::execution::OrderId;


// ============================================================
// IBKR SUBMISSION RESULT
// ============================================================

/// Internal message returned from an IBKR submission task to the
/// authoritative IBKR adapter event loop.
///
/// IBKR's `submit()` returns an IBKR order ID after the request has been sent
/// to TWS.
///
/// That does NOT prove the order has been accepted, routed, or filled.
///
/// `IbkrSubmissionResult` exists only so the spawned submission task can
/// return IBKR's order identity to the adapter event loop without sharing
/// mutable correlation through Arc<Mutex<...>>.
///
/// The adapter then records:
///
///     Engine OrderId <-> IBKR order ID
///
/// Actual order lifecycle state remains authoritative from IBKR's
/// asynchronous order update stream.
#[derive(Debug, Clone, Copy)]
pub struct IbkrSubmissionResult {
    pub order_id: OrderId,
    pub ibkr_order_id: i32,
}