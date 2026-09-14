#![allow(dead_code)]

use ethers::types::H160;

use hyperliquid_rust_sdk::Message;

use tokio::{
    sync::{
        mpsc::{
            unbounded_channel,
            UnboundedReceiver,
        },
        watch,
    },
    task::JoinHandle,
};

use super::{
    health::{
        execution_ws_health_channel,
        ExecutionWsHealth,
    },
    order_updates_ws::run_order_updates_ws,
    user_events_ws::run_user_events_ws,
};


// ============================================================
// EXECUTION WS RUNTIME
// ============================================================

/// Running Hyperliquid execution WebSocket infrastructure.
///
/// Both WebSocket connections feed their raw SDK messages into the same
/// internal channel. The adapter consumes that channel and translates the
/// messages through the Hyperliquid mapper layer.
///
/// Health is published independently through a watch channel.
pub struct HyperliquidExecutionWs {
    pub health_rx: watch::Receiver<ExecutionWsHealth>,
    pub raw_rx: UnboundedReceiver<Message>,

    order_updates_task: JoinHandle<()>,
    user_events_task: JoinHandle<()>,
}


// ============================================================
// START EXECUTION WS
// ============================================================

/// Starts both execution-critical Hyperliquid WebSocket subscriptions.
///
/// This function does not wait for readiness.
///
/// The adapter should verify execution WebSocket readiness before accepting
/// execution commands.
pub fn start_execution_ws(
    user: H160,
    debug_mode: bool,
) -> HyperliquidExecutionWs {
    // ============================================================
    // RAW MESSAGE CHANNEL
    // ============================================================

    let (
        tx_raw,
        raw_rx,
    ) =
        unbounded_channel();


    // ============================================================
    // HEALTH
    // ============================================================

    let (
        health_tx,
        health_rx,
    ) =
        execution_ws_health_channel();


    // ============================================================
    // ORDER UPDATES WS
    // ============================================================

    let order_updates_task =
        tokio::spawn(
            run_order_updates_ws(
                user,
                tx_raw.clone(),
                health_tx.clone(),
                debug_mode,
            )
        );


    // ============================================================
    // USER EVENTS WS
    // ============================================================

    let user_events_task =
        tokio::spawn(
            run_user_events_ws(
                user,
                tx_raw,
                health_tx,
                debug_mode,
            )
        );


    // ============================================================
    // RUNTIME
    // ============================================================

    HyperliquidExecutionWs {
        health_rx,
        raw_rx,
        order_updates_task,
        user_events_task,
    }
}