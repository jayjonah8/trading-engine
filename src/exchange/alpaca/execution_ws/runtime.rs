#![allow(dead_code)]

use serde_json::Value;

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

use crate::{
    core::config::Config,
    exchange::alpaca::types::AlpacaConfig,
};

use super::{
    health::{
        execution_ws_health_channel,
        ExecutionWsHealth,
    },
    trade_updates_ws::run_alpaca_trade_updates_ws,
};


// ============================================================
// EXECUTION WS RUNTIME
// ============================================================

/// Running Alpaca execution WebSocket infrastructure.
///
/// Alpaca exposes execution/order lifecycle updates through a single
/// `trade_updates` stream.
///
/// Raw Alpaca messages are forwarded through an internal channel for the
/// adapter/mapper layer to translate into canonical engine `ExecEvent`s.
///
/// Health is published independently through a watch channel.
pub struct AlpacaExecutionWs {
    pub health_rx: watch::Receiver<ExecutionWsHealth>,
    pub raw_rx: UnboundedReceiver<Value>,

    trade_updates_task: JoinHandle<()>,
}


// ============================================================
// START EXECUTION WS
// ============================================================

/// Starts Alpaca's execution-critical `trade_updates` WebSocket.
///
/// This function does not wait for readiness.
///
/// The adapter should wait for execution WebSocket readiness before accepting
/// execution commands.
pub fn start_execution_ws(
    cfg: Config,
    alpaca_config: AlpacaConfig,
) -> AlpacaExecutionWs {
    // ============================================================
    // RAW MESSAGE CHANNEL
    // ============================================================

    let (tx_raw, raw_rx) = unbounded_channel();


    // ============================================================
    // HEALTH
    // ============================================================

    let (health_tx, health_rx) = execution_ws_health_channel();


    // ============================================================
    // TRADE UPDATES WS
    // ============================================================

    let trade_updates_task = tokio::spawn(async move {
        if let Err(error) = run_alpaca_trade_updates_ws(
            cfg,
            alpaca_config,
            tx_raw,
            health_tx,
        ).await {
            eprintln!(
                "[ALPACA EXECUTION WS] supervisor stopped | error={}",
                error
            );
        }
    });


    // ============================================================
    // RUNTIME
    // ============================================================

    AlpacaExecutionWs {
        health_rx,
        raw_rx,
        trade_updates_task,
    }
}