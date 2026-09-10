#![allow(dead_code)]

// ============================================================
// GENERAL ENGINE STATE
// ============================================================

#[derive(Debug, Default, Clone)]
pub struct General {
    /// Total realized PnL produced by live broker-executed trades.
    pub realized_pnl: f64,

    /// Total realized PnL produced by simulated trades.
    pub sim_realized_pnl: f64,

    /// Current account balance used by the engine.
    pub account_balance: f64,
}