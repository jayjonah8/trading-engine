#![allow(dead_code)]


// ============================================================
// INTERACTIVE BROKERS ACCOUNT STATE
// ============================================================

#[derive(Debug, Clone)]
pub struct IbkrAccountState {
    pub net_liquidation: f64,

    pub available_funds: f64,

    pub excess_liquidity: f64,

    pub initial_margin_requirement: f64,

    pub maintenance_margin_requirement: f64,
}