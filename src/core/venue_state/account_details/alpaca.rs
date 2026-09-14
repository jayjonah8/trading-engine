#![allow(dead_code)]


// ============================================================
// ALPACA ACCOUNT STATE
// ============================================================

#[derive(Debug, Clone)]
pub struct AlpacaAccountState {
    pub equity: f64,

    pub cash: f64,

    pub buying_power: f64,

    pub long_market_value: f64,

    pub short_market_value: f64,

    pub daytrade_count: u32,
}