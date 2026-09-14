#![allow(dead_code)]


// ============================================================
// HYPERLIQUID ACCOUNT STATE
// ============================================================

/// Hyperliquid-specific account information.
///
/// These values preserve concepts that are useful on Hyperliquid but should
/// not be forced into the canonical AccountState abstraction.
#[derive(Debug, Clone)]
pub struct HyperliquidAccountState {
    pub account_value: f64,

    pub total_margin_used: f64,

    pub withdrawable: f64,

    pub total_ntl_pos: f64,
}