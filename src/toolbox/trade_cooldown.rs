#![allow(dead_code)]

use std::collections::HashMap;

// ============================================================
// TRADE COOLDOWN
// ============================================================
//
// Provides per-symbol trade entry cooldown management for strategies.
//
// Each strategy maintains its own independent cooldown state, allowing
// multiple strategies to trade the same symbol using different cooldown
// durations without interfering with one another.
//
// Cooldowns are based on event timestamps (milliseconds), ensuring
// consistent and deterministic behavior across live trading,
// simulation, and historical backtesting.
//
// Features:
// - Configurable cooldown duration in milliseconds.
// - Independent cooldown tracking for each symbol.
// - Prevents rapid consecutive trade entry submissions.
// - Supports clearing individual symbol or all cooldown states.
// - A cooldown of zero or less disables cooldown restrictions.
//
// Cooldowns are recorded explicitly when a strategy submits a trade
// request, rather than automatically when checking eligibility.
//
// This component operates entirely at the strategy level and does
// not modify engine state, manage orders, or enforce risk limits.
//
// ============================================================

pub struct TradeCooldown {
    pub cooldown_ms: i64,

    last_trade_ts: HashMap<String, i64>,
}

impl TradeCooldown {
    pub fn new(cooldown_ms: i64) -> Self {
        Self {
            cooldown_ms,
            last_trade_ts: HashMap::new(),
        }
    }

    /// Returns true if the symbol is eligible for another trade.
    pub fn can_trade(
        &self,
        symbol: &str,
        ts_ms: i64,
    ) -> bool {
        if self.cooldown_ms <= 0 {
            return true;
        }

        match self.last_trade_ts.get(symbol) {
            Some(&last_ts) => {
                ts_ms.saturating_sub(last_ts) >= self.cooldown_ms
            }
            None => true,
        }
    }

    /// Records the timestamp of a trade submission.
    pub fn record_trade(
        &mut self,
        symbol: &str,
        ts_ms: i64,
    ) {
        self.last_trade_ts.insert(
            symbol.to_string(),
            ts_ms,
        );
    }

    /// Clears the cooldown for a specific symbol.
    pub fn clear_symbol(
        &mut self,
        symbol: &str,
    ) -> bool {
        self.last_trade_ts.remove(symbol).is_some()
    }

    /// Clears all cooldowns.
    pub fn clear_all(&mut self) {
        self.last_trade_ts.clear();
    }
}