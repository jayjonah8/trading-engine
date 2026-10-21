#![allow(dead_code)]

use std::collections::HashMap;

use crate::shared::print_log;

// ============================================================
// BLACK SWAN PROTECTION
// ============================================================
//
// Provides market-wide volatility protection by monitoring the
// average absolute 24-hour percentage price movement across assets.
//
// Features:
// - Tracks validated 24-hour price movements for each asset.
// - Calculates the average absolute movement across all assets.
// - Halts new trades when average movement reaches ex. 10%.
// - Resumes trading when average movement falls to ex. 8%.
// - Requires a minimum number of assets for market evaluation.
// - Removes cached asset statistics older than ex. 10 minutes.
// - Uses market timestamps for live trading and backtesting.
// - Maintains a global trading halt state for the strategy.
//
// Trading Behavior:
// - Blocks new LONG and SHORT entries while protection is active.
// - Does not interfere with existing trades or their exits.
// - Continues monitoring market conditions while trading is halted.
// - Automatically resumes new entries when conditions normalize.
//
// ============================================================

pub struct BlackSwanProtection {
    // Enable or disable black swan protection.
    pub enabled: bool,

    // Enable or disable debug logging.
    pub debug_mode: bool,

    // Whether new trades are currently halted.
    pub trading_halt: bool,

    // Halt when average absolute movement reaches this threshold.
    pub halt_threshold_pct: f64,

    // Resume when average absolute movement falls to this threshold.
    pub resume_threshold_pct: f64,

    // Minimum number of assets required to evaluate the market.
    pub min_assets: usize,

    // Maximum age of cached market statistics.
    pub max_data_age_ms: i64,

    // Latest validated 24-hour movement for each asset.
    market_24h_moves: HashMap<String, (f64, i64)>,
}

impl BlackSwanProtection {

    // ============================================================
    // NEW
    // ============================================================

    pub fn new(
        enabled: bool,
        debug_mode: bool,
        halt_threshold_pct: f64,
        resume_threshold_pct: f64,
        min_assets: usize,
    ) -> Self {
        Self {
            enabled,
            debug_mode,
            trading_halt: false,
            halt_threshold_pct,
            resume_threshold_pct,
            min_assets,
            max_data_age_ms: 600_000, // 10 minutes
            market_24h_moves: HashMap::new(),
        }
    }

    // ============================================================
    // UPDATE MARKET STATISTICS
    // ============================================================

    // Updates an asset's validated 24-hour percentage movement.
    //
    // Checks:
    // - Movement is valid and finite.
    // - Cached statistics are not stale.
    // - Enough assets are available.
    // - Average absolute movement exceeds the halt threshold.
    // - Average movement falls below the resume threshold.
    //
    // Uses market timestamps for both live and backtest execution.
    pub fn update(
        &mut self,
        symbol: &str,
        price_change_24h: Option<f64>,
        current_ts: i64,
    ) {
        if !self.enabled {
            self.trading_halt = false;
            return;
        }

        // ============================================================
        // UPDATE ASSET CACHE
        // ============================================================

        match price_change_24h {
            Some(change_pct) if change_pct.is_finite() => {
                self.market_24h_moves.insert(
                    symbol.to_string(),
                    (change_pct, current_ts),
                );
            }

            _ => {
                self.market_24h_moves.remove(symbol);
            }
        }

        // ============================================================
        // REMOVE STALE STATISTICS
        // ============================================================

        self.market_24h_moves.retain(|_, (_, ts)| {
            *ts <= current_ts
                && current_ts.saturating_sub(*ts) <= self.max_data_age_ms
        });

        // ============================================================
        // REQUIRE MINIMUM MARKET COVERAGE
        // ============================================================

        if self.market_24h_moves.len() < self.min_assets {
            // Do not automatically resume an existing halt
            // when market statistics become unavailable.

            self.log_state(symbol, current_ts, None);

            return;
        }

        // ============================================================
        // CALCULATE AVERAGE ABSOLUTE MOVEMENT
        // ============================================================

        let total_move: f64 = self.market_24h_moves
            .values()
            .map(|(pct, _)| pct.abs())
            .sum();

        let average_move =
            total_move / self.market_24h_moves.len() as f64;

        // ============================================================
        // UPDATE TRADING HALT
        // ============================================================

        if !self.trading_halt {
            if average_move >= self.halt_threshold_pct {
                self.trading_halt = true;
            }
        } else if average_move <= self.resume_threshold_pct {
            self.trading_halt = false;
        }

        // ============================================================
        // DEBUG STATE
        // ============================================================

        self.log_state(
            symbol,
            current_ts,
            Some(average_move),
        );
    }

    // ============================================================
    // DEBUG STATE
    // ============================================================

    // Prints the current market-wide protection state.
    //
    // Includes:
    // - Protection enabled/disabled.
    // - Trading halt state.
    // - Number of tracked assets.
    // - Minimum required assets.
    // - Average absolute 24-hour movement.
    // - Halt and resume thresholds.
    // - Maximum cached data age.
    // - Current market timestamp.
    fn log_state(
        &self,
        symbol: &str,
        current_ts: i64,
        average_move: Option<f64>,
    ) {
        if self.debug_mode {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[BLACK SWAN PROTECTION][{}] \
                    enabled={} | \
                    trading_halt={} | \
                    assets={}/{} | \
                    avg_24h_move={:?}% | \
                    halt_threshold={:.2}% | \
                    resume_threshold={:.2}% | \
                    max_data_age={}s | \
                    ts_ms={}",
                    symbol,
                    self.enabled,
                    self.trading_halt,
                    self.market_24h_moves.len(),
                    self.min_assets,
                    average_move,
                    self.halt_threshold_pct,
                    self.resume_threshold_pct,
                    self.max_data_age_ms / 1000,
                    current_ts,
                ),
            );
        }
    }

    // ============================================================
    // IS TRADING HALTED
    // ============================================================

    pub fn is_halted(&self) -> bool {
        self.enabled && self.trading_halt
    }
}