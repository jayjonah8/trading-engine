#![allow(dead_code)]

use super::config::MultiSyncConfig;

use super::state::{
    MultiSyncState,
    MultiSyncProfitProtectionState,
    MultiSyncDrawdownProtectionState,
};


// ============================================================
// MULTI-SYNC TOOLBOX
// ============================================================
//
// PURPOSE:
// Provides the main MultiSync implementation and coordinates
// shared state, configuration, and protection systems.
//
// CURRENT IMPLEMENTATION (PHASE 1):
// - Initializes Multi-Sync configuration and state.
// - Validates configuration before initialization.
// - Tracks independent Profit Protection and Drawdown
//   Protection state.
// - Exposes feature and window status helpers.
// - Classifies trades using historical window timestamps.
// - Calculates realized PnL pressure since the anchor.
//
// PNL CALCULATION:
// pnl_since_anchor = running_realized_pnl - anchor_realized_pnl
//
// pnl_pct = (pnl_since_anchor / reference_margin_usd) * 100
//
// STRATEGY ISOLATION:
// Each strategy owns its own MultiSync instance.
//
// Realized PnL must belong to that specific strategy.
// Existing Trade/Trades PnL helpers can provide the values.
//
// V2 COMPATIBILITY:
// Multi-Sync uses a shared cumulative PnL anchor and
// supports independent protection stages.
//
// PLANNED PHASE 3:
// - Process completed trades.
// - Evaluate protection triggers.
// - Activate and complete windows.
// - Temporarily reverse MarketRegime.
// - Restore the original regime at scheduled expiration.
// - Advance stages and reset the shared PnL anchor.
// - Prevent window-entered trades from retriggering.
//
// ============================================================


pub struct MultiSync {
    pub config: MultiSyncConfig,

    pub state: MultiSyncState,

    pub debug_mode: bool,
}


// ============================================================
// MULTI-SYNC IMPLEMENTATION
// ============================================================

impl MultiSync {
    // ========================================================
    // INITIALIZATION
    // ========================================================

    pub fn new(
        config: MultiSyncConfig,
    ) -> Result<Self, &'static str> {
        config.validate()?;

        let state = MultiSyncState {
            running_realized_pnl: 0.0,

            anchor_realized_pnl: 0.0,

            active_window: None,

            historical_windows: Vec::new(),

            profit_protection: MultiSyncProfitProtectionState {
                enabled: config.enabled
                    && config.profit_protection.enabled,

                settings: config
                    .profit_protection
                    .default_settings
                    .clone(),

                current_idx: 0,

                window_triggers: 0,

                completed_cycles: 0,
            },

            drawdown_protection: MultiSyncDrawdownProtectionState {
                enabled: config.enabled
                    && config.drawdown_protection.enabled,

                settings: config
                    .drawdown_protection
                    .default_settings
                    .clone(),

                current_idx: 0,

                window_triggers: 0,

                completed_cycles: 0,
            },
        };

        let debug_mode = config.debug_mode;

        Ok(Self {
            config,
            state,
            debug_mode,
        })
    }


    // ========================================================
    // FEATURE STATUS
    // ========================================================

    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }


    // ========================================================
    // WINDOW STATUS
    // ========================================================

    /// Returns whether a regime reversal is active
    /// at the supplied timestamp.
    pub fn is_window_active(&self, ts_ms: i64) -> bool {
        if !self.config.enabled {
            return false;
        }

        self.state
            .active_window
            .as_ref()
            .map_or(false, |window| {
                window.includes_entry(ts_ms)
            })
    }


    // ========================================================
    // PENDING ACCOUNTING WINDOW
    // ========================================================

    /// Returns whether a window still needs accounting
    /// completion, even if its scheduled duration has ended.
    pub fn has_pending_window(&self) -> bool {
        self.state.active_window.is_some()
    }


    // ========================================================
    // HISTORICAL WINDOW CLASSIFICATION
    // ========================================================

    /// Determines whether a trade opened during any
    /// previously triggered Multi-Sync window.
    pub fn opened_during_window(
        &self,
        entry_ts_ms: i64,
    ) -> bool {
        self.state
            .historical_windows
            .iter()
            .any(|window| {
                window.includes_entry(entry_ts_ms)
            })
    }


    // ========================================================
    // REALIZED PNL PRESSURE
    // ========================================================

    /// Calculates cumulative realized PnL since the anchor,
    /// expressed as a percentage of reference margin.
    ///
    /// This mirrors the V2 simulation:
    ///
    /// (running_abs - anchor_abs) / margin * 100
    pub fn current_pnl_pressure_pct(
        &self,
        reference_margin_usd: f64,
    ) -> Option<f64> {
        if !reference_margin_usd.is_finite()
            || reference_margin_usd <= 0.0
        {
            return None;
        }

        let pnl_since_anchor =
            self.state.running_realized_pnl
                - self.state.anchor_realized_pnl;

        Some(
            (pnl_since_anchor / reference_margin_usd) * 100.0
        )
    }
}