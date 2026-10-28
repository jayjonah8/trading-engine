#![allow(dead_code)]


// ============================================================
// MULTI-SYNC CONFIGURATION
// ============================================================
//
// PURPOSE:
// Defines and validates configuration settings for the
// Multi-Sync Profit Protection and Drawdown Protection systems.
//
// CONFIGURATION:
// - MultiSyncSetting:
//     Defines a PnL threshold and window duration.
//
// - MultiSyncProfitProtectionConfig:
//     Enables Profit Protection and defines its stages.
//
// - MultiSyncDrawdownProtectionConfig:
//     Enables Drawdown Protection and defines its stages.
//
// - MultiSyncConfig:
//     Controls the overall feature and debug logging.
//
// THRESHOLDS:
// Thresholds are percentages of the strategy's reference
// margin, calculated using realized PnL since the anchor.
//
// WINDOW DURATIONS:
// Some(milliseconds) = Finite window.
// None = Indefinite window.
//
// VALIDATION:
// Ensures thresholds are positive and finite, durations
// are positive, and enabled protections have settings.
//
// V2 COMPATIBILITY:
// Trades opened during Multi-Sync windows are permanently
// prevented from triggering additional windows when they
// close. This behavior is not configurable.
//
// CONFIGURATION LIFECYCLE:
// Settings are supplied during initialization.
// Runtime configuration changes are not required.
//
// ============================================================


// ============================================================
// MULTI-SYNC SETTING
// ============================================================

#[derive(Debug, Clone)]
pub struct MultiSyncSetting {
    /// Realized PnL threshold as a percentage of reference margin.
    pub threshold_pct: f64,

    /// Duration of the regime reversal window in milliseconds.
    /// None means the window remains active indefinitely.
    pub window_ms: Option<i64>,
}


// ============================================================
// MULTI-SYNC PROFIT PROTECTION CONFIG
// ============================================================

#[derive(Debug, Clone)]
pub struct MultiSyncProfitProtectionConfig {
    pub enabled: bool,

    pub default_settings: Vec<MultiSyncSetting>,
}


// ============================================================
// MULTI-SYNC DRAWDOWN PROTECTION CONFIG
// ============================================================

#[derive(Debug, Clone)]
pub struct MultiSyncDrawdownProtectionConfig {
    pub enabled: bool,

    pub default_settings: Vec<MultiSyncSetting>,
}


// ============================================================
// MULTI-SYNC CONFIG
// ============================================================

#[derive(Debug, Clone)]
pub struct MultiSyncConfig {
    /// Enables or disables the entire feature.
    pub enabled: bool,

    pub profit_protection: MultiSyncProfitProtectionConfig,

    pub drawdown_protection: MultiSyncDrawdownProtectionConfig,

    /// Enables or disables debug logging.
    pub debug_mode: bool,
}


// ============================================================
// CONFIG VALIDATION
// ============================================================

impl MultiSyncConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        for setting in self
            .profit_protection
            .default_settings
            .iter()
            .chain(self.drawdown_protection.default_settings.iter())
        {
            if !setting.threshold_pct.is_finite()
                || setting.threshold_pct <= 0.0
            {
                return Err(
                    "Multi-Sync thresholds must be finite and positive"
                );
            }

            if matches!(setting.window_ms, Some(ms) if ms <= 0) {
                return Err(
                    "Multi-Sync window durations must be positive"
                );
            }
        }

        if !self.enabled {
            return Ok(());
        }

        if self.profit_protection.enabled
            && self.profit_protection.default_settings.is_empty()
        {
            return Err(
                "Profit Protection requires at least one setting"
            );
        }

        if self.drawdown_protection.enabled
            && self.drawdown_protection.default_settings.is_empty()
        {
            return Err(
                "Drawdown Protection requires at least one setting"
            );
        }

        if !self.profit_protection.enabled
            && !self.drawdown_protection.enabled
        {
            return Err(
                "At least one Multi-Sync protection must be enabled"
            );
        }

        Ok(())
    }
}