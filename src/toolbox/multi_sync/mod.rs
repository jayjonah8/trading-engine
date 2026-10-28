// ============================================================
// MULTI-SYNC MODULES
// ============================================================

pub mod config;
pub mod state;

mod multi_sync;
mod profit_protection;
mod drawdown_protection;
mod timer;


// ============================================================
// PUBLIC EXPORTS
// ============================================================

pub use multi_sync::MultiSync;

pub use config::{
    MultiSyncConfig,
    MultiSyncSetting,
    MultiSyncProfitProtectionConfig,
    MultiSyncDrawdownProtectionConfig,
};

pub use state::{
    MultiSyncState,
    MultiSyncWindow,
    MultiSyncWindowKind,
    MultiSyncProfitProtectionState,
    MultiSyncDrawdownProtectionState,
};


// ============================================================
// MULTI-SYNC TESTS
// ============================================================

#[cfg(test)]
mod tests;