#![allow(dead_code)]

/// Tracks trading fees and performance metrics.
/// Additional metrics such as Sharpe ratio, drawdown,
/// expected value, and profit factor can be added later.
#[derive(Debug, Clone, Default)]
pub struct Metrics {
    /// Total confirmed entry execution fees.
    pub entry_fees: f64,

    /// Total confirmed exit execution fees.
    pub exit_fees: f64,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the total accumulated trading fees.
    pub fn total_fees(&self) -> f64 {
        self.entry_fees + self.exit_fees
    }
}