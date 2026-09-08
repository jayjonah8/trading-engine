/// CROSS-ASSET INDICATOR CONFIG
///
/// Shared configuration for indicators that combine data from
/// multiple assets.
///
/// min_sync_pct controls how many tracked assets must have the
/// same latest completed candle timestamp before a cross-asset
/// market snapshot is considered valid.
///
/// Example:
///     min_sync_pct = 80.0
///
/// If 10 symbols are tracked, at least 8 must be synchronized.
///
/// This prevents one stale or inactive symbol from blocking
/// every market-wide indicator indefinitely.
#[derive(Debug, Clone, Copy)]
pub struct CrossAssetConfig {
    pub min_sync_pct: f64,
}


impl Default for CrossAssetConfig {
    fn default() -> Self {
        Self {
            // Require at least 80% of tracked assets to be
            // synchronized before calculating market indicators.
            min_sync_pct: 80.0,
        }
    }
}