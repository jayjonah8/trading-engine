// ============================================================
// HISTORICAL DATA PROVIDER
// ============================================================

/// Selects the provider used to obtain historical market data for backtests.
///
/// Historical providers are intentionally separate from live market-data
/// providers because their APIs, storage formats, and replay requirements
/// may differ substantially from streaming market-data sources.
///
/// `Synthetic` will eventually generate deterministic historical tape/bar
/// datasets for testing the complete backtesting pipeline without requiring
/// downloaded market data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoricalDataProvider {
    Alpaca,
    Databento,
    Hyperliquid,
    Synthetic,
}