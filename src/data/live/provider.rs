// ============================================================
// LIVE MARKET DATA PROVIDER
// ============================================================

/// Selects the provider used for live market-data events.
///
/// This is intentionally independent from the execution venue. For example,
/// the engine may use synthetic market data while executing through a paper
/// venue, allowing market-data and execution systems to be tested separately.
///
/// This enum applies only to live/streaming data. Historical/backtest data
/// providers are configured separately through the historical data system.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveMarketDataProvider {
    Alpaca,
    Hyperliquid,
    // Databento...
    Synthetic,
}