mod hyperliquid;
mod validation;

// ====================================================
// TODO: HISTORICAL TIMESTAMP SUPPORT
// ====================================================
//
// Add support for configuring a specific historical UTC
// timestamp when running price level detection tests.
//
// Currently, tests retrieve the latest completed candles.
// This upgrade should allow testing market structure at
// any specified point in historical time.
//
// Requirements:
// - Support a configurable historical analysis timestamp.
// - Fetch candles ending at or before that timestamp using
//   fetch_hyperliquid_candles_range().
// - Automatically calculate the required historical lookback.
// - Include sufficient historical candles for swing confirmation.
// - Ensure only candles completed by the analysis timestamp
//   are available to the detector.
// - Prevent lookahead bias during historical analysis.
// - Display the selected timestamp in the test output.
// - Support reproducible tests using identical historical data.
//
// Goal: Compare detected support and resistance levels against
// actual market structure at specific historical timestamps.


// ====================================================
// TODO: PRICE LEVEL CHART VISUALIZATION
// ====================================================
//
// Add visual charting support to the Price Levels testing
// module for evaluating detected support and resistance zones.
//
// Requirements:
// - Generate candlestick charts from fetched historical candles.
// - Overlay detected support and resistance price zones.
// - Display upper and lower boundaries for each zone.
// - Differentiate support, resistance, and combined levels.
// - Highlight detected swing highs and swing lows.
// - Display level strength scores and reaction counts.
// - Identify overlapping or duplicate price zones.
// - Support configurable symbols, timeframes, and lookbacks.
// - Export charts as image files or interactive HTML charts.
// - Ensure visualized levels correspond to the selected
//   historical analysis timestamp.
//
// Future enhancements:
// - Visualize multi-timeframe support and resistance.
// - Highlight multi-timeframe confluence regions.
// - Display breakouts, retests, and support/resistance flips.
// - Show how detected levels evolve over historical time.
//
// Goal: Visually compare detected price levels against real
// market price action to validate detection accuracy,
// clustering quality, and overall market structure analysis.