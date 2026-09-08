# Indicators

This folder contains the technical and cross-asset indicators used by
the trading system.

## Price & Trend Indicators

### SMA --- `sma.rs`

**Simple Moving Average.** Calculates the average closing price over a
selected number of candles to smooth price movement and help identify
the underlying trend.

### EMA --- `ema.rs`

**Exponential Moving Average.** Calculates a moving average that gives
more weight to recent prices, allowing it to react faster to new price
movement than an SMA.

### Linear Regression --- `linear_regression.rs`

Measures the direction, strength, and consistency of a price trend using
a best-fit regression line. Useful values include the normalized price
slope and R-squared (`R²`) trend quality.

### Rate of Change --- `rate_of_change.rs`

**ROC.** Measures the percentage change in price between the current
close and the close a specified number of candles ago. Positive values
indicate gains; negative values indicate losses.

### RSI --- `rsi.rs`

**Relative Strength Index.** Measures recent upward versus downward
price movement to estimate momentum and identify relatively overbought
or oversold conditions.

### MACD --- `macd.rs`

**Moving Average Convergence Divergence.** Compares fast and slow
exponential moving averages to measure trend direction and momentum,
along with a signal line and histogram.

### Rolling High / Low --- `rolling_high_low.rs`

Finds the highest high and lowest low over a selected candle window.
Useful for identifying recent ranges, breakouts, support, and
resistance.

## Volatility & Candle Behavior

### ATR --- `atr.rs`

**Average True Range.** Measures the average absolute trading range of
recent candles, including gaps from the previous close, to estimate
price volatility.

### ATR Percent --- `atr_percent.rs`

Expresses ATR as a percentage of the current price. This makes
volatility easier to compare between assets with very different prices.

### Realized Volatility --- `realized_volatility.rs`

Measures the observed variability of recent price returns using
historical candles. Higher values indicate greater price movement or
choppiness.

### Noise Ratio --- `noise_ratio.rs`

Compares price variability with the asset's net directional movement.
Higher values indicate more chop relative to progress, while lower
values indicate cleaner directional movement.

### Bollinger Bands --- `bollinger_bands.rs`

Creates upper and lower volatility bands around a moving average using
standard deviation. Useful for measuring price dispersion, volatility
expansion/contraction, and position relative to a recent range.

### Candle Strength --- `candle_strength.rs`

Analyzes candle bodies and ranges to describe the strength and structure
of recent price movement, including how strongly the latest candle
closes within its range.

## Volume Indicators

### Relative Volume --- `relative_volume.rs`

**RVOL.** Compares current candle volume with typical recent volume.
Values above `1.0x` indicate volume is higher than its recent baseline.

### Volume Moving Average --- `volume_moving_average.rs`

Calculates moving averages of trading volume, such as volume SMA and
volume EMA, to establish a baseline for whether market activity is
increasing or decreasing.

### VWAP --- `vwap.rs`

**Volume-Weighted Average Price.** Calculates the average traded price
weighted by volume. The system can use session VWAP to show where the
average unit of volume traded during the current market session.

## Cross-Asset / Market-Wide Indicators

### Market Breadth --- `market_breadth.rs`

Measures how many tracked assets are moving upward versus downward over
the same lookback period. Helps determine whether a market move has
broad participation or is isolated to only a few assets.

### Market Volume Expansion --- `market_volume_expansion.rs`

Measures whether unusually high relative volume is occurring across many
tracked assets simultaneously. Useful for identifying broad increases in
market participation.

### Market Volume Acceleration --- `market_volume_acceleration.rs`

Measures how quickly relative volume is increasing or decreasing across
the tracked market. Helps identify sudden increases in participation and
fading market activity.

## Cross-Asset Infrastructure

### Cross-Asset Synchronization --- `cross_asset_sync.rs`

Aligns market-wide calculations to a common completed candle timestamp.
This prevents cross-asset indicators from accidentally comparing assets
at different points in market time.

### Cross-Asset Configuration --- `cross_asset_config.rs`

Stores configuration for cross-asset calculations, including the minimum
percentage of tracked assets required for a synchronized market
snapshot. This allows the system to tolerate some stale or missing
assets without requiring every symbol to update.

------------------------------------------------------------------------

## Quick Reference

  Indicator                    Primary Purpose
  ---------------------------- ---------------------------------------
  SMA                          Smoothed price trend
  EMA                          Faster-reacting price trend
  Linear Regression            Trend direction and quality
  Rate of Change               Price momentum / net move
  RSI                          Momentum strength
  MACD                         Trend and momentum
  Rolling High / Low           Recent price range
  ATR                          Absolute volatility
  ATR Percent                  Price-normalized volatility
  Realized Volatility          Historical return variability
  Noise Ratio                  Trend cleanliness vs. chop
  Bollinger Bands              Volatility and price dispersion
  Candle Strength              Candle structure and conviction
  Relative Volume              Current volume vs. normal volume
  Volume Moving Average        Volume baseline and trend
  VWAP                         Volume-weighted average price
  Market Breadth               Cross-asset directional participation
  Market Volume Expansion      Cross-asset volume participation
  Market Volume Acceleration   Speed of market-wide volume change
