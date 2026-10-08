// Examples of how to use the provided indicators within a strategy.rs or backtest_strategy.rs
//
// Candle history may not contain enough data yet.
// Always check for None / required length before using candles.
// If data is missing, safely skip the trading decision.

Imports

```rust
use crate::market::{
    candle::CandleConfig,
    candle_history::CandleHistory,
    timeframe::Timeframe,
    session::regular_market_session,
};

use crate::indicators::{
    atr,
    atr_percent,
    bollinger_bands,
    candle_strength,
    ema,
    linear_regression_trend,
    macd,
    noise_ratio,
    rate_of_change,
    realized_volatility_pct,
    relative_volume,
    rolling_high_low,
    rsi,
    sma,
    volume_ema,
    volume_sma,
    vwap_between,

    CrossAssetConfig,
    market_breadth,
    market_volume_acceleration,
    market_volume_expansion,
};

```

Find the function where your trading desicions are made:

```rust
 async fn on_event(
        &mut self,
        ev: AppEventEnvelope,
    ) {}
```

These examples are shown inside the BarClosed match block, but self.candle_history is part of the strategy’s state
and can be accessed from anywhere within the strategy. This means historical candle data is available throughout
the strategy wherever it is needed.

```rust

AppEvent::BarClosed(bar) => {
    // Candle history may not contain enough data yet.
    // Always check for None / required length before using candles.
    // If data is missing, safely skip the trading decision.

    if self.debug_mode {
        println!(
            "[BASE BACKTEST BAR CLOSED][{}][{}s] \
            {} -> {} | \
            O={:.2} H={:.2} L={:.2} C={:.2} | \
            V={} | trades={}",
            ev.symbol,
            bar.timeframe.as_secs(),
            bar.start,
            bar.end,
            bar.open,
            bar.high,
            bar.low,
            bar.close,
            bar.volume,
            bar.trade_count,
        );
    }

    let timeframe =
        bar.timeframe;

    self.candle_history
        .push(bar);

    if self.debug_mode {
        println!(
            "[BASE BACKTEST CANDLES][{}][{}s] stored={}",
            ev.symbol,
            timeframe.as_secs(),
            self.candle_history.len(
                &ev.symbol,
                timeframe,
            ),
        );
    }

    // ============================================================
    // EXAMPLE CANDLE HISTORY USE
    // ============================================================

    // Get the latest completed 5-minute candle.

    if let Some(last_bar) =
        self.candle_history.latest(
            &ev.symbol,
            Timeframe::FIVE_MINUTES,
        )
    {
        // Use:
        // last_bar.open
        // last_bar.high
        // last_bar.low
        // last_bar.close
        // last_bar.volume
        // last_bar.trade_count
    }


    // Get stored 1-minute candles for this symbol.
    if let Some(bars) =
        self.candle_history.get(
            &ev.symbol,
            Timeframe::ONE_MINUTE,
        )
    {
        // Example: analyze the last 20 one-minute candles.

        for bar in bars.iter().rev().take(20) {
            // Use candle data here.
        }
    //
    //     // Calculate things like:
    //     //
    //     // average / relative volume
    //     // candle momentum
    //     // ATR
    //     // EMA
    //     // candle body strength
    //     // candle patterns
    //     // etc.
    }

    // You can independently access any configured timeframe:
    Timeframe::ONE_MINUTE
    Timeframe::FIVE_MINUTES
    Timeframe::FIFTEEN_MINUTES
    Timeframe::TWO_HOURS



    // Test common candle-based indicators on the selected timeframe:
    //
    // Price / trend:
    // - SMA
    // - EMA
    // - RSI
    // - MACD
    // - Bollinger Bands
    // - Rate of Change (ROC)
    // - Rolling High / Low
    // - Linear Regression Slope
    // - Linear Regression R²
    //
    // Volatility / candle behavior:
    // - ATR
    // - ATR Percent
    // - Realized Volatility
    // - Noise Ratio
    // - Candle Strength
    //
    // Volume:
    // - Relative Volume (RVOL)
    // - Volume SMA
    // - Volume EMA
    // - Exact Session VWAP
    if let Some(bars) =
        self.candle_history.get(
            &ev.symbol,
            Timeframe::FIVE_MINUTES,
        )
    {
        // VecDeque is not guaranteed to be contiguous,
        // so collect the candles into a Vec for indicator calculations.
        let bars_vec: Vec<_> =
            bars.iter()
                .cloned()
                .collect();


        // ============================================================
        // SMA
        // ============================================================

        if let Some(value) =
            sma(
                &bars_vec,
                20,
            )
        {
            if self.debug_mode {
                println!(
                    "[BASE BACKTEST INDICATOR][{}][5m] SMA20={:.4}",
                    ev.symbol,
                    value
                );
            }
        }


        // ============================================================
        // EMA
        // ============================================================

        if let Some(value) =
            ema(
                &bars_vec,
                20,
            )
        {
            if self.debug_mode {
                println!(
                    "[BASE BACKTEST INDICATOR][{}][5m] EMA20={:.4}",
                    ev.symbol,
                    value
                );
            }
        }


        // ============================================================
        // ATR
        // ============================================================

        if let Some(value) =
            atr(
                &bars_vec,
                14,
            )
        {
            if self.debug_mode {
                println!(
                    "[BASE BACKTEST INDICATOR][{}][5m] ATR14={:.4}",
                    ev.symbol,
                    value
                );
            }
        }


        // ============================================================
        // ATR PERCENT
        // ============================================================

        // ATR normalized as a percentage of the latest closing price.
        // Useful for comparing volatility across differently priced assets.
        if let Some(value) =
            atr_percent(
                &bars_vec,
                14,
            )
        {
            if self.debug_mode {
                println!(
                    "[BASE BACKTEST INDICATOR][{}][5m] ATR_PCT14={:.4}%",
                    ev.symbol,
                    value
                );
            }
        }


        // ============================================================
        // REALIZED VOLATILITY
        // ============================================================

        // Standard deviation of recent candle log returns.
        // Higher values mean recent price movement is more variable/choppy.
        if let Some(value) =
            realized_volatility_pct(
                &bars_vec,
                20,
            )
        {
            if self.debug_mode {
                println!(
                    "[BASE BACKTEST INDICATOR][{}][5m] REALIZED_VOL20={:.4}%",
                    ev.symbol,
                    value
                );
            }
        }


        // ============================================================
        // NOISE RATIO
        // ============================================================

        // Compares realized volatility against the absolute net move.
        //
        // Lower = cleaner directional movement.
        // Higher = more chop relative to actual progress.
        if let Some(value) =
            noise_ratio(
                &bars_vec,
                20,
            )
        {
            if self.debug_mode {
                println!(
                    "[BASE BACKTEST INDICATOR][{}][5m] NOISE_RATIO20={:.4}",
                    ev.symbol,
                    value
                );
            }
        }


        // ============================================================
        // LINEAR REGRESSION TREND
        // ============================================================

        // Measures:
        //
        // slope_pct_per_hour:
        // normalized trend direction and speed.
        //
        // r2:
        // how closely price follows a straight-line trend.
        if let Some(value) =
            linear_regression_trend(
                &bars_vec,
                20,
            )
        {
            if self.debug_mode {
                println!(
                    "[BASE BACKTEST INDICATOR][{}][5m] \
                    LINEAR_REGRESSION20 | \
                    SLOPE={:+.4}%/hr | \
                    R2={:.4}",
                    ev.symbol,
                    value.slope_pct_per_hour,
                    value.r2,
                );
            }
        }


        // ============================================================
        // RELATIVE VOLUME
        // ============================================================

        if let Some(value) =
            relative_volume(
                &bars_vec,
                20,
            )
        {
            if self.debug_mode {
                println!(
                    "[BASE BACKTEST INDICATOR][{}][5m] RVOL20={:.2}x",
                    ev.symbol,
                    value
                );
            }
        }


        // ============================================================
        // VOLUME SMA
        // ============================================================

        if let Some(value) =
            volume_sma(
                &bars_vec,
                20,
            )
        {
            if self.debug_mode {
                println!(
                    "[BASE BACKTEST INDICATOR][{}][5m] VOLUME_SMA20={:.2}",
                    ev.symbol,
                    value
                );
            }
        }


        // ============================================================
        // VOLUME EMA
        // ============================================================

        if let Some(value) =
            volume_ema(
                &bars_vec,
                20,
            )
        {
            if self.debug_mode {
                println!(
                    "[BASE BACKTEST INDICATOR][{}][5m] VOLUME_EMA20={:.2}",
                    ev.symbol,
                    value
                );
            }
        }


        // ============================================================
        // RATE OF CHANGE
        // ============================================================

        if let Some(value) =
            rate_of_change(
                &bars_vec,
                20,
            )
        {
            if self.debug_mode {
                println!(
                    "[BASE BACKTEST INDICATOR][{}][5m] ROC20={:.3}%",
                    ev.symbol,
                    value
                );
            }
        }


        // ============================================================
        // RSI
        // ============================================================

        if let Some(value) =
            rsi(
                &bars_vec,
                14,
            )
        {
            if self.debug_mode {
                println!(
                    "[BASE BACKTEST INDICATOR][{}][5m] RSI14={:.2}",
                    ev.symbol,
                    value
                );
            }
        }


        // ============================================================
        // MACD
        // ============================================================

        // Standard MACD:
        // fast EMA = 12
        // slow EMA = 26
        // signal EMA = 9
        if let Some(value) =
            macd(
                &bars_vec,
                12,
                26,
                9,
            )
        {
            if self.debug_mode {
                println!(
                    "[BASE BACKTEST INDICATOR][{}][5m] \
                    MACD={:.4} | SIGNAL={:.4} | HISTOGRAM={:.4}",
                    ev.symbol,
                    value.macd,
                    value.signal,
                    value.histogram,
                );
            }
        }


        // ============================================================
        // BOLLINGER BANDS
        // ============================================================

        // Standard Bollinger Bands:
        // 20 candle SMA
        // 2 standard deviations
        if let Some(value) =
            bollinger_bands(
                &bars_vec,
                20,
                2.0,
            )
        {
            if self.debug_mode {
                println!(
                    "[BASE BACKTEST INDICATOR][{}][5m] \
                    BOLLINGER | LOWER={:.4} | MID={:.4} | UPPER={:.4} | WIDTH={:.4}",
                    ev.symbol,
                    value.lower,
                    value.middle,
                    value.upper,
                    value.width,
                );
            }
        }


        // ============================================================
        // ROLLING HIGH / LOW
        // ============================================================

        // Highest high and lowest low over the last 20 candles.
        if let Some(value) =
            rolling_high_low(
                &bars_vec,
                20,
            )
        {
            if self.debug_mode {
                println!(
                    "[BASE BACKTEST INDICATOR][{}][5m] \
                    HIGH_LOW20 | HIGH={:.4} | LOW={:.4} | RANGE={:.4}",
                    ev.symbol,
                    value.high,
                    value.low,
                    value.range,
                );
            }
        }


        // ============================================================
        // CANDLE STRENGTH
        // ============================================================

        // Measures average candle range/body and the shape
        // of the latest completed candle.
        if let Some(value) =
            candle_strength(
                &bars_vec,
                20,
            )
        {
            if self.debug_mode {
                println!(
                    "[BASE BACKTEST INDICATOR][{}][5m] \
                    CANDLE_STRENGTH20 | \
                    AVG_RANGE={:.4} | \
                    AVG_BODY={:.4} | \
                    AVG_BODY_PCT={:.3}% | \
                    LATEST_BODY_RANGE={:.3} | \
                    LATEST_CLOSE_POSITION={:.3}",
                    ev.symbol,
                    value.average_range,
                    value.average_body,
                    value.average_body_percent,
                    value.latest_body_to_range,
                    value.latest_close_position,
                );
            }
        }


        // ============================================================
        // SESSION VWAP
        // ============================================================

        // VWAP uses exact raw-trade price * volume accumulation.
        //
        // Only candles from the current regular market session are used,
        // preventing previous trading days from affecting today's VWAP.
        //
        // Using the candle timestamp makes this work correctly for both
        // historical backtests and live trading.
        if let Some(latest_bar) =
            bars_vec.last()
        {
            if let Some((
                session_start,
                session_end,
            )) =
                regular_market_session(
                    latest_bar.end
                )
            {
                if let Some(value) =
                    vwap_between(
                        &bars_vec,
                        session_start,
                        session_end,
                    )
                {
                    if self.debug_mode {
                        println!(
                            "[BASE BACKTEST INDICATOR][{}][5m] SESSION_VWAP={:.4}",
                            ev.symbol,
                            value
                        );
                    }
                }
            }
        }
    }

    // ============================================================
    // CROSS-ASSET MARKET INDICATORS
    // ============================================================

    // Market-wide indicators analyze multiple symbols together.
    //
    // Default synchronization requirement:
    // at least 80% of configured assets must have the same
    // latest completed candle timestamp.
    //
    // Assets that are stale at that timestamp are excluded from
    // the current market snapshot.
    let cross_asset_config =
        CrossAssetConfig::default();

    let min_sync_pct =
        cross_asset_config
            .min_sync_pct;


    let symbols =
        {
            let view =
                self.rx_view.borrow();

            view.cfg.symbols.clone()
        };


    // ============================================================
    // MARKET VOLUME EXPANSION
    // ============================================================

    if let Some(value) =
        market_volume_expansion(
            &self.candle_history,
            &symbols,
            Timeframe::FIVE_MINUTES,
            20,
            1.5,
            min_sync_pct,
        )
    {
        if self.debug_mode {
            println!(
                "[BASE BACKTEST MARKET INDICATOR][5m] \
                VOLUME_EXPANSION | \
                AVG_RVOL={:.2}x | \
                EXPANDING={:.1}% | \
                ASSETS={}/{} | \
                SYNC={:.1}% | \
                END={}",
                value.average_rvol,
                value.expanding_assets_pct,
                value.assets_used,
                value.assets_total,
                value.sync_pct,
                value.snapshot_end,
            );
        }
    }


    // ============================================================
    // MARKET VOLUME ACCELERATION
    // ============================================================

    if let Some(value) =
        market_volume_acceleration(
            &self.candle_history,
            &symbols,
            Timeframe::FIVE_MINUTES,
            20,
            min_sync_pct,
        )
    {
        if self.debug_mode {
            println!(
                "[BASE BACKTEST MARKET INDICATOR][5m] \
                VOLUME_ACCELERATION | \
                CURRENT_RVOL={:.2}x | \
                PREVIOUS_RVOL={:.2}x | \
                CHANGE={:+.2} | \
                CHANGE_PCT={:+.1}% | \
                ACCELERATING={:.1}% | \
                ASSETS={}/{} | \
                SYNC={:.1}% | \
                END={}",
                value.current_average_rvol,
                value.previous_average_rvol,
                value.rvol_change,
                value.rvol_change_pct,
                value.accelerating_assets_pct,
                value.assets_used,
                value.assets_total,
                value.sync_pct,
                value.snapshot_end,
            );
        }
    }


    // ============================================================
    // MARKET BREADTH
    // ============================================================

    if let Some(value) =
        market_breadth(
            &self.candle_history,
            &symbols,
            Timeframe::FIVE_MINUTES,
            20,
            min_sync_pct,
        )
    {
        if self.debug_mode {
            println!(
                "[BASE BACKTEST MARKET INDICATOR][5m] \
                BREADTH20 | \
                BULLISH={:.1}% | \
                BEARISH={:.1}% | \
                NEUTRAL={:.1}% | \
                AVG_MOVE={:+.3}% | \
                ASSETS={}/{} | \
                SYNC={:.1}% | \
                END={}",
                value.bullish_assets_pct,
                value.bearish_assets_pct,
                value.neutral_assets_pct,
                value.average_move_pct,
                value.assets_used,
                value.assets_total,
                value.sync_pct,
                value.snapshot_end,
            );
        }
    }

}

```
