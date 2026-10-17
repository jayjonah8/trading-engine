#![allow(dead_code)]

use std::sync::Arc;

use tokio::sync::{
    mpsc::UnboundedSender,
    watch,
};

use crate::{
    core::{
        engine::engine::EngineEvent,
        engine_view::EngineView,
        events::{
            AppEvent,
            AppEventEnvelope,
            Bar,
        },
    },

    market::{
        candle::CandleConfig,
        candle_history::CandleHistory,
        timeframe::Timeframe,
        session::regular_market_session,
    },

    indicators::{
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

        price_pct_change,
        volume_asset,
        volume_usd,

        CrossAssetConfig,
        market_breadth,
        market_volume_acceleration,
        market_volume_expansion,
    },

    shared::print_log,

    strategies::Strategy,
};


// ============================================================
// INDICATOR EXECUTION
// ============================================================

// Indicators can be calculated using completed candle history
// (BarClosed) or evaluated on individual market events such as
// incoming trades (TapeTrade) and bid/ask quotes (Quote).
//
// BarClosed:
// Indicators are calculated when a candle closes on the
// configured timeframe. This is generally the preferred
// approach for standard candle-based indicators because
// calculations only occur when new candle data is available.
//
// For normal trading strategies, calculating candle-based
// indicators on BarClosed is generally more efficient,
// especially when processing large amounts of historical
// market data or trading many symbols simultaneously.
//
// RECOMMENDED:
// Calculate standard indicators on BarClosed, while using
// TapeTrade or Quote to evaluate entry and exit conditions
// against the latest calculated indicator values.
//
// This provides the efficiency of candle-based calculations
// while allowing strategies to react immediately to market
// movements without recalculating indicators on every event.
//
// TapeTrade / Quote:
// Indicators can also be evaluated on every incoming trade
// or quote during live trading or historical backtesting.
//
// WARNING:
// Calculating indicators on every TapeTrade or Quote event
// can significantly increase CPU usage, especially when
// processing high-frequency data or many symbols.
//
// WHEN TO USE:
// TapeTrade / Quote evaluation is useful when strategies
// require immediate reactions to market movements or
// intrabar signals.
//
// Note: Indicators using only completed candles will not
// change between candle closes unless their calculations
// also incorporate current trade or quote information.



// ============================================================
// INDICATOR BACKTEST STRATEGY
// ============================================================

pub struct IndicatorBacktestStrategy {
    tx_engine: UnboundedSender<EngineEvent>,
    rx_view: watch::Receiver<Arc<EngineView>>,

    pub enable_strategy: bool,
    pub debug_mode: bool,

    // Stores completed multi-timeframe candles.
    pub candle_history: CandleHistory,

    // Timeframe used for individual indicators.
    pub indicator_timeframe: Timeframe,

    // Timeframe used for cross-asset indicators.
    pub market_indicator_timeframe: Timeframe,

    // Number of one-minute candles used for market statistics.
    pub market_stats_period: usize,

    // Minimum USD volume requirement.
    pub min_volume_usd: f64,
}


// ============================================================
// CONSTRUCTOR
// ============================================================

impl IndicatorBacktestStrategy {
    pub fn new(
        tx_engine: UnboundedSender<EngineEvent>,
        rx_view: watch::Receiver<Arc<EngineView>>,
    ) -> Self {
        let candle_config =
            CandleConfig::default();

        Self {
            tx_engine,
            rx_view,

            enable_strategy: true,
            debug_mode: true,

            candle_history:
                CandleHistory::new(
                    candle_config.history_retention,
                ),

            // Use ONE_MINUTE for testing
            indicator_timeframe:
                Timeframe::ONE_MINUTE,

            // Use ONE_MINUTE for testing
            market_indicator_timeframe:
                Timeframe::ONE_MINUTE,

            // Use 30 for short time period testing and 
            // change can_trade to a ONE_MINUTE timeframe
            // using >= 30. Normal is 1440
            market_stats_period: 30, 

            min_volume_usd: 10_000.0,
        }
    }
}


// ============================================================
// FACTORY
// ============================================================

pub fn make_indicator_backtest(
    tx_engine: UnboundedSender<EngineEvent>,
    rx_view: watch::Receiver<Arc<EngineView>>,
) -> Box<dyn Strategy> {
    Box::new(
        IndicatorBacktestStrategy::new(
            tx_engine,
            rx_view,
        )
    )
}


// ============================================================
// STRATEGY IMPLEMENTATION
// ============================================================

#[async_trait::async_trait]
impl Strategy for IndicatorBacktestStrategy {
    fn name(&self) -> &'static str {
        "IndicatorBacktestStrategy"
    }

    async fn on_event(
        &mut self,
        ev: AppEventEnvelope,
    ) {
        if !self.enable_strategy {
            return;
        }

        match ev.event {

            // ====================================================
            // HISTORICAL TRADE
            // ====================================================

            AppEvent::TapeTrade(trade) => {
                print_log(
                    self.debug_mode,
                    false,
                    format!(
                        "[INDICATOR BACKTEST STRAT TRADE][{}] \
                         price=${:.4} | size={} | timestamp_ms={}",
                        ev.symbol,
                        trade.price,
                        trade.size,
                        trade.timestamp_ms,
                    ),
                );
            }


            // ====================================================
            // HISTORICAL QUOTE
            // ====================================================

            AppEvent::Quote(quote) => {
                print_log(
                    self.debug_mode,
                    false,
                    format!(
                        "[INDICATOR BACKTEST STRAT QUOTE][{}] \
                         bid=${:.4} | ask=${:.4} | timestamp_ms={}",
                        ev.symbol,
                        quote.bid_price,
                        quote.ask_price,
                        quote.timestamp_ms,
                    ),
                );
            }


            // ====================================================
            // HISTORICAL CANDLE
            // ====================================================

            AppEvent::BarClosed(bar) => {
                let timeframe =
                    bar.timeframe;

                let current_ts =
                    bar.end.timestamp_millis();

                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[INDICATOR BACKTEST STRAT BAR CLOSED][{}][{}s] \
                         {} -> {} | \
                         O={:.2} H={:.2} L={:.2} C={:.2} | \
                         V={} | trades={}",
                        ev.symbol,
                        timeframe.as_secs(),
                        bar.start,
                        bar.end,
                        bar.open,
                        bar.high,
                        bar.low,
                        bar.close,
                        bar.volume,
                        bar.trade_count,
                    ),
                );

                // Store the completed candle.
                self.candle_history.push(bar);

                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[INDICATOR BACKTEST STRAT CANDLES][{}][{}s] stored={}",
                        ev.symbol,
                        timeframe.as_secs(),
                        self.candle_history.len(
                            &ev.symbol,
                            timeframe,
                        ),
                    ),
                );


                // ============================================================
                // INDIVIDUAL INDICATORS
                // ============================================================

                // Calculate individual indicators only when their
                // configured candle timeframe closes.

                if timeframe == self.indicator_timeframe {
                    if let Some(bars) =
                        self.candle_history.get(
                            &ev.symbol,
                            self.indicator_timeframe,
                        )
                    {
                        // VecDeque is not guaranteed to be contiguous.
                        let bars_vec: Vec<Bar> =
                            bars.iter()
                                .cloned()
                                .collect();

                        let timeframe_secs =
                            self.indicator_timeframe.as_secs();


                        // ====================================================
                        // SMA
                        // ====================================================

                        if let Some(value) =
                            sma(
                                &bars_vec,
                                20,
                            )
                        {
                            print_log(
                                self.debug_mode,
                                true,
                                format!(
                                    "[INDICATOR BACKTEST STRAT INDICATOR][{}][{}s] SMA20={:.4}",
                                    ev.symbol,
                                    timeframe_secs,
                                    value,
                                ),
                            );
                        }


                        // ====================================================
                        // EMA
                        // ====================================================

                        if let Some(value) =
                            ema(
                                &bars_vec,
                                20,
                            )
                        {
                            print_log(
                                self.debug_mode,
                                true,
                                format!(
                                    "[INDICATOR BACKTEST STRAT INDICATOR][{}][{}s] EMA20={:.4}",
                                    ev.symbol,
                                    timeframe_secs,
                                    value,
                                ),
                            );
                        }


                        // ====================================================
                        // ATR
                        // ====================================================

                        if let Some(value) =
                            atr(
                                &bars_vec,
                                14,
                            )
                        {
                            print_log(
                                self.debug_mode,
                                true,
                                format!(
                                    "[INDICATOR BACKTEST STRAT INDICATOR][{}][{}s] ATR14={:.4}",
                                    ev.symbol,
                                    timeframe_secs,
                                    value,
                                ),
                            );
                        }


                        // ====================================================
                        // ATR PERCENT
                        // ====================================================

                        if let Some(value) =
                            atr_percent(
                                &bars_vec,
                                14,
                            )
                        {
                            print_log(
                                self.debug_mode,
                                true,
                                format!(
                                    "[INDICATOR BACKTEST STRAT INDICATOR][{}][{}s] ATR_PCT14={:.4}%",
                                    ev.symbol,
                                    timeframe_secs,
                                    value,
                                ),
                            );
                        }


                        // ====================================================
                        // REALIZED VOLATILITY
                        // ====================================================

                        if let Some(value) =
                            realized_volatility_pct(
                                &bars_vec,
                                20,
                            )
                        {
                            print_log(
                                self.debug_mode,
                                true,
                                format!(
                                    "[INDICATOR BACKTEST STRAT INDICATOR][{}][{}s] REALIZED_VOL20={:.4}%",
                                    ev.symbol,
                                    timeframe_secs,
                                    value,
                                ),
                            );
                        }


                        // ====================================================
                        // NOISE RATIO
                        // ====================================================

                        if let Some(value) =
                            noise_ratio(
                                &bars_vec,
                                20,
                            )
                        {
                            print_log(
                                self.debug_mode,
                                true,
                                format!(
                                    "[INDICATOR BACKTEST STRAT INDICATOR][{}][{}s] NOISE_RATIO20={:.4}",
                                    ev.symbol,
                                    timeframe_secs,
                                    value,
                                ),
                            );
                        }


                        // ====================================================
                        // LINEAR REGRESSION TREND
                        // ====================================================

                        if let Some(value) =
                            linear_regression_trend(
                                &bars_vec,
                                20,
                            )
                        {
                            print_log(
                                self.debug_mode,
                                true,
                                format!(
                                    "[INDICATOR BACKTEST STRAT INDICATOR][{}][{}s] \
                                     LINEAR_REGRESSION20 | \
                                     SLOPE={:+.4}%/hr | \
                                     R2={:.4}",
                                    ev.symbol,
                                    timeframe_secs,
                                    value.slope_pct_per_hour,
                                    value.r2,
                                ),
                            );
                        }


                        // ====================================================
                        // RELATIVE VOLUME
                        // ====================================================

                        if let Some(value) =
                            relative_volume(
                                &bars_vec,
                                20,
                            )
                        {
                            print_log(
                                self.debug_mode,
                                true,
                                format!(
                                    "[INDICATOR BACKTEST STRAT INDICATOR][{}][{}s] RVOL20={:.2}x",
                                    ev.symbol,
                                    timeframe_secs,
                                    value,
                                ),
                            );
                        }


                        // ====================================================
                        // VOLUME SMA
                        // ====================================================

                        if let Some(value) =
                            volume_sma(
                                &bars_vec,
                                20,
                            )
                        {
                            print_log(
                                self.debug_mode,
                                true,
                                format!(
                                    "[INDICATOR BACKTEST STRAT INDICATOR][{}][{}s] VOLUME_SMA20={:.2}",
                                    ev.symbol,
                                    timeframe_secs,
                                    value,
                                ),
                            );
                        }


                        // ====================================================
                        // VOLUME EMA
                        // ====================================================

                        if let Some(value) =
                            volume_ema(
                                &bars_vec,
                                20,
                            )
                        {
                            print_log(
                                self.debug_mode,
                                true,
                                format!(
                                    "[INDICATOR BACKTEST STRAT INDICATOR][{}][{}s] VOLUME_EMA20={:.2}",
                                    ev.symbol,
                                    timeframe_secs,
                                    value,
                                ),
                            );
                        }


                        // ====================================================
                        // RATE OF CHANGE
                        // ====================================================

                        if let Some(value) =
                            rate_of_change(
                                &bars_vec,
                                20,
                            )
                        {
                            print_log(
                                self.debug_mode,
                                true,
                                format!(
                                    "[INDICATOR BACKTEST STRAT INDICATOR][{}][{}s] ROC20={:.3}%",
                                    ev.symbol,
                                    timeframe_secs,
                                    value,
                                ),
                            );
                        }


                        // ====================================================
                        // RSI
                        // ====================================================

                        if let Some(value) =
                            rsi(
                                &bars_vec,
                                14,
                            )
                        {
                            print_log(
                                self.debug_mode,
                                true,
                                format!(
                                    "[INDICATOR BACKTEST STRAT INDICATOR][{}][{}s] RSI14={:.2}",
                                    ev.symbol,
                                    timeframe_secs,
                                    value,
                                ),
                            );
                        }


                        // ====================================================
                        // MACD
                        // ====================================================

                        if let Some(value) =
                            macd(
                                &bars_vec,
                                12,
                                26,
                                9,
                            )
                        {
                            print_log(
                                self.debug_mode,
                                true,
                                format!(
                                    "[INDICATOR BACKTEST STRAT INDICATOR][{}][{}s] \
                                     MACD={:.4} | SIGNAL={:.4} | HISTOGRAM={:.4}",
                                    ev.symbol,
                                    timeframe_secs,
                                    value.macd,
                                    value.signal,
                                    value.histogram,
                                ),
                            );
                        }


                        // ====================================================
                        // BOLLINGER BANDS
                        // ====================================================

                        if let Some(value) =
                            bollinger_bands(
                                &bars_vec,
                                20,
                                2.0,
                            )
                        {
                            print_log(
                                self.debug_mode,
                                true,
                                format!(
                                    "[INDICATOR BACKTEST STRAT INDICATOR][{}][{}s] \
                                     BOLLINGER | LOWER={:.4} | MID={:.4} | UPPER={:.4} | WIDTH={:.4}",
                                    ev.symbol,
                                    timeframe_secs,
                                    value.lower,
                                    value.middle,
                                    value.upper,
                                    value.width,
                                ),
                            );
                        }


                        // ====================================================
                        // ROLLING HIGH / LOW
                        // ====================================================

                        if let Some(value) =
                            rolling_high_low(
                                &bars_vec,
                                20,
                            )
                        {
                            print_log(
                                self.debug_mode,
                                true,
                                format!(
                                    "[INDICATOR BACKTEST STRAT INDICATOR][{}][{}s] \
                                     HIGH_LOW20 | HIGH={:.4} | LOW={:.4} | RANGE={:.4}",
                                    ev.symbol,
                                    timeframe_secs,
                                    value.high,
                                    value.low,
                                    value.range,
                                ),
                            );
                        }


                        // ====================================================
                        // CANDLE STRENGTH
                        // ====================================================

                        if let Some(value) =
                            candle_strength(
                                &bars_vec,
                                20,
                            )
                        {
                            print_log(
                                self.debug_mode,
                                true,
                                format!(
                                    "[INDICATOR BACKTEST STRAT INDICATOR][{}][{}s] \
                                     CANDLE_STRENGTH20 | \
                                     AVG_RANGE={:.4} | \
                                     AVG_BODY={:.4} | \
                                     AVG_BODY_PCT={:.3}% | \
                                     LATEST_BODY_RANGE={:.3} | \
                                     LATEST_CLOSE_POSITION={:.3}",
                                    ev.symbol,
                                    timeframe_secs,
                                    value.average_range,
                                    value.average_body,
                                    value.average_body_percent,
                                    value.latest_body_to_range,
                                    value.latest_close_position,
                                ),
                            );
                        }


                        // ====================================================
                        // SESSION VWAP
                        // ====================================================

                        // VWAP uses exact raw-trade price * volume
                        // accumulation and only candles from the
                        // current regular market session.

                        if let Some(latest_bar) =
                            bars_vec.last()
                        {
                            if let Some((
                                session_start,
                                session_end,
                            )) =
                                regular_market_session(
                                    latest_bar.end,
                                )
                            {
                                if let Some(value) =
                                    vwap_between(
                                        &bars_vec,
                                        session_start,
                                        session_end,
                                    )
                                {
                                    print_log(
                                        self.debug_mode,
                                        true,
                                        format!(
                                            "[INDICATOR BACKTEST STRAT INDICATOR][{}][{}s] SESSION_VWAP={:.4}",
                                            ev.symbol,
                                            timeframe_secs,
                                            value,
                                        ),
                                    );
                                }
                            }
                        }
                    }
                }


                // ============================================================
                // CROSS-ASSET MARKET INDICATORS
                // ============================================================

                if timeframe == self.market_indicator_timeframe {
                    let cross_asset_config =
                        CrossAssetConfig::default();

                    let min_sync_pct =
                        cross_asset_config.min_sync_pct;

                    let symbols = {
                        let view =
                            self.rx_view.borrow();

                        view.cfg.symbols.clone()
                    };

                    let timeframe_secs =
                        self.market_indicator_timeframe.as_secs();


                    // ====================================================
                    // MARKET VOLUME EXPANSION
                    // ====================================================

                    if let Some(value) =
                        market_volume_expansion(
                            &self.candle_history,
                            &symbols,
                            self.market_indicator_timeframe,
                            20,
                            1.5,
                            min_sync_pct,
                        )
                    {
                        print_log(
                            self.debug_mode,
                            true,
                            format!(
                                "[INDICATOR BACKTEST STRAT MARKET INDICATOR][{}s] \
                                 VOLUME_EXPANSION | \
                                 AVG_RVOL={:.2}x | \
                                 EXPANDING={:.1}% | \
                                 ASSETS={}/{} | \
                                 SYNC={:.1}% | \
                                 END={}",
                                timeframe_secs,
                                value.average_rvol,
                                value.expanding_assets_pct,
                                value.assets_used,
                                value.assets_total,
                                value.sync_pct,
                                value.snapshot_end,
                            ),
                        );
                    }


                    // ====================================================
                    // MARKET VOLUME ACCELERATION
                    // ====================================================

                    if let Some(value) =
                        market_volume_acceleration(
                            &self.candle_history,
                            &symbols,
                            self.market_indicator_timeframe,
                            20,
                            min_sync_pct,
                        )
                    {
                        print_log(
                            self.debug_mode,
                            true,
                            format!(
                                "[INDICATOR BACKTEST STRAT MARKET INDICATOR][{}s] \
                                 VOLUME_ACCELERATION | \
                                 CURRENT_RVOL={:.2}x | \
                                 PREVIOUS_RVOL={:.2}x | \
                                 CHANGE={:+.2} | \
                                 CHANGE_PCT={:+.1}% | \
                                 ACCELERATING={:.1}% | \
                                 ASSETS={}/{} | \
                                 SYNC={:.1}% | \
                                 END={}",
                                timeframe_secs,
                                value.current_average_rvol,
                                value.previous_average_rvol,
                                value.rvol_change,
                                value.rvol_change_pct,
                                value.accelerating_assets_pct,
                                value.assets_used,
                                value.assets_total,
                                value.sync_pct,
                                value.snapshot_end,
                            ),
                        );
                    }


                    // ====================================================
                    // MARKET BREADTH
                    // ====================================================

                    if let Some(value) =
                        market_breadth(
                            &self.candle_history,
                            &symbols,
                            self.market_indicator_timeframe,
                            20,
                            min_sync_pct,
                        )
                    {
                        print_log(
                            self.debug_mode,
                            true,
                            format!(
                                "[INDICATOR BACKTEST STRAT MARKET INDICATOR][{}s] \
                                 BREADTH20 | \
                                 BULLISH={:.1}% | \
                                 BEARISH={:.1}% | \
                                 NEUTRAL={:.1}% | \
                                 AVG_MOVE={:+.3}% | \
                                 ASSETS={}/{} | \
                                 SYNC={:.1}% | \
                                 END={}",
                                timeframe_secs,
                                value.bullish_assets_pct,
                                value.bearish_assets_pct,
                                value.neutral_assets_pct,
                                value.average_move_pct,
                                value.assets_used,
                                value.assets_total,
                                value.sync_pct,
                                value.snapshot_end,
                            ),
                        );
                    }
                }


                // ============================================================
                // 24-HOUR MARKET STATISTICS
                // ============================================================

                // Recalculate when a completed one-minute candle
                // becomes available.

                if timeframe == Timeframe::ONE_MINUTE {
                    let period =
                        self.market_stats_period;

                    if let Some(bars) =
                        self.candle_history.get(
                            &ev.symbol,
                            Timeframe::ONE_MINUTE,
                        )
                    {
                        let bars_vec: Vec<Bar> =
                            bars.iter()
                                .cloned()
                                .collect();

                        let price_change_24h =
                            price_pct_change(
                                &bars_vec,
                                period,
                                current_ts,
                            );

                        let volume_asset_24h =
                            volume_asset(
                                &bars_vec,
                                period,
                                current_ts,
                            );

                        let volume_usd_24h =
                            volume_usd(
                                &bars_vec,
                                period,
                                current_ts,
                            );

                        if let (
                            Some(price_change_24h),
                            Some(volume_asset_24h),
                            Some(volume_usd_24h),
                        ) = (
                            price_change_24h,
                            volume_asset_24h,
                            volume_usd_24h,
                        ) {
                            print_log(
                                self.debug_mode,
                                true,
                                format!(
                                    "[INDICATOR BACKTEST STRAT 24H MARKET STATS][{}] \
                                     CHANGE={:+.4}% | \
                                     ASSET_VOLUME={:.4} | \
                                     USD_VOLUME=${:.2}",
                                    ev.symbol,
                                    price_change_24h,
                                    volume_asset_24h,
                                    volume_usd_24h,
                                ),
                            );
                        }


                        // ====================================================
                        // TRADING REQUIREMENTS
                        // ====================================================

                        // let can_trade =
                        //     self.candle_history.len(
                        //         &ev.symbol,
                        //         Timeframe::ONE_HOUR,
                        //     ) >= 24;

                        // Short 30 minute version for testing
                        let can_trade =
                            self.candle_history.len(
                                &ev.symbol,
                                Timeframe::ONE_MINUTE,
                            ) >= 30;

                        let has_enough_volume =
                            volume_usd_24h.is_some_and(
                                |usd_volume| {
                                    usd_volume >= self.min_volume_usd
                                }
                            );

                        let requirements_met =
                            can_trade && has_enough_volume;

                        print_log(
                            self.debug_mode,
                            true,
                            format!(
                                "[INDICATOR BACKTEST STRAT REQUIREMENTS][{}] \
                                CAN_TRADE={} | \
                                ENOUGH_VOLUME={} | \
                                REQUIREMENTS_MET={} | \
                                CURRENT_VOLUME_USD={} | \
                                MIN_VOLUME_USD=${:.2}",
                                ev.symbol,
                                can_trade,
                                has_enough_volume,
                                requirements_met,
                                volume_usd_24h
                                    .map(|v| format!("${:.2}", v))
                                    .unwrap_or_else(|| "N/A".to_string()),
                                self.min_volume_usd,
                            ),
                        );
                    }
                }
            }
        }
    }
}