#![allow(dead_code)]

use std::{
    collections::HashMap,
    sync::Arc,
};

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

    indicators::{
        price_pct_change,
        volume_usd,
        relative_volume,
    },

    market::{
        candle::CandleConfig,
        candle_history::CandleHistory,
        timeframe::Timeframe,
    },

    strategies::Strategy,

    toolbox::{
        TradeCooldown,
        TradeDurationWatchdog,
        black_swan_protection::BlackSwanProtection,
    },

    shared::print_log,
};

use crate::{
    core::{
        trade_intent::TradeIntent,
        trades::Direction,
       
        execution::order::{
            OrderType,
            TimeInForce,
        },
    },
};

use super::backtest_state::{
    ResearchV4BacktestStrategy,
    TrendingConfig,
    MeanReversionConfig,
    MeanReversionRange,
    MeanReversionBoundary,
    MeanReversionBoundaryMonitor,
};

// ============================================================
// RESEARCH V4 DESCRIPTION
// ============================================================
//
// Research V4 is a market-structure-based strategy supporting
// two independent trading systems:
//
// 1. Trend Following
//    - Detect established trends.
//    - Identify temporary pullbacks.
//    - Confirm trend continuation.
//    - Enter in the direction of the trend.
//
// 2. Mean Reversion
//    - Detect established trading ranges.
//    - Validate range boundaries across multiple windows.
//    - Identify price approaching range boundaries.
//    - Confirm rejection from the boundary.
//    - Enter toward the opposite side of the range.
//
// Both systems support multiple independent configurations.
//
// All trades must pass market qualification and strategy
// risk-management requirements before execution.
//
// ============================================================


// ============================================================
// TODO — FUTURE DEVELOPMENT
// ============================================================
//
// - Assign a unique config_id to every TrendingConfig and
//   MeanReversionConfig.
//
// - Track individual trading setups using (symbol, config_id)
//   rather than only the asset symbol.
//
// - Allow multiple configurations to independently monitor
//   and evaluate the same asset simultaneously.
//
// - Enforce one active trade per symbol across all Research V4
//   configurations, regardless of strategy type.
//
// - Add setup prioritization when multiple configurations
//   generate valid entry signals for the same asset.
//
// - Add advanced opposite-direction trading research after
//   the baseline trending and mean-reversion systems have
//   been thoroughly validated.
//
// - Integrate additional toolbox features after establishing
//   consistent baseline backtesting performance.
//
// ============================================================




impl ResearchV4BacktestStrategy {
    pub fn new(
        tx_engine: UnboundedSender<EngineEvent>,
        rx_view: watch::Receiver<Arc<EngineView>>,
    ) -> Self {
        let candle_config = CandleConfig::default();

        Self {
            // ====================================================
            // GENERAL
            // ====================================================

            enable_strategy: true,
            debug_mode: true,
            tx_engine,
            rx_view,

            // ====================================================
            // CAPITAL / EXECUTION
            // ====================================================

            max_trades_allowed: 8,
            reference_margin_usd: 1_600.0,
            trade_amount_usd: 200.0,

            leverage: None,
            emergency_exit_percent: Some(5.0),
            min_sl_delay: Some(0),

            // ====================================================
            // MARKET QUALIFICATION
            // ====================================================

            enable_trading_eligibility_check: false,
            min_volume_usd: Some(350_000.0),
            min_24h_move_pct: Some(3.0),
            max_24h_move_pct: Some(15.0),

            // Disabled initially for baseline testing.
            min_relative_volume: None,
            relative_volume_period: 20,

            can_trade: HashMap::new(),

            // ====================================================
            // TREND FOLLOWING CONFIGURATIONS
            // ====================================================

            trending_configs: vec![
                TrendingConfig {
                    enabled: true,
                    trend_window_secs: 4 * 3600,
                    max_pullback_secs: 2 * 3600,
                    min_trend_move_pct: 3.0,
                    min_pullback_pct: 0.5,
                    confirmation_pct: 0.3,
                    enable_confirmations: true,
                    take_profit_pct: Some(3.0),
                    stop_loss_pct: Some(2.0),
                    trade_duration_secs: Some(4800),
                },
            ],

            // ====================================================
            // MEAN REVERSION CONFIGURATIONS
            // ====================================================

            mean_reversion_configs: vec![
                MeanReversionConfig {
                    enabled: true,
                    short_window_secs: 2 * 3600,
                    long_window_secs: 4 * 3600,
                    min_range_width_pct: 4.0,
                    max_range_width_pct: 8.0,
                    max_range_boundary_diff_pct: 1.0,
                    min_confirmed_reversals: 3,
                    min_reversal_fraction: 0.60,
                    boundary_proximity_pct: 15.0, 
                    confirmation_pct: 0.3,
                    enable_confirmations: true,
                    range_profit_target_fraction: 0.70,
                    boundary_break_buffer_pct: 0.5,
                    stop_loss_boundary_buffer_pct: 0.5,
                    trade_duration_secs: Some(4800),
                },
            ],

            mean_reversion_ranges: HashMap::new(),

            mean_reversion_boundary_monitors: HashMap::new(),

            // ====================================================
            // MARKET DATA
            // ====================================================

            candle_history: CandleHistory::new(
                candle_config.history_retention,
            ),

            // ====================================================
            // RISK / TOOLBOX
            // ====================================================

            black_swan_protection: BlackSwanProtection::new(
                false,
                false,
                15.0,
                8.0,
                15,
            ),

            trade_cooldown: TradeCooldown::new(200),

            trade_duration_watchdog: TradeDurationWatchdog::new(
                false,
                10.0,
                300_000,
            ),
        }
    }
}


// ============================================================
// FACTORY
// ============================================================

pub fn make_research_v4_backtest(
    tx_engine: UnboundedSender<EngineEvent>,
    rx_view: watch::Receiver<Arc<EngineView>>,
) -> Box<dyn Strategy> {
    Box::new(
        ResearchV4BacktestStrategy::new(
            tx_engine,
            rx_view,
        )
    )
}


// ============================================================
// STRATEGY IMPLEMENTATION
// ============================================================

#[async_trait::async_trait]
impl Strategy for ResearchV4BacktestStrategy {
    fn name(&self) -> &'static str {
        "ResearchV4BacktestStrategy"
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
                if self.debug_mode {
                    print_log(
                        self.debug_mode,
                        false,
                        format_args!(
                            "[RESEARCH V4 BACKTEST TRADE][{}] \
                            price=${:.4} | size={} | timestamp_ms={}",
                            ev.symbol,
                            trade.price,
                            trade.size,
                            trade.timestamp_ms,
                        ),
                    );
                }

                // ====================================================
                // TRADING ELIGIBILITY
                // ====================================================

                // Read cached eligibility for this symbol.
                // When eligibility checking is disabled, all symbols
                // are considered eligible.
                let can_trade = if self.enable_trading_eligibility_check {
                    self.can_trade
                        .get(&ev.symbol)
                        .copied()
                        .unwrap_or(false)
                } else {
                    true
                };

                // ====================================================
                // MONITOR MEAN REVERSION BOUNDARIES
                // ====================================================

                // Monitor tick prices approaching preserved
                // range boundaries.
                //
                // Lower boundary -> Potential LONG setup.
                // Upper boundary -> Potential SHORT setup.
                //
                // No trade execution occurs at this stage.
                self.monitor_mean_reversion_boundaries(
                    &ev.symbol,
                    trade.price,
                    trade.timestamp_ms,
                );

                // ====================================================
                // EXECUTE CONFIRMED MEAN REVERSION ENTRY
                // ====================================================

                self.execute_mean_reversion_entry(
                    &ev.symbol,
                    trade.price,
                    trade.timestamp_ms,
                    can_trade,
                );

                // TODO:
                // 1. Update active trending setups.
                // 2. Confirm mean-reversion reversals.
                // 3. Evaluate entry confirmations.
                // 4. Apply trading eligibility (can_trade).
                // 5. Validate risk/reward.
                // 6. Submit trade intent.
            }

            // ====================================================
            // HISTORICAL QUOTE
            // ====================================================

            AppEvent::Quote(quote) => {
                if self.debug_mode {
                    print_log(
                        self.debug_mode,
                        false,
                        format_args!(
                            "[RESEARCH V4 BACKTEST QUOTE][{}] \
                            bid=${:.4} | ask=${:.4}",
                            ev.symbol,
                            quote.bid_price,
                            quote.ask_price,
                        ),
                    );
                }
            }

            // ====================================================
            // HISTORICAL CANDLE
            // ====================================================

            AppEvent::BarClosed(bar) => {
                let timeframe = bar.timeframe;
                let current_ts = bar.end.timestamp_millis();

                // Store completed candle.
                self.candle_history.push(bar);

                // ====================================================
                // UPDATE TRADING ELIGIBILITY
                // ====================================================

                self.update_trading_eligibility(
                    &ev.symbol,
                    timeframe,
                    current_ts,
                );

                // ====================================================
                // UPDATE MEAN REVERSION RANGES
                // ====================================================

                self.update_mean_reversion_ranges(
                    &ev.symbol,
                    timeframe,
                    current_ts,
                );

                // TODO:
                // 1. Update trend indicators.
                // 2. Update BTC volatility protection.
                // 3. Detect and invalidate entry setups.

                if self.debug_mode {
                    print_log(
                        self.debug_mode,
                        false,
                        format_args!(
                            "[RESEARCH V4 BACKTEST CANDLE][{}][{}s] \
                            stored={} | can_trade={} | range_detected={}",
                            ev.symbol,
                            timeframe.as_secs(),
                            self.candle_history.len(
                                &ev.symbol,
                                timeframe,
                            ),
                            self.can_trade
                                .get(&ev.symbol)
                                .copied()
                                .unwrap_or(false),
                            self.mean_reversion_ranges
                                .contains_key(&ev.symbol),
                        ),
                    );
                }
            }
        }
    }
}





impl ResearchV4BacktestStrategy {

    // ============================================================
    // UPDATE TRADING ELIGIBILITY
    // ============================================================

    // Updates trading eligibility for each asset using validated
    // 24-hour market statistics and optional recent relative volume.
    //
    // Runs on completed one-minute candles.
    //
    // Checks:
    // - At least 1440 completed one-minute candles.
    // - Valid 24-hour USD trading volume.
    // - Minimum 24-hour USD volume (if enabled).
    // - Valid 24-hour percentage price movement.
    // - Minimum absolute 24-hour movement (if enabled).
    // - Maximum absolute 24-hour movement (if enabled).
    // - Minimum recent relative volume (if enabled).
    //
    // Caches the final eligibility result per symbol.
    // Trading is allowed only when all enabled checks pass.
    fn update_trading_eligibility(
        &mut self,
        symbol: &str,
        timeframe: Timeframe,
        current_ts: i64,
    ) {
        // Only update on completed one-minute candles.
        if timeframe != Timeframe::ONE_MINUTE {
            return;
        }

        // Number of one-minute candles in 24 hours.
        let period: usize = 1440;

        // Default to ineligible until all requirements pass.
        let mut requirements_met = false;

        // ============================================================
        // GET ONE-MINUTE CANDLE HISTORY
        // ============================================================

        if let Some(bars) = self.candle_history.get(
            symbol,
            Timeframe::ONE_MINUTE,
        ) {
            if bars.len() >= period {

                // Collect the latest 24 hours in chronological order.
                let bars_vec: Vec<Bar> = bars
                    .iter()
                    .rev()
                    .take(period)
                    .cloned()
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect();

                // ====================================================
                // VALIDATED 24-HOUR INDICATORS
                // ====================================================

                let volume_usd_24h = volume_usd(
                    &bars_vec,
                    period,
                    current_ts,
                );

                let price_change_24h = price_pct_change(
                    &bars_vec,
                    period,
                    current_ts,
                );

                // ====================================================
                // RECENT RELATIVE VOLUME
                // ====================================================

                // Only calculate RVOL when the filter is enabled.
                //
                // Pass the full recent 24-hour candle history so the
                // indicator has sufficient observations.
                let recent_rvol = if self.min_relative_volume.is_some() {
                    relative_volume(
                        &bars_vec,
                        self.relative_volume_period,
                    )
                } else {
                    None
                };

                // ====================================================
                // MINIMUM 24-HOUR USD VOLUME
                // ====================================================

                let has_enough_volume = match self.min_volume_usd {
                    Some(min_volume) => {
                        volume_usd_24h.is_some_and(|usd_volume| {
                            usd_volume >= min_volume
                        })
                    }

                    None => true,
                };

                // ====================================================
                // MINIMUM 24-HOUR PRICE MOVEMENT
                // ====================================================

                let meets_min_move = match self.min_24h_move_pct {
                    Some(min_pct) => {
                        price_change_24h.is_some_and(|change_pct| {
                            change_pct.abs() >= min_pct
                        })
                    }

                    None => true,
                };

                // ====================================================
                // MAXIMUM 24-HOUR PRICE MOVEMENT
                // ====================================================

                let within_max_move = match self.max_24h_move_pct {
                    Some(max_pct) => {
                        price_change_24h.is_some_and(|change_pct| {
                            change_pct.abs() <= max_pct
                        })
                    }

                    None => true,
                };

                // ====================================================
                // MINIMUM RECENT RELATIVE VOLUME
                // ====================================================

                let has_enough_relative_volume =
                    match self.min_relative_volume {
                        Some(min_rvol) => {
                            recent_rvol.is_some_and(|rvol| {
                                rvol.is_finite()
                                    && rvol >= min_rvol
                            })
                        }

                        None => true,
                    };

                // ====================================================
                // FINAL TRADING REQUIREMENTS
                // ====================================================

                requirements_met =
                    volume_usd_24h.is_some()
                    && price_change_24h.is_some()
                    && has_enough_volume
                    && meets_min_move
                    && within_max_move
                    && has_enough_relative_volume;

                // ====================================================
                // MARKET STATISTICS DEBUG
                // ====================================================

                if self.debug_mode {
                    print_log(
                        self.debug_mode,
                        false,
                        format_args!(
                            "[RESEARCH V4 BACKTEST MARKET STATS][{}] \
                             CHANGE={:?}% | \
                             USD_VOLUME={:?} | \
                             RVOL={:?} | \
                             ENOUGH_VOLUME={} | \
                             MIN_MOVE={} | \
                             MAX_MOVE={} | \
                             ENOUGH_RVOL={} | \
                             CAN_TRADE={}",
                            symbol,
                            price_change_24h,
                            volume_usd_24h,
                            recent_rvol,
                            has_enough_volume,
                            meets_min_move,
                            within_max_move,
                            has_enough_relative_volume,
                            requirements_met,
                        ),
                    );
                }
            }
        }

        // ============================================================
        // CACHE TRADING ELIGIBILITY
        // ============================================================

        self.can_trade.insert(
            symbol.to_string(),
            requirements_met,
        );

        // ============================================================
        // ELIGIBILITY DEBUG
        // ============================================================

        if self.debug_mode {
            print_log(
                self.debug_mode,
                false,
                format_args!(
                    "[RESEARCH V4 BACKTEST ELIGIBILITY][{}] \
                     can_trade={}",
                    symbol,
                    requirements_met,
                ),
            );
        }
    }
}




impl ResearchV4BacktestStrategy {

    // ============================================================
    // UPDATE MEAN REVERSION RANGES
    // ============================================================

    // Detects and preserves established mean-reversion ranges.
    //
    // Range lifecycle:
    // 1. Discover a valid range using rolling candle history.
    // 2. Validate range width and boundary consistency.
    // 3. Validate confirmed oscillations and reversals.
    // 4. Preserve the validated range boundaries.
    // 5. Monitor the preserved range for breakouts.
    // 6. Expire the range after its configured lifetime.
    //
    // Once validated, boundaries remain fixed until the range
    // expires or is invalidated by a breakout.
    //
    // Runs once per completed one-minute candle.
    fn update_mean_reversion_ranges(
        &mut self,
        symbol: &str,
        timeframe: Timeframe,
        current_ts: i64,
    ) {
        if timeframe != Timeframe::ONE_MINUTE {
            return;
        }

        // Baseline: use the first enabled configuration.
        // TODO: Evaluate all enabled configurations independently.
        let Some(config) = self.mean_reversion_configs
            .iter()
            .find(|config| config.enabled)
        else {
            self.mean_reversion_ranges.remove(symbol);
            self.mean_reversion_boundary_monitors.remove(symbol);
            return;
        };

        let short_period =
            (config.short_window_secs / 60) as usize;

        let long_period =
            (config.long_window_secs / 60) as usize;

        if short_period == 0
            || long_period == 0
            || short_period > long_period
        {
            self.mean_reversion_ranges.remove(symbol);
            self.mean_reversion_boundary_monitors.remove(symbol);
            return;
        }

        // ====================================================
        // GET CANDLE HISTORY
        // ====================================================

        let Some(bars) = self.candle_history.get(
            symbol,
            Timeframe::ONE_MINUTE,
        ) else {
            self.mean_reversion_ranges.remove(symbol);
            self.mean_reversion_boundary_monitors.remove(symbol);
            return;
        };

        if bars.len() < long_period {
            self.mean_reversion_ranges.remove(symbol);
            self.mean_reversion_boundary_monitors.remove(symbol);
            return;
        }

        // ====================================================
        // CHECK EXISTING VALIDATED RANGE
        // ====================================================

        if let Some(existing_range) =
            self.mean_reversion_ranges.get(symbol)
        {
            let latest_bar = bars.back().unwrap();

            // ====================================================
            // CHECK RANGE EXPIRATION
            // ====================================================

            let elapsed_ms =
                current_ts.saturating_sub(
                    existing_range.detected_ts_ms,
                );

            let max_lifetime_ms =
                (config.long_window_secs as i64)
                    .saturating_mul(1000);

            let expired =
                elapsed_ms >= max_lifetime_ms;

            // ====================================================
            // CHECK RANGE BREAKOUT
            // ====================================================

            let breakout_buffer =
                config.boundary_break_buffer_pct / 100.0;

            let upper_breakout_price =
                existing_range.upper_boundary
                    * (1.0 + breakout_buffer);

            let lower_breakout_price =
                existing_range.lower_boundary
                    * (1.0 - breakout_buffer);

            // Use completed candle close to confirm a breakout.
            // Temporary intrabar wicks do not invalidate a range.
            let breakout_above =
                latest_bar.close > upper_breakout_price;

            let breakout_below =
                latest_bar.close < lower_breakout_price;

            let breakout =
                breakout_above || breakout_below;

            // ====================================================
            // PRESERVE VALID RANGE
            // ====================================================

            if !expired && !breakout {
                // The validated range remains active.
                // Its original boundaries are preserved.
                return;
            }

            // ====================================================
            // INVALIDATE EXISTING RANGE
            // ====================================================

            if self.debug_mode {
                print_log(
                    self.debug_mode,
                    true,
                    format_args!(
                        "[RESEARCH V4 BACKTEST RANGE INVALIDATED][{}] \
                         REASON={} | \
                         LOW=${:.4} | HIGH=${:.4} | \
                         AGE={}s",
                        symbol,
                        if breakout {
                            "BREAKOUT"
                        } else {
                            "EXPIRED"
                        },
                        existing_range.lower_boundary,
                        existing_range.upper_boundary,
                        elapsed_ms / 1000,
                    ),
                );
            }

            // Remove the validated range and any pending
            // boundary approach associated with that range.
            self.mean_reversion_ranges.remove(symbol);
            self.mean_reversion_boundary_monitors.remove(symbol);

            // Do not immediately establish a replacement range
            // using the same candle that invalidated the old one.
            //
            // The next completed candle can initiate discovery.
            return;
        }

        // ====================================================
        // COLLECT ROLLING CANDLE HISTORY
        // ====================================================

        let bars_vec: Vec<Bar> = bars
            .iter()
            .rev()
            .take(long_period)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();

        // ====================================================
        // CALCULATE LONG-WINDOW BOUNDARIES
        // ====================================================

        let long_range_high = bars_vec
            .iter()
            .map(|bar| bar.high)
            .fold(f64::NEG_INFINITY, f64::max);

        let long_range_low = bars_vec
            .iter()
            .map(|bar| bar.low)
            .fold(f64::INFINITY, f64::min);

        // ====================================================
        // CALCULATE SHORT-WINDOW BOUNDARIES
        // ====================================================

        let short_bars = &bars_vec[
            bars_vec.len() - short_period..
        ];

        let short_range_high = short_bars
            .iter()
            .map(|bar| bar.high)
            .fold(f64::NEG_INFINITY, f64::max);

        let short_range_low = short_bars
            .iter()
            .map(|bar| bar.low)
            .fold(f64::INFINITY, f64::min);

        // ====================================================
        // VALIDATE PRICE BOUNDARIES
        // ====================================================

        if !long_range_high.is_finite()
            || !long_range_low.is_finite()
            || long_range_low <= 0.0
            || long_range_high <= long_range_low
        {
            return;
        }

        // ====================================================
        // CALCULATE RANGE WIDTH
        // ====================================================

        let range_width_pct =
            ((long_range_high - long_range_low)
                / long_range_low) * 100.0;

        if range_width_pct < config.min_range_width_pct
            || range_width_pct > config.max_range_width_pct
        {
            return;
        }

        // ====================================================
        // COMPARE RANGE BOUNDARIES
        // ====================================================

        let upper_boundary_diff_pct =
            ((short_range_high - long_range_high).abs()
                / long_range_high) * 100.0;

        let lower_boundary_diff_pct =
            ((short_range_low - long_range_low).abs()
                / long_range_low) * 100.0;

        let boundaries_match =
            upper_boundary_diff_pct
                <= config.max_range_boundary_diff_pct
            && lower_boundary_diff_pct
                <= config.max_range_boundary_diff_pct;

        if !boundaries_match {
            return;
        }

        // ====================================================
        // VALIDATE RANGE OSCILLATIONS
        // ====================================================

        let valid_oscillations =
            Self::validate_mean_reversion_oscillations(
                &bars_vec,
                long_range_low,
                long_range_high,
                config.boundary_proximity_pct,
                config.min_reversal_fraction,
                config.min_confirmed_reversals,
            );

        if !valid_oscillations {
            return;
        }

        // ====================================================
        // CACHE VALIDATED RANGE
        // ====================================================

        let detected_range = MeanReversionRange {
            upper_boundary: long_range_high,
            lower_boundary: long_range_low,
            range_width_pct,
            short_range_high,
            short_range_low,
            long_range_high,
            long_range_low,
            detected_ts_ms: current_ts,
        };

        self.mean_reversion_ranges.insert(
            symbol.to_string(),
            detected_range,
        );

        // Clear any stale boundary monitoring state.
        self.mean_reversion_boundary_monitors.remove(symbol);

        // ====================================================
        // RANGE DETECTION DEBUG
        // ====================================================

        if self.debug_mode {
            print_log(
                self.debug_mode,
                true,
                format_args!(
                    "[RESEARCH V4 BACKTEST RANGE VALIDATED][{}] \
                     LOW=${:.4} | HIGH=${:.4} | \
                     WIDTH={:.3}% | \
                     SHORT_LOW=${:.4} | SHORT_HIGH=${:.4} | \
                     LONG_LOW=${:.4} | LONG_HIGH=${:.4} | \
                     LIFETIME={}s",
                    symbol,
                    long_range_low,
                    long_range_high,
                    range_width_pct,
                    short_range_low,
                    short_range_high,
                    long_range_low,
                    long_range_high,
                    config.long_window_secs,
                ),
            );
        }
    }
}




impl ResearchV4BacktestStrategy {

    // ============================================================
    // VALIDATE MEAN REVERSION OSCILLATIONS
    // ============================================================

    // Confirms that price has demonstrated meaningful reversals
    // from both boundaries of an established trading range.
    //
    // Validation:
    // - Requires at least one confirmed upper-boundary reversal.
    // - Requires at least one confirmed lower-boundary reversal.
    // - Requires the configured minimum number of reversals.
    // - Counts only completed reversals.
    // - Prevents repeated counting of the same boundary interaction.
    // - Rejects directional movement without sufficient reversals.
    //
    // Boundary proximity is calculated as a percentage of the
    // established range width rather than the asset price.
    //
    // Uses completed one-minute candles in chronological order.
    fn validate_mean_reversion_oscillations(
        bars: &[Bar],
        lower_boundary: f64,
        upper_boundary: f64,
        boundary_proximity_pct: f64,
        min_reversal_fraction: f64,
        min_confirmed_reversals: usize,
    ) -> bool {
        if bars.is_empty()
            || !lower_boundary.is_finite()
            || !upper_boundary.is_finite()
            || lower_boundary <= 0.0
            || upper_boundary <= lower_boundary
            || !boundary_proximity_pct.is_finite()
            || !(0.0..=50.0).contains(&boundary_proximity_pct)
            || !min_reversal_fraction.is_finite()
            || min_reversal_fraction <= 0.0
            || min_reversal_fraction > 1.0
            || min_confirmed_reversals < 2
        {
            return false;
        }

        // ====================================================
        // CALCULATE REVERSAL REQUIREMENTS
        // ====================================================

        let range_width = upper_boundary - lower_boundary;

        let required_reversal =
            range_width * min_reversal_fraction;

        // Calculate boundary proximity as a percentage
        // of the established range width.
        //
        // Example:
        // Range: $100 - $106
        // Width: $6
        // Proximity: 20%
        //
        // Lower zone: $100 - $101.20
        // Upper zone: $104.80 - $106
        let boundary_proximity =
            range_width * (boundary_proximity_pct / 100.0);

        // ====================================================
        // TRACK BOUNDARY INTERACTIONS
        // ====================================================

        // 0 = waiting for a boundary interaction.
        // 1 = tracking a reversal from the lower boundary.
        // 2 = tracking a reversal from the upper boundary.
        let mut active_boundary: u8 = 0;

        let mut boundary_extreme = 0.0_f64;

        let mut confirmed_reversals = 0_usize;
        let mut lower_reversals = 0_usize;
        let mut upper_reversals = 0_usize;

        // Prevent consecutive reversals from being counted
        // from the same boundary without visiting the opposite
        // boundary first.
        let mut last_confirmed_boundary: u8 = 0;

        // ====================================================
        // PROCESS CANDLES CHRONOLOGICALLY
        // ====================================================

        for bar in bars {
            if !bar.high.is_finite()
                || !bar.low.is_finite()
                || !bar.close.is_finite()
                || bar.low <= 0.0
                || bar.high < bar.low
                || bar.close <= 0.0
                || bar.close < bar.low
                || bar.close > bar.high
            {
                return false;
            }

            let near_lower =
                bar.low <= lower_boundary + boundary_proximity;

            let near_upper =
                bar.high >= upper_boundary - boundary_proximity;

            match active_boundary {

                // ============================================
                // WAITING FOR A BOUNDARY INTERACTION
                // ============================================

                0 => {
                    // A candle touching both boundaries is
                    // ambiguous without intrabar ordering.
                    if near_lower && near_upper {
                        continue;
                    }

                    if near_lower
                        && last_confirmed_boundary != 1
                    {
                        active_boundary = 1;
                        boundary_extreme = bar.low;
                    } else if near_upper
                        && last_confirmed_boundary != 2
                    {
                        active_boundary = 2;
                        boundary_extreme = bar.high;
                    }
                }

                // ============================================
                // LOWER BOUNDARY REVERSAL
                // ============================================

                1 => {
                    // Track the lowest price reached.
                    boundary_extreme =
                        boundary_extreme.min(bar.low);

                    // Require price to recover sufficiently
                    // from the observed lower extreme.
                    //
                    // Use the candle close for confirmation.
                    if bar.close - boundary_extreme
                        >= required_reversal
                    {
                        confirmed_reversals += 1;
                        lower_reversals += 1;

                        last_confirmed_boundary = 1;
                        active_boundary = 0;
                    }
                }

                // ============================================
                // UPPER BOUNDARY REVERSAL
                // ============================================

                2 => {
                    // Track the highest price reached.
                    boundary_extreme =
                        boundary_extreme.max(bar.high);

                    // Require price to decline sufficiently
                    // from the observed upper extreme.
                    //
                    // Use the candle close for confirmation.
                    if boundary_extreme - bar.close
                        >= required_reversal
                    {
                        confirmed_reversals += 1;
                        upper_reversals += 1;

                        last_confirmed_boundary = 2;
                        active_boundary = 0;
                    }
                }

                _ => {}
            }
        }

        // ====================================================
        // FINAL OSCILLATION VALIDATION
        // ====================================================

        confirmed_reversals >= min_confirmed_reversals
            && lower_reversals >= 1
            && upper_reversals >= 1
    }
}




impl ResearchV4BacktestStrategy {

    // ============================================================
    // MONITOR MEAN REVERSION RANGE BOUNDARIES
    // ============================================================

    // Monitors tick prices approaching preserved range boundaries
    // and confirms reversals from observed price extremes.
    //
    // Lower boundary -> Potential LONG setup.
    // Upper boundary -> Potential SHORT setup.
    //
    // Boundary proximity is calculated as a percentage of the
    // established range width.
    //
    // Reversal confirmation is calculated as a percentage
    // of the observed extreme price.
    //
    // Confirmations must occur inside the validated range.
    //
    // Trade execution is handled separately.
    fn monitor_mean_reversion_boundaries(
        &mut self,
        symbol: &str,
        price: f64,
        current_ts: i64,
    ) {
        // ====================================================
        // VALIDATE PRICE
        // ====================================================

        if !price.is_finite() || price <= 0.0 {
            return;
        }

        // ====================================================
        // SKIP SYMBOLS WITH ACTIVE TRADES
        // ====================================================

        // Includes Opening, Open, and Closing trades.
        let symbol_has_active_trade = {
            let view = self.rx_view.borrow();

            view.trades.has_active_trade_for_strategy_symbol(
                self.name(),
                symbol,
            )
        };

        if symbol_has_active_trade {
            // Remove any pending or previously confirmed setup.
            self.mean_reversion_boundary_monitors.remove(symbol);
            return;
        }

        // ====================================================
        // GET ENABLED CONFIGURATION
        // ====================================================

        let Some(config) = self.mean_reversion_configs
            .iter()
            .find(|config| config.enabled)
        else {
            self.mean_reversion_boundary_monitors.remove(symbol);
            return;
        };

        if !config.confirmation_pct.is_finite()
            || config.confirmation_pct <= 0.0
            || !config.boundary_proximity_pct.is_finite()
            || !(0.0..=50.0).contains(&config.boundary_proximity_pct)
            || !config.boundary_break_buffer_pct.is_finite()
            || config.boundary_break_buffer_pct < 0.0
        {
            self.mean_reversion_boundary_monitors.remove(symbol);
            return;
        }

        let confirmation_pct = config.confirmation_pct;
        let enable_confirmations = config.enable_confirmations;
        let boundary_proximity_pct = config.boundary_proximity_pct;
        let breakout_buffer_pct = config.boundary_break_buffer_pct;

        // ====================================================
        // GET PRESERVED RANGE
        // ====================================================

        let Some(range) = self.mean_reversion_ranges.get(symbol)
        else {
            self.mean_reversion_boundary_monitors.remove(symbol);
            return;
        };

        let lower_boundary = range.lower_boundary;
        let upper_boundary = range.upper_boundary;

        // ====================================================
        // CALCULATE BOUNDARY PROXIMITY
        // ====================================================

        let range_width = upper_boundary - lower_boundary;

        if !range_width.is_finite() || range_width <= 0.0 {
            self.mean_reversion_boundary_monitors.remove(symbol);
            return;
        }

        let proximity =
            range_width * (boundary_proximity_pct / 100.0);

        let lower_zone = lower_boundary + proximity;
        let upper_zone = upper_boundary - proximity;

        // ====================================================
        // CALCULATE BREAKOUT LIMITS
        // ====================================================

        let breakout_buffer = breakout_buffer_pct / 100.0;

        let upper_breakout_price =
            upper_boundary * (1.0 + breakout_buffer);

        let lower_breakout_price =
            lower_boundary * (1.0 - breakout_buffer);

        // ====================================================
        // CANCEL SETUP ON EXCESSIVE BREAKOUT
        // ====================================================

        // An intrabar breakout cancels the pending setup.
        // The validated range itself remains active until
        // candle-level invalidation confirms the breakout.

        if price > upper_breakout_price
            || price < lower_breakout_price
        {
            if self.mean_reversion_boundary_monitors
                .remove(symbol)
                .is_some()
                && self.debug_mode
            {
                print_log(
                    self.debug_mode,
                    true,
                    format_args!(
                        "[RESEARCH V4 BACKTEST SETUP CANCELLED][{}] \
                         REASON=TICK_BREAKOUT | PRICE=${:.4} | \
                         LOW=${:.4} | HIGH=${:.4}",
                        symbol,
                        price,
                        lower_boundary,
                        upper_boundary,
                    ),
                );
            }

            return;
        }

        // ====================================================
        // DETERMINE CURRENT BOUNDARY
        // ====================================================

        let current_boundary =
            if price < lower_boundary || price > upper_boundary {
                None
            } else if price <= lower_zone {
                Some(MeanReversionBoundary::Lower)
            } else if price >= upper_zone {
                Some(MeanReversionBoundary::Upper)
            } else {
                None
            };

        // ====================================================
        // UPDATE EXISTING SETUP
        // ====================================================

        if let Some(monitor) =
            self.mean_reversion_boundary_monitors.get_mut(symbol)
        {
            // Confirmed setups are evaluated for execution
            // on their confirmation tick.
            //
            // If execution is unavailable, the setup remains
            // consumed and cannot trigger a delayed entry.
            if monitor.confirmed {
                return;
            }

            // ====================================================
            // CANCEL IF OPPOSITE BOUNDARY IS REACHED
            // ====================================================

            if let Some(boundary) = current_boundary {
                if boundary != monitor.boundary {
                    if self.debug_mode {
                        print_log(
                            self.debug_mode,
                            true,
                            format_args!(
                                "[RESEARCH V4 BACKTEST SETUP CANCELLED][{}] \
                                 REASON=OPPOSITE_BOUNDARY | \
                                 PRICE=${:.4}",
                                symbol,
                                price,
                            ),
                        );
                    }

                    self.mean_reversion_boundary_monitors.remove(symbol);
                    return;
                }
            }

            // ====================================================
            // TRACK PRICE EXTREME
            // ====================================================

            match monitor.boundary {
                MeanReversionBoundary::Lower => {
                    monitor.extreme_price =
                        monitor.extreme_price.min(price);
                }

                MeanReversionBoundary::Upper => {
                    monitor.extreme_price =
                        monitor.extreme_price.max(price);
                }
            }

            // ====================================================
            // CHECK REVERSAL CONFIRMATION
            // ====================================================

            let reversal_pct = match monitor.boundary {
                MeanReversionBoundary::Lower => {
                    ((price - monitor.extreme_price)
                        / monitor.extreme_price) * 100.0
                }

                MeanReversionBoundary::Upper => {
                    ((monitor.extreme_price - price)
                        / monitor.extreme_price) * 100.0
                }
            };

            // ====================================================
            // REQUIRE CONFIRMATION INSIDE RANGE
            // ====================================================

            let inside_range =
                price >= lower_boundary
                    && price <= upper_boundary;

            if inside_range
                && (!enable_confirmations
                    || reversal_pct >= confirmation_pct)
            {
                monitor.confirmed = true;

                // ====================================================
                // CONFIRMATION DEBUG
                // ====================================================

                if self.debug_mode {
                    print_log(
                        self.debug_mode,
                        true,
                        format_args!(
                            "[RESEARCH V4 BACKTEST REVERSAL CONFIRMED][{}] \
                             SIDE={} | PRICE=${:.4} | \
                             EXTREME=${:.4} | REVERSAL={:.3}% | \
                             REQUIRED={:.3}%",
                            symbol,
                            match monitor.boundary {
                                MeanReversionBoundary::Lower => "LONG",
                                MeanReversionBoundary::Upper => "SHORT",
                            },
                            price,
                            monitor.extreme_price,
                            reversal_pct,
                            confirmation_pct,
                        ),
                    );
                }
            }

            return;
        }

        // ====================================================
        // DETECT NEW BOUNDARY APPROACH
        // ====================================================

        let Some(boundary) = current_boundary else {
            return;
        };

        self.mean_reversion_boundary_monitors.insert(
            symbol.to_string(),
            MeanReversionBoundaryMonitor {
                boundary,
                detected_ts_ms: current_ts,
                detected_price: price,
                extreme_price: price,
                confirmed: false,
                trade_submitted: false,
            },
        );

        // ====================================================
        // BOUNDARY APPROACH DEBUG
        // ====================================================

        if self.debug_mode {
            print_log(
                self.debug_mode,
                true,
                format_args!(
                    "[RESEARCH V4 BACKTEST BOUNDARY APPROACH][{}] \
                     SIDE={:?} | PRICE=${:.4} | \
                     LOW=${:.4} | HIGH=${:.4} | \
                     PROXIMITY={:.1}%",
                    symbol,
                    boundary,
                    price,
                    lower_boundary,
                    upper_boundary,
                    boundary_proximity_pct,
                ),
            );
        }
    }
}




impl ResearchV4BacktestStrategy {

    // ============================================================
    // EXECUTE MEAN REVERSION ENTRY
    // ============================================================

    /// Executes a confirmed mean-reversion entry using the validated range.
    ///
    /// Entry behavior:
    /// - Consumes confirmed signals immediately, even when trading is blocked,
    ///   preventing stale signals from executing later.
    /// - Determines LONG or SHORT direction from the confirmed range boundary.
    /// - Calculates take profit at a configurable fraction of the validated range.
    /// - Places stop loss at a configurable percentage beyond the entry-side boundary.
    /// - Converts absolute TP/SL price levels into percentages for engine risk management.
    /// - Rejects entries with invalid prices, configurations, or TP/SL geometry.
    /// - Prevents duplicate trades and enforces the strategy's maximum active-trade limit.
    /// - Includes Opening, Open, and Closing trades in active-trade checks.
    /// - Submits entries through the canonical EngineEvent::OpenTrade lifecycle.
    /// - Records trade cooldowns only after successful event submission.
    /// - Logs entry prices, range boundaries, TP/SL levels, and dynamic risk/reward.
    ///
    /// Risk management:
    /// - TP/SL distances are determined by range geometry rather than a fixed
    ///   risk/reward ratio.
    /// - The engine evaluates TP/SL percentages against the actual average fill price.
    /// - Execution slippage may shift absolute exit levels from their intended prices.
    /// - Existing emergency stop-loss and maximum trade-duration protections remain active.
    /// - All exit execution is managed by the engine, not directly by this strategy.

    fn execute_mean_reversion_entry(
        &mut self,
        symbol: &str,
        price: f64,
        current_ts: i64,
        can_trade: bool,
    ) {
        // ====================================================
        // GET CONFIRMED SETUP
        // ====================================================

        let Some(monitor) =
            self.mean_reversion_boundary_monitors.get_mut(symbol)
        else {
            return;
        };

        if !monitor.confirmed || monitor.trade_submitted {
            return;
        }

        // ====================================================
        // CONSUME CONFIRMED SETUP
        // ====================================================

        // Consume immediately, regardless of whether trading
        // is currently allowed.
        //
        // Prevents confirmed signals from executing later
        // when position capacity becomes available.

        monitor.trade_submitted = true;

        let boundary = monitor.boundary;

        // ====================================================
        // VALIDATE PRICE AND TRADING ELIGIBILITY
        // ====================================================

        if !can_trade
            || !price.is_finite()
            || price <= 0.0
            || self.black_swan_protection.is_halted()
            || !self.trade_cooldown.can_trade(symbol, current_ts)
        {
            return;
        }

        // ====================================================
        // GET ENABLED CONFIGURATION
        // ====================================================

        let Some(config) = self.mean_reversion_configs
            .iter()
            .find(|config| config.enabled)
        else {
            return;
        };

        let profit_fraction =
            config.range_profit_target_fraction;

        let stop_buffer_pct =
            config.stop_loss_boundary_buffer_pct;

        if !profit_fraction.is_finite()
            || profit_fraction <= 0.0
            || profit_fraction > 1.0
            || !stop_buffer_pct.is_finite()
            || stop_buffer_pct < 0.0
        {
            return;
        }

        // ====================================================
        // DETERMINE TRADE DIRECTION
        // ====================================================

        let direction = match boundary {
            MeanReversionBoundary::Lower => Direction::Long,
            MeanReversionBoundary::Upper => Direction::Short,
        };

        // ====================================================
        // GET VALIDATED RANGE
        // ====================================================

        let Some(range) =
            self.mean_reversion_ranges.get(symbol)
        else {
            return;
        };

        let lower_boundary =
            range.lower_boundary;

        let upper_boundary =
            range.upper_boundary;

        let range_width =
            upper_boundary - lower_boundary;

        if !lower_boundary.is_finite()
            || !upper_boundary.is_finite()
            || lower_boundary <= 0.0
            || upper_boundary <= lower_boundary
            || !range_width.is_finite()
        {
            return;
        }

        // ====================================================
        // CALCULATE RANGE-BASED TAKE PROFIT / STOP LOSS
        // ====================================================

        // Take profit is positioned at a configured fraction
        // of the validated range.
        //
        // LONG:
        //     TP = lower + (range_width * fraction)
        //
        // SHORT:
        //     TP = upper - (range_width * fraction)
        //
        // Stop loss is positioned outside the entry-side
        // boundary using the configured percentage buffer.
        //
        // LONG:
        //     SL = lower * (1 - buffer / 100)
        //
        // SHORT:
        //     SL = upper * (1 + buffer / 100)

        let (take_profit_price, stop_loss_price) =
            match direction {

                Direction::Long => {
                    (
                        lower_boundary
                            + range_width * profit_fraction,

                        lower_boundary
                            * (1.0 - stop_buffer_pct / 100.0),
                    )
                }

                Direction::Short => {
                    (
                        upper_boundary
                            - range_width * profit_fraction,

                        upper_boundary
                            * (1.0 + stop_buffer_pct / 100.0),
                    )
                }
            };

        // ====================================================
        // VALIDATE EXIT PRICES
        // ====================================================

        if !take_profit_price.is_finite()
            || !stop_loss_price.is_finite()
            || take_profit_price <= 0.0
            || stop_loss_price <= 0.0
        {
            return;
        }

        // Ensure TP is profitable and SL is adverse
        // relative to the current signal price.

        let valid_exit_geometry =
            match direction {
                Direction::Long => {
                    stop_loss_price < price
                        && price < take_profit_price
                }

                Direction::Short => {
                    take_profit_price < price
                        && price < stop_loss_price
                }
            };

        if !valid_exit_geometry {
            return;
        }

        // ====================================================
        // CONVERT EXIT PRICES TO ENGINE PERCENTAGES
        // ====================================================

        // Engine risk management evaluates TP/SL using
        // the actual average entry fill price.
        //
        // These percentages are calculated from the
        // signal price and may therefore produce slightly
        // different absolute exit prices if execution slips.

        let take_profit_percent =
            match direction {
                Direction::Long => {
                    ((take_profit_price - price) / price)
                        * 100.0
                }

                Direction::Short => {
                    ((price - take_profit_price) / price)
                        * 100.0
                }
            };

        let stop_loss_percent =
            match direction {
                Direction::Long => {
                    ((price - stop_loss_price) / price)
                        * 100.0
                }

                Direction::Short => {
                    ((stop_loss_price - price) / price)
                        * 100.0
                }
            };

        if !take_profit_percent.is_finite()
            || !stop_loss_percent.is_finite()
            || take_profit_percent <= 0.0
            || stop_loss_percent <= 0.0
        {
            return;
        }

        // ====================================================
        // CHECK EXISTING TRADES
        // ====================================================

        let (symbol_has_active_trade, active_trade_count) = {
            let view = self.rx_view.borrow();

            (
                view.trades.has_active_trade_for_strategy_symbol(
                    self.name(),
                    symbol,
                ),
                view.trades.count_active_for_strategy(
                    self.name(),
                ),
            )
        };

        // Opening, Open, and Closing trades are included.
        //
        // The confirmed signal was already consumed,
        // preventing delayed execution if capacity
        // becomes available later.

        if symbol_has_active_trade
            || active_trade_count >= self.max_trades_allowed
        {
            return;
        }

        // ====================================================
        // CONSTRUCT TRADE INTENT
        // ====================================================

        let intent = TradeIntent {
            strategy_id: self.name().to_string(),
            symbol: symbol.to_string(),
            direction,
            signal_px: price,

            target_notional: self.trade_amount_usd,
            margin_usd: self.trade_amount_usd,

            order_type: OrderType::Market,
            time_in_force: TimeInForce::Day,
            limit_px: None,
            stop_px: None,
            post_only: false,

            take_profit_percent: Some(take_profit_percent),
            stop_loss_percent: Some(stop_loss_percent),

            emergency_stop_percent: self.emergency_exit_percent,
            stop_loss_delay_secs: self.min_sl_delay,
            trade_duration_secs: Some(86_400),

            trailing_stop: None,
            ts_ms: current_ts,
        };

        // ====================================================
        // SUBMIT TRADE INTENT
        // ====================================================

        if self.tx_engine.send(
            EngineEvent::OpenTrade(intent)
        ).is_ok() {

            // Record submission cooldown.

            self.trade_cooldown.record_trade(
                symbol,
                current_ts,
            );

            // ====================================================
            // ENTRY DEBUG
            // ====================================================

            if self.debug_mode {
                print_log(
                    self.debug_mode,
                    true,
                    format_args!(
                        "[RESEARCH V4 BACKTEST {:?} ENTRY][{}] \
                         PRICE=${:.6} | \
                         RANGE_LOW=${:.6} | RANGE_HIGH=${:.6} | \
                         TP_PRICE=${:.6} | SL_PRICE=${:.6} | \
                         TP={:.3}% | SL={:.3}% | \
                         TARGET_FRACTION={:.2} | \
                         SL_BUFFER={:.3}% | \
                         RR=1:{:.3} | \
                         AMOUNT=${:.2}",
                        direction,
                        symbol,
                        price,
                        lower_boundary,
                        upper_boundary,
                        take_profit_price,
                        stop_loss_price,
                        take_profit_percent,
                        stop_loss_percent,
                        profit_fraction,
                        stop_buffer_pct,
                        take_profit_percent / stop_loss_percent,
                        self.trade_amount_usd,
                    ),
                );
            }
        }
    }
}