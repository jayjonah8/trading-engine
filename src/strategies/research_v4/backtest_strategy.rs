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

use super::backtest_state::{
    ResearchV4BacktestStrategy,
    TrendingConfig,
    MeanReversionConfig,
    MeanReversionRange,
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
                    min_range_width_pct: 2.0,
                    max_range_width_pct: 8.0,
                    max_range_boundary_diff_pct: 0.5,
                    min_confirmed_reversals: 2,
                    min_reversal_fraction: 0.25,
                    boundary_proximity_pct: 0.8,
                    confirmation_pct: 0.3,
                    enable_confirmations: true,
                    range_profit_target_fraction: 0.75,
                    boundary_break_buffer_pct: 0.5,
                    stop_loss_pct: Some(2.0),
                    trade_duration_secs: Some(4800),
                },
            ],

            mean_reversion_ranges: HashMap::new(),

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

                // TODO:
                // 1. Check cached market qualification.
                // 2. Update active trending setups.
                // 3. Update active mean-reversion setups.
                // 4. Evaluate entry confirmations.
                // 5. Validate risk/reward.
                // 6. Submit trade intent.

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

                // TODO:
                // Use can_trade when evaluating trending and
                // mean-reversion entry confirmations.
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

    // Detects established trading ranges using two rolling windows.
    //
    // Validation:
    // - Requires sufficient completed one-minute candles.
    // - Calculates highs and lows for both observation windows.
    // - Validates the longer window's range width.
    // - Compares short and long window boundaries.
    // - Rejects ranges with inconsistent boundaries.
    // - Caches valid ranges for future entry confirmation.
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
            return;
        };

        if bars.len() < long_period {
            self.mean_reversion_ranges.remove(symbol);
            return;
        }

        // Collect only the latest long-window candles.
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
            self.mean_reversion_ranges.remove(symbol);
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
            self.mean_reversion_ranges.remove(symbol);
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
            self.mean_reversion_ranges.remove(symbol);
            return;
        }

        // ====================================================
        // VALIDATE RANGE OSCILLATIONS
        // ====================================================

        // Require genuine price reversals from both established
        // boundaries before accepting the range as mean-reverting.
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
            self.mean_reversion_ranges.remove(symbol);
            return;
        }

        // ====================================================
        // CACHE VALID RANGE
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

        // ====================================================
        // RANGE DETECTION DEBUG
        // ====================================================

        if self.debug_mode {
            print_log(
                self.debug_mode,
                true,
                format_args!(
                    "[RESEARCH V4 BACKTEST RANGE DETECTED][{}] \
                     LOW=${:.4} | HIGH=${:.4} | \
                     WIDTH={:.3}% | \
                     SHORT_LOW=${:.4} | SHORT_HIGH=${:.4} | \
                     LONG_LOW=${:.4} | LONG_HIGH=${:.4}",
                    symbol,
                    long_range_low,
                    long_range_high,
                    range_width_pct,
                    short_range_low,
                    short_range_high,
                    long_range_low,
                    long_range_high,
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
            || boundary_proximity_pct < 0.0
            || !min_reversal_fraction.is_finite()
            || min_reversal_fraction <= 0.0
            || min_reversal_fraction > 1.0
        {
            return false;
        }

        // ====================================================
        // CALCULATE REVERSAL REQUIREMENTS
        // ====================================================

        let range_width = upper_boundary - lower_boundary;

        let required_reversal =
            range_width * min_reversal_fraction;

        let lower_proximity =
            lower_boundary * (boundary_proximity_pct / 100.0);

        let upper_proximity =
            upper_boundary * (boundary_proximity_pct / 100.0);

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
                || bar.low <= 0.0
                || bar.high < bar.low
            {
                return false;
            }

            let near_lower =
                bar.low <= lower_boundary + lower_proximity;

            let near_upper =
                bar.high >= upper_boundary - upper_proximity;

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