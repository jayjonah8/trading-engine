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
        weighted_trade_flip::{
            WeightedTradeFlip,
            WeightedTradeFlipConfig,
        },
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
    StrategyConfigKey,
    BreakoutConfigKey,
    MeanReversionConfig,
    MeanReversionRange,
    MeanReversionBoundary,
    MeanReversionBoundaryMonitor,
    MeanReversionRiskConfig,
    BreakoutConfig,
    BreakoutRiskConfig,
    BreakoutDirection,
    BreakoutSetup,
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

            enable_trading_eligibility_check: true,
            min_volume_usd: Some(350_000.0),
            min_24h_move_pct: None,
            max_24h_move_pct: Some(15.0),

            // Disabled initially for baseline testing.
            min_relative_volume: None,
            relative_volume_period: 20,

            can_trade: HashMap::new(),

            // ====================================================
            // MEAN REVERSION CONFIGURATIONS
            // ====================================================

            mean_reversion_configs: vec![
                MeanReversionConfig {
                    id: "mr_4h".to_string(),

                    enabled: true,

                    // ====================================================
                    // RANGE DETECTION
                    // ====================================================

                    short_window_secs: 2 * 3600,
                    long_window_secs: 4 * 3600,

                    min_range_width_pct: 4.0,
                    max_range_width_pct: 8.0,

                    max_range_boundary_diff_pct: 1.0,

                    min_confirmed_reversals: 3,
                    min_reversal_fraction: 0.60,

                    // ====================================================
                    // ENTRY CONFIRMATION
                    // ====================================================

                    boundary_proximity_pct: 15.0,
                    confirmation_pct: 0.5,
                    enable_confirmations: true,

                    // true = candle-close confirmation
                    // false = tick-based confirmation
                    require_candle_confirmation: true,

                    boundary_break_buffer_pct: 0.5,

                    // ====================================================
                    // RISK MANAGEMENT
                    // ====================================================

                    risk: MeanReversionRiskConfig {
                        range_profit_target_fraction: 0.70,
                        stop_loss_boundary_buffer_pct: 0.5,
                        trade_duration_secs: Some(4800),
                    },
                },
            ],

            // ====================================================
            // BREAKOUT CONFIGURATIONS
            // ====================================================

            breakout_configs: vec![
                BreakoutConfig {
                    id: "breakout_4h".to_string(),

                    enabled: true,

                    confirmation_pct: 0.5,

                    // true = candle-close confirmation
                    // false = tick-based confirmation
                    require_candle_confirmation: true,

                    // Pending breakout expires after 30 minutes.
                    setup_expiration_secs: 1800,

                    risk: BreakoutRiskConfig {
                        take_profit_pct: 3.0,
                        stop_loss_pct: 2.0,
                        trade_duration_secs: Some(4800),
                    },
                },
            ],

            // ====================================================
            // BREAKOUT STATE
            // ====================================================

            breakout_setups: HashMap::new(),

            // ====================================================
            // MEAN REVERSION STATE
            // ====================================================

            // Validated ranges indexed by symbol + config ID.
            mean_reversion_ranges: HashMap::new(),

            // Independent boundary monitors per configuration.
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
                true,
                false,
                15.0,
                8.0,
                15,
            ),

            trade_cooldown: TradeCooldown::new(200),

            trade_duration_watchdog: TradeDurationWatchdog::new(
                true,
                10.0,
                300_000,
            ),

            // ====================================================
            // WEIGHTED TRADE FLIP
            // ====================================================

            weighted_trade_flip: WeightedTradeFlip::new(
                WeightedTradeFlipConfig {
                    enabled: false,

                    min_profit_percent: 0.0,

                    flip_probability_after_2_wins: 0.50,
                    flip_probability_after_3_wins: 0.75,
                    flip_probability_after_4_wins: 0.90,

                    seed: 42,
                },
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

                let can_trade =
                    if self.enable_trading_eligibility_check {
                        self.can_trade
                            .get(&ev.symbol)
                            .copied()
                            .unwrap_or(false)
                    } else {
                        true
                    };

                // ====================================================
                // MONITOR TICK-BASED MEAN REVERSION SETUPS
                // ====================================================

                // Track boundary approaches and intrabar extremes.
                //
                // Configurations with tick confirmation enabled
                // can confirm reversals on this event.
                //
                // Candle-based configurations continue tracking
                // extremes but wait for a completed candle.

                self.monitor_mean_reversion_boundaries(
                    &ev.symbol,
                    trade.price,
                    trade.timestamp_ms,
                    false,
                );

                // ====================================================
                // EXECUTE CONFIRMED MEAN REVERSION ENTRIES
                // ====================================================

                // Execute newly confirmed tick-based reversals.
                //
                // Confirmed signals are consumed immediately,
                // even when trading is blocked.

                self.execute_mean_reversion_entry(
                    &ev.symbol,
                    trade.price,
                    trade.timestamp_ms,
                    can_trade,
                );

                // ====================================================
                // MONITOR TICK-BASED BREAKOUT SETUPS
                // ====================================================

                // Configurations that do not require candle
                // confirmation can confirm on trade ticks.
                //
                // Each breakout configuration independently
                // evaluates its own confirmation threshold.

                self.monitor_breakout_setups(
                    &ev.symbol,
                    trade.price,
                    trade.timestamp_ms,
                    false,
                );

                // ====================================================
                // EXECUTE CONFIRMED BREAKOUT ENTRIES
                // ====================================================

                // Execute newly confirmed tick-based breakouts
                // using the current trade price.

                self.execute_breakout_entry(
                    &ev.symbol,
                    trade.price,
                    trade.timestamp_ms,
                    can_trade,
                );

                // ====================================================
                // CLEANUP CONSUMED BREAKOUT SETUPS
                // ====================================================

                // Remove setups already evaluated for entry.
                // Preserve pending setups awaiting confirmation.

                self.cleanup_breakout_setups(
                    &ev.symbol,
                );

                // ====================================================
                // FUTURE STRATEGY DEVELOPMENT
                // ====================================================

                // TODO:
                // 1. Integrate weighted trade flipping.
                // 2. Expand strategy-specific risk management.
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

                // ====================================================
                // CAPTURE CANDLE INFORMATION
                // ====================================================

                // Capture the close price before moving the candle
                // into CandleHistory.

                let timeframe = bar.timeframe;

                let current_ts =
                    bar.end.timestamp_millis();

                let close_price = bar.close;

                // ====================================================
                // STORE COMPLETED CANDLE
                // ====================================================

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

                // Detect new ranges and invalidate expired
                // or broken ranges.
                //
                // Confirmed range failures generate independent
                // pending breakout setups.

                self.update_mean_reversion_ranges(
                    &ev.symbol,
                    timeframe,
                    current_ts,
                );

                // ====================================================
                // PROCESS ONE-MINUTE CANDLE CONFIRMATIONS
                // ====================================================

                if timeframe == Timeframe::ONE_MINUTE {

                    // ====================================================
                    // TRADING ELIGIBILITY
                    // ====================================================

                    // Read updated eligibility after processing
                    // the completed candle.

                    let can_trade =
                        if self.enable_trading_eligibility_check {
                            self.can_trade
                                .get(&ev.symbol)
                                .copied()
                                .unwrap_or(false)
                        } else {
                            true
                        };

                    // ====================================================
                    // MONITOR CANDLE-BASED MEAN REVERSION SETUPS
                    // ====================================================

                    // Evaluate existing boundary monitors using
                    // the completed candle's closing price.
                    //
                    // Configurations requiring candle confirmation
                    // can confirm reversals here.
                    //
                    // Intrabar extremes remain tick-based.
                    // Candle closes do not create new monitors
                    // or modify their tracked extremes.

                    self.monitor_mean_reversion_boundaries(
                        &ev.symbol,
                        close_price,
                        current_ts,
                        true,
                    );

                    // ====================================================
                    // EXECUTE CANDLE-CONFIRMED MEAN REVERSION ENTRIES
                    // ====================================================

                    // Submit newly confirmed candle-based
                    // mean-reversion signals.
                    //
                    // The completed candle close is used as
                    // the signal price.
                    //
                    // Actual fill prices are determined
                    // by the engine's execution adapter.

                    self.execute_mean_reversion_entry(
                        &ev.symbol,
                        close_price,
                        current_ts,
                        can_trade,
                    );

                    // ====================================================
                    // MONITOR CANDLE-BASED BREAKOUT SETUPS
                    // ====================================================

                    // Evaluate breakout confirmations using
                    // the completed candle's closing price.
                    //
                    // Configurations requiring candle confirmation
                    // can confirm their breakouts here.

                    self.monitor_breakout_setups(
                        &ev.symbol,
                        close_price,
                        current_ts,
                        true,
                    );

                    // ====================================================
                    // EXECUTE CANDLE-CONFIRMED BREAKOUT ENTRIES
                    // ====================================================

                    // Submit newly confirmed candle-based
                    // breakout signals.
                    //
                    // The completed candle close is used
                    // as the signal price.
                    //
                    // The engine handles actual order execution.

                    self.execute_breakout_entry(
                        &ev.symbol,
                        close_price,
                        current_ts,
                        can_trade,
                    );

                    // ====================================================
                    // CLEANUP CONSUMED BREAKOUT SETUPS
                    // ====================================================

                    // Remove setups already evaluated for entry.
                    //
                    // Preserve pending setups awaiting confirmation.

                    self.cleanup_breakout_setups(
                        &ev.symbol,
                    );
                }

                // ====================================================
                // FUTURE STRATEGY DEVELOPMENT
                // ====================================================

                // TODO:
                // 1. Integrate weighted trade flipping.
                // 2. Update additional market protection logic.

                // ====================================================
                // CANDLE DEBUG
                // ====================================================

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
                                .keys()
                                .any(|key| key.symbol == ev.symbol),
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

    // Detects and preserves established mean-reversion ranges
    // independently for every enabled configuration.
    //
    // Range lifecycle:
    // 1. Discover a valid range using rolling candle history.
    // 2. Validate range width and boundary consistency.
    // 3. Validate confirmed oscillations and reversals.
    // 4. Preserve the validated range boundaries.
    // 5. Detect confirmed range breakouts.
    // 6. Generate independent pending breakout setups.
    // 7. Invalidate or expire the original range.
    //
    // Each configuration maintains its own validated range
    // and boundary monitoring state for each asset.
    //
    // Breakout setups are preserved independently using:
    // (symbol, range_config_id, breakout_config_id).
    //
    // Individual breakout confirmation is handled separately
    // by monitor_breakout_setups().
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

        // ====================================================
        // GET CANDLE HISTORY
        // ====================================================

        let Some(bars) = self.candle_history.get(
            symbol,
            Timeframe::ONE_MINUTE,
        ) else {
            return;
        };

        // ====================================================
        // EVALUATE ALL MEAN REVERSION CONFIGURATIONS
        // ====================================================

        for config in self.mean_reversion_configs.iter() {

            let key = StrategyConfigKey::new(
                symbol,
                &config.id,
            );

            // ====================================================
            // CHECK CONFIGURATION STATUS
            // ====================================================

            if !config.enabled {
                self.mean_reversion_ranges.remove(&key);
                self.mean_reversion_boundary_monitors.remove(&key);
                continue;
            }

            let short_period =
                (config.short_window_secs / 60) as usize;

            let long_period =
                (config.long_window_secs / 60) as usize;

            if short_period == 0
                || long_period == 0
                || short_period > long_period
            {
                self.mean_reversion_ranges.remove(&key);
                self.mean_reversion_boundary_monitors.remove(&key);
                continue;
            }

            if bars.len() < long_period {
                self.mean_reversion_ranges.remove(&key);
                self.mean_reversion_boundary_monitors.remove(&key);
                continue;
            }

            // ====================================================
            // CHECK EXISTING VALIDATED RANGE
            // ====================================================

            if let Some(existing_range) =
                self.mean_reversion_ranges.get(&key)
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

                // Use completed candle close to confirm breakout.
                // Temporary intrabar wicks do not invalidate ranges.

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
                    continue;
                }

                // ====================================================
                // CREATE PENDING BREAKOUT SETUPS
                // ====================================================

                // Only confirmed range failures generate
                // breakout setups.
                //
                // Expiration alone does not create a breakout.
                //
                // Every enabled breakout configuration receives
                // an independent pending setup.
                //
                // Each configuration will later confirm using
                // its own confirmation threshold.

                if breakout && !expired {

                    // ====================================================
                    // DETERMINE BREAKOUT DIRECTION
                    // ====================================================

                    let direction =
                        if breakout_above {
                            BreakoutDirection::Bullish
                        } else {
                            BreakoutDirection::Bearish
                        };

                    // ====================================================
                    // EVALUATE BREAKOUT CONFIGURATIONS
                    // ====================================================

                    for breakout_config in self.breakout_configs.iter() {

                        if !breakout_config.enabled {
                            continue;
                        }

                        // ====================================================
                        // VALIDATE BREAKOUT CONFIGURATION
                        // ====================================================

                        if !breakout_config.confirmation_pct.is_finite()
                            || breakout_config.confirmation_pct < 0.0
                        {
                            continue;
                        }

                        // ====================================================
                        // BUILD BREAKOUT CONFIGURATION KEY
                        // ====================================================

                        let breakout_key = BreakoutConfigKey::new(
                            symbol,
                            &config.id,
                            &breakout_config.id,
                        );

                        // ====================================================
                        // PRESERVE PENDING BREAKOUT SETUP
                        // ====================================================

                        self.breakout_setups.insert(
                            breakout_key,
                            BreakoutSetup {
                                direction,

                                upper_boundary:
                                    existing_range.upper_boundary,

                                lower_boundary:
                                    existing_range.lower_boundary,

                                breakout_price:
                                    latest_bar.close,

                                breakout_ts_ms:
                                    current_ts,

                                confirmation_price: None,
                                confirmed_ts_ms: None,

                                confirmed: false,
                                trade_submitted: false,
                            },
                        );

                        // ====================================================
                        // BREAKOUT DETECTION DEBUG
                        // ====================================================

                        if self.debug_mode {
                            print_log(
                                self.debug_mode,
                                true,
                                format_args!(
                                    "[RESEARCH V4 BACKTEST BREAKOUT DETECTED]\
                                     [{}][{}][{}] \
                                     DIRECTION={:?} | \
                                     BREAKOUT_PRICE=${:.6} | \
                                     RANGE_LOW=${:.6} | \
                                     RANGE_HIGH=${:.6} | \
                                     CONFIRMATION={:.3}% | \
                                     STATUS=PENDING",
                                    symbol,
                                    config.id,
                                    breakout_config.id,
                                    direction,
                                    latest_bar.close,
                                    existing_range.lower_boundary,
                                    existing_range.upper_boundary,
                                    breakout_config.confirmation_pct,
                                ),
                            );
                        }
                    }
                }

                // ====================================================
                // INVALIDATE EXISTING RANGE
                // ====================================================

                if self.debug_mode {
                    print_log(
                        self.debug_mode,
                        true,
                        format_args!(
                            "[RESEARCH V4 BACKTEST RANGE INVALIDATED][{}][{}] \
                             REASON={} | \
                             LOW=${:.4} | HIGH=${:.4} | \
                             AGE={}s",
                            symbol,
                            config.id,
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

                // ====================================================
                // REMOVE INVALIDATED RANGE
                // ====================================================

                // Remove only this configuration's range
                // and its associated mean-reversion monitor.
                //
                // Pending breakout setups remain preserved
                // independently of the original range.

                self.mean_reversion_ranges.remove(&key);

                self.mean_reversion_boundary_monitors.remove(&key);

                // Do not immediately establish a replacement range
                // using the candle that invalidated this range.
                //
                // Other configurations continue independently.

                continue;
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
                continue;
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
                continue;
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
                continue;
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
                continue;
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
                key.clone(),
                detected_range,
            );

            // Clear stale boundary monitoring state
            // belonging only to this configuration.

            self.mean_reversion_boundary_monitors.remove(&key);

            // ====================================================
            // RANGE DETECTION DEBUG
            // ====================================================

            if self.debug_mode {
                print_log(
                    self.debug_mode,
                    true,
                    format_args!(
                        "[RESEARCH V4 BACKTEST RANGE VALIDATED][{}][{}] \
                         LOW=${:.4} | HIGH=${:.4} | \
                         WIDTH={:.3}% | \
                         SHORT_LOW=${:.4} | SHORT_HIGH=${:.4} | \
                         LONG_LOW=${:.4} | LONG_HIGH=${:.4} | \
                         LIFETIME={}s",
                        symbol,
                        config.id,
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

    // Monitors preserved range boundaries independently for
    // every enabled mean-reversion configuration.
    //
    // Lower boundary -> Potential LONG setup.
    // Upper boundary -> Potential SHORT setup.
    //
    // Boundary detection:
    // - Uses trade ticks to detect boundary approaches.
    // - Tracks intrabar price extremes using trade ticks.
    // - Cancels pending setups on excessive breakouts.
    // - Cancels setups when the opposite boundary is reached.
    //
    // Reversal confirmation:
    // - enable_confirmations controls whether a reversal is required.
    // - require_candle_confirmation selects candle or tick confirmation.
    // - Candle confirmation uses completed one-minute candle closes.
    // - Tick confirmation uses individual trade prices.
    // - Confirmation must occur inside the validated range.
    //
    // Trade execution is handled separately.

    fn monitor_mean_reversion_boundaries(
        &mut self,
        symbol: &str,
        price: f64,
        current_ts: i64,
        candle_closed: bool,
    ) {
        // ====================================================
        // VALIDATE PRICE
        // ====================================================

        if !price.is_finite() || price <= 0.0 {
            return;
        }

        // ====================================================
        // EVALUATE ALL MEAN REVERSION CONFIGURATIONS
        // ====================================================

        for config in self.mean_reversion_configs.iter() {

            let key = StrategyConfigKey::new(
                symbol,
                &config.id,
            );

            // ====================================================
            // CHECK CONFIGURATION STATUS
            // ====================================================

            if !config.enabled {
                self.mean_reversion_boundary_monitors.remove(&key);
                continue;
            }

            if !config.confirmation_pct.is_finite()
                || config.confirmation_pct <= 0.0
                || !config.boundary_proximity_pct.is_finite()
                || !(0.0..=50.0).contains(
                    &config.boundary_proximity_pct
                )
                || !config.boundary_break_buffer_pct.is_finite()
                || config.boundary_break_buffer_pct < 0.0
            {
                self.mean_reversion_boundary_monitors.remove(&key);
                continue;
            }

            let confirmation_pct = config.confirmation_pct;

            let enable_confirmations =
                config.enable_confirmations;

            let require_candle_confirmation =
                config.require_candle_confirmation;

            let boundary_proximity_pct =
                config.boundary_proximity_pct;

            let breakout_buffer_pct =
                config.boundary_break_buffer_pct;

            // ====================================================
            // GET PRESERVED RANGE
            // ====================================================

            let Some(range) =
                self.mean_reversion_ranges.get(&key)
            else {
                self.mean_reversion_boundary_monitors.remove(&key);
                continue;
            };

            let lower_boundary = range.lower_boundary;
            let upper_boundary = range.upper_boundary;

            // ====================================================
            // CALCULATE BOUNDARY PROXIMITY
            // ====================================================

            let range_width =
                upper_boundary - lower_boundary;

            if !range_width.is_finite()
                || range_width <= 0.0
            {
                self.mean_reversion_boundary_monitors.remove(&key);
                continue;
            }

            let proximity =
                range_width * (boundary_proximity_pct / 100.0);

            let lower_zone =
                lower_boundary + proximity;

            let upper_zone =
                upper_boundary - proximity;

            // ====================================================
            // CALCULATE BREAKOUT LIMITS
            // ====================================================

            let breakout_buffer =
                breakout_buffer_pct / 100.0;

            let upper_breakout_price =
                upper_boundary * (1.0 + breakout_buffer);

            let lower_breakout_price =
                lower_boundary * (1.0 - breakout_buffer);

            // ====================================================
            // CANCEL SETUP ON EXCESSIVE BREAKOUT
            // ====================================================

            // Intrabar breakout cancellation remains tick-based.
            //
            // The validated range itself remains active until
            // candle-level invalidation confirms the breakout.

            if !candle_closed
                && (price > upper_breakout_price
                    || price < lower_breakout_price)
            {
                if self.mean_reversion_boundary_monitors
                    .remove(&key)
                    .is_some()
                    && self.debug_mode
                {
                    print_log(
                        self.debug_mode,
                        true,
                        format_args!(
                            "[RESEARCH V4 BACKTEST SETUP CANCELLED][{}][{}] \
                             REASON=TICK_BREAKOUT | PRICE=${:.4} | \
                             LOW=${:.4} | HIGH=${:.4}",
                            symbol,
                            config.id,
                            price,
                            lower_boundary,
                            upper_boundary,
                        ),
                    );
                }

                continue;
            }

            // ====================================================
            // DETERMINE CURRENT BOUNDARY
            // ====================================================

            let current_boundary =
                if price < lower_boundary
                    || price > upper_boundary
                {
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
                self.mean_reversion_boundary_monitors.get_mut(&key)
            {
                // Confirmed setups are consumed by the
                // entry execution function.
                //
                // Never reconfirm an existing setup.

                if monitor.confirmed {
                    continue;
                }

                // ====================================================
                // CANCEL IF OPPOSITE BOUNDARY IS REACHED
                // ====================================================

                // Only ticks modify boundary monitoring state.

                if !candle_closed {

                    if let Some(boundary) = current_boundary {

                        if boundary != monitor.boundary {

                            if self.debug_mode {
                                print_log(
                                    self.debug_mode,
                                    true,
                                    format_args!(
                                        "[RESEARCH V4 BACKTEST SETUP CANCELLED][{}][{}] \
                                         REASON=OPPOSITE_BOUNDARY | \
                                         PRICE=${:.4}",
                                        symbol,
                                        config.id,
                                        price,
                                    ),
                                );
                            }

                            self.mean_reversion_boundary_monitors
                                .remove(&key);

                            continue;
                        }
                    }

                    // ====================================================
                    // TRACK PRICE EXTREME
                    // ====================================================

                    // LONG: Track the lowest observed price.
                    // SHORT: Track the highest observed price.
                    //
                    // Candle-close events must not modify extremes.

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
                }

                // ====================================================
                // CHECK CONFIRMATION MODE
                // ====================================================

                // When reversal confirmation is enabled:
                //
                // Candle mode -> Only evaluate candle closes.
                // Tick mode   -> Only evaluate trade ticks.
                //
                // When reversal confirmation is disabled,
                // preserve immediate tick-based confirmation.

                let confirmation_event_allowed =
                    if !enable_confirmations {
                        !candle_closed
                    } else if require_candle_confirmation {
                        candle_closed
                    } else {
                        !candle_closed
                    };

                if !confirmation_event_allowed {
                    continue;
                }

                // ====================================================
                // CHECK REVERSAL CONFIRMATION
                // ====================================================

                let reversal_pct =
                    match monitor.boundary {

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
                                "[RESEARCH V4 BACKTEST REVERSAL CONFIRMED][{}][{}] \
                                 SIDE={} | PRICE=${:.4} | \
                                 EXTREME=${:.4} | REVERSAL={:.3}% | \
                                 REQUIRED={:.3}% | MODE={}",
                                symbol,
                                config.id,
                                match monitor.boundary {
                                    MeanReversionBoundary::Lower => "LONG",
                                    MeanReversionBoundary::Upper => "SHORT",
                                },
                                price,
                                monitor.extreme_price,
                                reversal_pct,
                                confirmation_pct,
                                if !enable_confirmations {
                                    "NONE"
                                } else if require_candle_confirmation {
                                    "CANDLE"
                                } else {
                                    "TICK"
                                },
                            ),
                        );
                    }
                }

                continue;
            }

            // ====================================================
            // DETECT NEW BOUNDARY APPROACH
            // ====================================================

            // New boundary setups are detected only on ticks.
            //
            // Candle-close events can confirm existing setups,
            // but cannot create new ones.

            if candle_closed {
                continue;
            }

            let Some(boundary) = current_boundary else {
                continue;
            };

            self.mean_reversion_boundary_monitors.insert(
                key,
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
                        "[RESEARCH V4 BACKTEST BOUNDARY APPROACH][{}][{}] \
                         SIDE={:?} | PRICE=${:.4} | \
                         LOW=${:.4} | HIGH=${:.4} | \
                         PROXIMITY={:.1}%",
                        symbol,
                        config.id,
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
}




impl ResearchV4BacktestStrategy {

    // ============================================================
    // MONITOR BREAKOUT SETUPS
    // ============================================================

    /// Independently monitors pending breakout setups for every
    /// enabled breakout configuration.
    ///
    /// Breakout lifecycle:
    /// 1. A validated mean-reversion range fails.
    /// 2. A pending breakout setup is created.
    /// 3. Monitor price against the configuration's threshold.
    /// 4. Confirm the breakout when its conditions are satisfied.
    /// 5. Cancel the setup if price returns inside the range.
    /// 6. Expire the setup if confirmation takes too long.
    ///
    /// Confirmation:
    /// - Candle-based configurations require a completed candle.
    /// - Tick-based configurations can confirm intrabar.
    /// - Each configuration uses its own confirmation percentage.
    ///
    /// Cancellation:
    /// - Bullish setups cancel when price returns inside the range.
    /// - Bearish setups cancel when price returns inside the range.
    /// - Confirmed setups remain available for entry execution.
    ///
    /// Expiration:
    /// - Pending setups expire after their configured lifetime.
    /// - Confirmed setups are not expired by this function.
    ///
    /// This function does not submit trades.
    /// Trade execution is handled separately.

    fn monitor_breakout_setups(
        &mut self,
        symbol: &str,
        price: f64,
        current_ts: i64,
        candle_closed: bool,
    ) {
        // ====================================================
        // VALIDATE PRICE
        // ====================================================

        if !price.is_finite() || price <= 0.0 {
            return;
        }

        // ====================================================
        // EVALUATE ALL BREAKOUT CONFIGURATIONS
        // ====================================================

        for config in self.breakout_configs.iter() {

            // ====================================================
            // GET MATCHING BREAKOUT SETUPS
            // ====================================================

            // A single breakout configuration may have setups
            // originating from multiple mean-reversion ranges.

            let keys: Vec<BreakoutConfigKey> = self
                .breakout_setups
                .keys()
                .filter(|key| {
                    key.symbol == symbol
                        && key.breakout_config_id == config.id
                })
                .cloned()
                .collect();

            // ====================================================
            // EVALUATE EACH BREAKOUT SETUP
            // ====================================================

            for key in keys {

                // ====================================================
                // CHECK CONFIGURATION STATUS
                // ====================================================

                if !config.enabled {
                    self.breakout_setups.remove(&key);
                    continue;
                }

                // ====================================================
                // GET BREAKOUT SETUP
                // ====================================================

                let Some(setup) =
                    self.breakout_setups.get_mut(&key)
                else {
                    continue;
                };

                // ====================================================
                // SKIP CONFIRMED / SUBMITTED SETUPS
                // ====================================================

                if setup.confirmed || setup.trade_submitted {
                    continue;
                }

                // ====================================================
                // CHECK SETUP EXPIRATION
                // ====================================================

                let elapsed_ms =
                    current_ts.saturating_sub(
                        setup.breakout_ts_ms,
                    );

                let expiration_ms =
                    (config.setup_expiration_secs as i64)
                        .saturating_mul(1000);

                if elapsed_ms >= expiration_ms {

                    if self.debug_mode {
                        print_log(
                            self.debug_mode,
                            true,
                            format_args!(
                                "[RESEARCH V4 BACKTEST BREAKOUT EXPIRED]\
                                 [{}][{}][{}] \
                                 DIRECTION={:?} | \
                                 AGE={}s | \
                                 MAX_AGE={}s",
                                symbol,
                                key.range_config_id,
                                config.id,
                                setup.direction,
                                elapsed_ms / 1000,
                                config.setup_expiration_secs,
                            ),
                        );
                    }

                    self.breakout_setups.remove(&key);
                    continue;
                }

                // ====================================================
                // CHECK RANGE REENTRY
                // ====================================================

                // A bullish breakout fails if price returns
                // to or below the original upper boundary.
                //
                // A bearish breakout fails if price returns
                // to or above the original lower boundary.

                let returned_inside_range =
                    match setup.direction {

                        BreakoutDirection::Bullish => {
                            price <= setup.upper_boundary
                        }

                        BreakoutDirection::Bearish => {
                            price >= setup.lower_boundary
                        }
                    };

                if returned_inside_range {

                    if self.debug_mode {
                        print_log(
                            self.debug_mode,
                            true,
                            format_args!(
                                "[RESEARCH V4 BACKTEST BREAKOUT CANCELLED]\
                                 [{}][{}][{}] \
                                 DIRECTION={:?} | \
                                 PRICE=${:.6} | \
                                 RANGE_LOW=${:.6} | \
                                 RANGE_HIGH=${:.6} | \
                                 REASON=RANGE_REENTRY",
                                symbol,
                                key.range_config_id,
                                config.id,
                                setup.direction,
                                price,
                                setup.lower_boundary,
                                setup.upper_boundary,
                            ),
                        );
                    }

                    self.breakout_setups.remove(&key);
                    continue;
                }

                // ====================================================
                // CHECK CONFIRMATION MODE
                // ====================================================

                // Candle-based configurations only confirm
                // using completed candle closes.
                //
                // Tick-based configurations may confirm
                // using live trade prices.

                if config.require_candle_confirmation
                    && !candle_closed
                {
                    continue;
                }

                // ====================================================
                // CALCULATE CONFIRMATION THRESHOLD
                // ====================================================

                let confirmation_buffer =
                    config.confirmation_pct / 100.0;

                if !confirmation_buffer.is_finite()
                    || confirmation_buffer < 0.0
                {
                    continue;
                }

                let upper_confirmation_price =
                    setup.upper_boundary
                        * (1.0 + confirmation_buffer);

                let lower_confirmation_price =
                    setup.lower_boundary
                        * (1.0 - confirmation_buffer);

                // ====================================================
                // CHECK BREAKOUT CONFIRMATION
                // ====================================================

                let breakout_confirmed =
                    match setup.direction {

                        BreakoutDirection::Bullish => {
                            price > upper_confirmation_price
                        }

                        BreakoutDirection::Bearish => {
                            price < lower_confirmation_price
                        }
                    };

                if !breakout_confirmed {
                    continue;
                }

                // ====================================================
                // CONFIRM BREAKOUT SETUP
                // ====================================================

                setup.confirmed = true;

                setup.confirmation_price = Some(price);

                setup.confirmed_ts_ms = Some(current_ts);

                // ====================================================
                // BREAKOUT CONFIRMATION DEBUG
                // ====================================================

                if self.debug_mode {
                    print_log(
                        self.debug_mode,
                        true,
                        format_args!(
                            "[RESEARCH V4 BACKTEST BREAKOUT CONFIRMED]\
                             [{}][{}][{}] \
                             DIRECTION={:?} | \
                             PRICE=${:.6} | \
                             RANGE_LOW=${:.6} | \
                             RANGE_HIGH=${:.6} | \
                             CONFIRMATION={:.3}% | \
                             AGE={}s | \
                             MODE={}",
                            symbol,
                            key.range_config_id,
                            config.id,
                            setup.direction,
                            price,
                            setup.lower_boundary,
                            setup.upper_boundary,
                            config.confirmation_pct,
                            elapsed_ms / 1000,
                            if config.require_candle_confirmation {
                                "CANDLE"
                            } else {
                                "TICK"
                            },
                        ),
                    );
                }
            }
        }
    }
}




impl ResearchV4BacktestStrategy {

    // ============================================================
    // CLEANUP BREAKOUT SETUPS
    // ============================================================

    /// Removes breakout setups that have already been consumed.
    ///
    /// Cleanup behavior:
    /// - Removes setups marked as trade_submitted.
    /// - Preserves pending setups awaiting confirmation.
    /// - Preserves confirmed setups awaiting execution.
    /// - Operates independently for each asset.
    ///
    /// Note:
    /// trade_submitted indicates that the signal was consumed,
    /// even if trading eligibility prevented order submission.
    ///
    /// This function does not affect active engine trades.

    fn cleanup_breakout_setups(
        &mut self,
        symbol: &str,
    ) {
        // ====================================================
        // REMOVE CONSUMED BREAKOUT SETUPS
        // ====================================================

        self.breakout_setups.retain(|key, setup| {

            // Preserve setups belonging to other assets.
            if key.symbol != symbol {
                return true;
            }

            // Remove consumed setups.
            !setup.trade_submitted
        });
    }
}




impl ResearchV4BacktestStrategy {

    // ============================================================
    // EXECUTE MEAN REVERSION ENTRY
    // ============================================================

    /// Executes confirmed mean-reversion entries independently
    /// for every enabled mean-reversion configuration.
    ///
    /// Entry behavior:
    /// - Evaluates all enabled configurations for the asset.
    /// - Consumes confirmed signals immediately, even when trading is blocked.
    /// - Determines LONG or SHORT direction from the confirmed boundary.
    /// - Calculates range-based take-profit and stop-loss levels.
    /// - Converts absolute exit prices into engine TP/SL percentages.
    /// - Rejects invalid configurations and exit geometry.
    /// - Allows multiple trades on the same asset.
    /// - Enforces the strategy's global maximum active-trade limit.
    /// - Submits trades through EngineEvent::OpenTrade.
    ///
    /// Risk management:
    /// - Each configuration independently determines its TP/SL.
    /// - TP/SL distances are calculated using validated range geometry.
    /// - The engine applies percentages relative to the actual fill price.
    /// - Emergency stop-loss protection remains enabled.
    /// - Trade duration is controlled by each configuration.
    /// - The engine manages all trade execution and exits.

    fn execute_mean_reversion_entry(
        &mut self,
        symbol: &str,
        price: f64,
        current_ts: i64,
        can_trade: bool,
    ) {
        // ====================================================
        // VALIDATE PRICE
        // ====================================================

        if !price.is_finite() || price <= 0.0 {
            return;
        }

        // ====================================================
        // EVALUATE ALL MEAN REVERSION CONFIGURATIONS
        // ====================================================

        for config in self.mean_reversion_configs.iter() {

            if !config.enabled {
                continue;
            }

            let key = StrategyConfigKey::new(
                symbol,
                &config.id,
            );

            // ====================================================
            // GET CONFIRMED SETUP
            // ====================================================

            let Some(monitor) =
                self.mean_reversion_boundary_monitors.get_mut(&key)
            else {
                continue;
            };

            if !monitor.confirmed || monitor.trade_submitted {
                continue;
            }

            // ====================================================
            // CONSUME CONFIRMED SETUP
            // ====================================================

            // Consume immediately, even when trading is blocked.
            // Prevents stale signals from executing later.

            monitor.trade_submitted = true;

            let boundary = monitor.boundary;

            // ====================================================
            // VALIDATE TRADING ELIGIBILITY
            // ====================================================

            if !can_trade
                || self.black_swan_protection.is_halted()
                || !self.trade_cooldown.can_trade(symbol, current_ts)
            {
                continue;
            }

            // ====================================================
            // GET CONFIGURATION RISK SETTINGS
            // ====================================================

            let profit_fraction =
                config.risk.range_profit_target_fraction;

            let stop_buffer_pct =
                config.risk.stop_loss_boundary_buffer_pct;

            if !profit_fraction.is_finite()
                || profit_fraction <= 0.0
                || profit_fraction > 1.0
                || !stop_buffer_pct.is_finite()
                || stop_buffer_pct < 0.0
            {
                continue;
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
                self.mean_reversion_ranges.get(&key)
            else {
                continue;
            };

            let lower_boundary = range.lower_boundary;
            let upper_boundary = range.upper_boundary;

            let range_width =
                upper_boundary - lower_boundary;

            if !lower_boundary.is_finite()
                || !upper_boundary.is_finite()
                || lower_boundary <= 0.0
                || upper_boundary <= lower_boundary
                || !range_width.is_finite()
            {
                continue;
            }

            // ====================================================
            // CALCULATE RANGE-BASED TAKE PROFIT / STOP LOSS
            // ====================================================

            // LONG:
            // TP = lower + (range_width * profit_fraction)
            // SL = lower * (1 - stop_buffer_pct / 100)
            //
            // SHORT:
            // TP = upper - (range_width * profit_fraction)
            // SL = upper * (1 + stop_buffer_pct / 100)

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
                continue;
            }

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
                continue;
            }

            // ====================================================
            // CONVERT EXIT PRICES TO ENGINE PERCENTAGES
            // ====================================================

            // The engine applies TP/SL relative to the actual
            // average entry fill price.

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
                continue;
            }

            // ====================================================
            // CHECK GLOBAL ACTIVE TRADE LIMIT
            // ====================================================

            // Includes Opening, Open, and Closing trades.
            //
            // Multiple trades on the same asset are allowed.
            // Only the strategy-wide position limit is enforced.

            let active_trade_count = {
                let view = self.rx_view.borrow();

                view.trades.count_active_for_strategy(
                    self.name(),
                )
            };

            if active_trade_count >= self.max_trades_allowed {
                continue;
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
                trade_duration_secs: config.risk.trade_duration_secs,

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
                            "[RESEARCH V4 BACKTEST {:?} ENTRY][{}][{}] \
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
                            config.id,
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
}




impl ResearchV4BacktestStrategy {

    // ============================================================
    // EXECUTE BREAKOUT ENTRY
    // ============================================================

    /// Executes confirmed breakout entries independently
    /// for every enabled breakout configuration.
    ///
    /// Entry behavior:
    /// - Evaluates confirmed breakout setups for the asset.
    /// - Supports multiple breakout configurations per asset.
    /// - Supports breakouts originating from different ranges.
    /// - Consumes confirmed signals immediately.
    /// - Determines direction from the confirmed breakout.
    /// - Validates configuration-specific risk settings.
    /// - Enforces trading eligibility and protection rules.
    /// - Allows multiple trades on the same asset.
    /// - Enforces the strategy's global active-trade limit.
    /// - Submits trades through EngineEvent::OpenTrade.
    ///
    /// Risk management:
    /// - Each breakout configuration has independent TP/SL.
    /// - TP/SL percentages are relative to the entry fill price.
    /// - Emergency stop-loss protection remains enabled.
    /// - Trade duration is controlled by BreakoutRiskConfig.
    /// - The engine manages execution and trade exits.

    fn execute_breakout_entry(
        &mut self,
        symbol: &str,
        price: f64,
        current_ts: i64,
        can_trade: bool,
    ) {
        // ====================================================
        // VALIDATE PRICE
        // ====================================================

        if !price.is_finite() || price <= 0.0 {
            return;
        }

        // ====================================================
        // EVALUATE ALL BREAKOUT CONFIGURATIONS
        // ====================================================

        for config in self.breakout_configs.iter() {

            if !config.enabled {
                continue;
            }

            // ====================================================
            // GET MATCHING BREAKOUT SETUPS
            // ====================================================

            // Multiple mean-reversion configurations may
            // generate breakouts for the same asset.

            let keys: Vec<BreakoutConfigKey> = self
                .breakout_setups
                .keys()
                .filter(|key| {
                    key.symbol == symbol
                        && key.breakout_config_id == config.id
                })
                .cloned()
                .collect();

            // ====================================================
            // EVALUATE EACH CONFIRMED SETUP
            // ====================================================

            for key in keys {

                // ====================================================
                // GET CONFIRMED BREAKOUT SETUP
                // ====================================================

                let Some(setup) =
                    self.breakout_setups.get_mut(&key)
                else {
                    continue;
                };

                if !setup.confirmed || setup.trade_submitted {
                    continue;
                }

                // ====================================================
                // CONSUME CONFIRMED SETUP
                // ====================================================

                // Consume immediately, even if trading is blocked.
                // Prevents stale signals from executing later.

                setup.trade_submitted = true;

                let breakout_direction = setup.direction;

                // ====================================================
                // VALIDATE TRADING ELIGIBILITY
                // ====================================================

                if !can_trade
                    || self.black_swan_protection.is_halted()
                    || !self.trade_cooldown.can_trade(
                        symbol,
                        current_ts,
                    )
                {
                    continue;
                }

                // ====================================================
                // GET BREAKOUT RISK SETTINGS
                // ====================================================

                let take_profit_percent =
                    config.risk.take_profit_pct;

                let stop_loss_percent =
                    config.risk.stop_loss_pct;

                let trade_duration_secs =
                    config.risk.trade_duration_secs;

                // ====================================================
                // VALIDATE RISK SETTINGS
                // ====================================================

                if !take_profit_percent.is_finite()
                    || !stop_loss_percent.is_finite()
                    || take_profit_percent <= 0.0
                    || stop_loss_percent <= 0.0
                {
                    continue;
                }

                // ====================================================
                // DETERMINE TRADE DIRECTION
                // ====================================================

                let direction = match breakout_direction {
                    BreakoutDirection::Bullish => Direction::Long,
                    BreakoutDirection::Bearish => Direction::Short,
                };

                // ====================================================
                // CHECK GLOBAL ACTIVE TRADE LIMIT
                // ====================================================

                // Includes Opening, Open, and Closing trades.
                //
                // Multiple trades on the same asset are allowed.
                // Only the strategy-wide limit is enforced.

                let active_trade_count = {
                    let view = self.rx_view.borrow();

                    view.trades.count_active_for_strategy(
                        self.name(),
                    )
                };

                if active_trade_count >= self.max_trades_allowed {
                    continue;
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

                    take_profit_percent: Some(
                        take_profit_percent,
                    ),

                    stop_loss_percent: Some(
                        stop_loss_percent,
                    ),

                    emergency_stop_percent:
                        self.emergency_exit_percent,

                    stop_loss_delay_secs:
                        self.min_sl_delay,

                    trade_duration_secs,

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
                                "[RESEARCH V4 BACKTEST BREAKOUT {:?} ENTRY]\
                                 [{}][{}][{}] \
                                 PRICE=${:.6} | \
                                 TP={:.3}% | SL={:.3}% | \
                                 RR=1:{:.3} | \
                                 AMOUNT=${:.2} | \
                                 DURATION={:?}",
                                direction,
                                symbol,
                                key.range_config_id,
                                config.id,
                                price,
                                take_profit_percent,
                                stop_loss_percent,
                                take_profit_percent
                                    / stop_loss_percent,
                                self.trade_amount_usd,
                                trade_duration_secs,
                            ),
                        );
                    }
                }
            }
        }
    }
}




impl ResearchV4BacktestStrategy {

    // ============================================================
    // EVALUATE WEIGHTED TRADE FLIP
    // ============================================================

    /// Evaluates historical consecutive winning trades
    /// and determines whether the next trade signal
    /// should be reversed.
    ///
    /// Uses closed trades retained by the engine view.
    ///
    /// Does not submit orders or modify TP/SL levels.
    fn evaluate_weighted_trade_flip(
        &self,
        signal_id: u64,
    ) -> bool {

        let view = self.rx_view.borrow();

        // ========================================================
        // BUILD WINNING STREAK SNAPSHOT
        // ========================================================

        let snapshot = self.weighted_trade_flip
            .winning_streak_snapshot(
                self.name(),
                &view.trades,
            );

        // ========================================================
        // DETERMINE WEIGHTED FLIP
        // ========================================================

        self.weighted_trade_flip.should_flip(
            &snapshot,
            signal_id,
        )
    }
}