#![allow(dead_code)]

use std::sync::Arc;
use std::collections::HashMap;

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
        execution::order::{
            OrderType,
            TimeInForce,
        },
        trade_intent::TradeIntent,
        trades::Direction,
    },

    market::{
        candle::CandleConfig,
        candle_history::CandleHistory,
        timeframe::Timeframe,
    },

    indicators::{
        price_pct_change,
        volume_usd,
    },

    strategies::Strategy,

    toolbox::price_window_ts::PriceWindowTs,
    toolbox::black_swan_protection::BlackSwanProtection,

    shared::print_log,
};

use super::backtest_state::ResearchV3BacktestStrategy;

/*
TODO:

* Should check for a smooth r2 before entering into a trade.

* The trade should go in to same direction as the trend.

*/



// ============================================================
// STRATEGY DESCRIPTION — RESEARCH V3 (RUNNER STRATEGY)
// ============================================================
//
// A momentum-based trading strategy designed to identify and
// capitalize on high-performing assets ("runners") exhibiting
// unusually large directional price movements over 24 hours (ie >= 15%).
//
// Continuously monitors assets across the market, analyzing
// their 24-hour percentage price changes, trading volume,
// price action, and momentum to identify emerging and
// established directional trends.
//
// The strategy seeks to identify assets experiencing strong
// upward or downward momentum and enter trades in the same
// direction as the prevailing price movement.
//
// Long positions target assets exhibiting strong positive
// momentum, while short positions target assets exhibiting
// strong negative momentum.
//
// Entry conditions prioritize sustained directional movement,
// trend strength, and confirmation that momentum is continuing
// rather than reversing or becoming exhausted.
//
// Trade management prioritizes dynamic trailing stop-loss
// mechanisms to capture extended directional movements while
// progressively protecting accumulated unrealized profits.
//
// The strategy aims to participate in significant price
// movements without relying on fixed profit targets, allowing
// winning positions to remain open while momentum persists.
//
// The primary objective is to systematically locate the
// market's strongest directional movers, enter positions
// aligned with their prevailing trends, and maximize profit
// potential by capturing continued price expansion.
//
// ============================================================



impl ResearchV3BacktestStrategy {
    pub fn new(
            tx_engine: UnboundedSender<EngineEvent>,
            rx_view: watch::Receiver<Arc<EngineView>>,
        ) -> Self {
            let candle_config = CandleConfig::default();

            Self {
                enable_strategy: true,
                debug_mode: true,
                tx_engine,
                rx_view,
                trade_amount_usd: 100.0,
                entry_percent: Some(2.0),
                exit_profit_percent: Some(2.0),
                exit_loss_percent: Some(2.0),
                emergency_exit_percent: Some(5.0),
                min_sl_delay: Some(1800), // 30 min
                trade_duration_secs: Some(3_600), // 1 hour
                min_volume_usd: Some(450_000.0),
                max_24h_move_pct: None,
                track_price_window_while_in_trade: false,
                price_window: PriceWindowTs::new(
                    1800, // 30 min rolling window
                    false,
                ),
                candle_history: CandleHistory::new(
                    candle_config.history_retention,
                ),
                can_trade: HashMap::new(),
                black_swan_protection: BlackSwanProtection::new(
                    true,   // Enable protection.
                    false,  // Enable or disable debug logging.
                    15.0,   // Halt when average absolute 24-hour movement reaches 15%.
                    8.0,    // Resume when average movement falls to 8%.
                    15,     // Minimum number of assets required.
                ),
            }
        }
}


pub fn make_research_v3_backtest(
    tx_engine: UnboundedSender<EngineEvent>,
    rx_view: watch::Receiver<Arc<EngineView>>,
) -> Box<dyn Strategy> {
    Box::new(
        ResearchV3BacktestStrategy::new(
            tx_engine,
            rx_view,
        )
    )
}


// ============================================================
// STRATEGY IMPLEMENTATION
// ============================================================

#[async_trait::async_trait]
impl Strategy for ResearchV3BacktestStrategy {
    fn name(&self) -> &'static str {
        "ResearchV3BacktestStrategy"
    }

    async fn on_event(
        &mut self,
        ev: AppEventEnvelope,
    ) {
        if !self.enable_strategy {
            return;
        }

        match ev.event {

            AppEvent::TapeTrade(trade) => {

                // Read the latest cached trading eligibility for this symbol.
                let can_trade = self.can_trade
                    .get(&ev.symbol)
                    .copied()
                    .unwrap_or(false);

                if self.debug_mode {
                    print_log(
                        self.debug_mode,
                        true,
                        format_args!(
                            "[RESEARCH V3 BACKTEST TRADE][{}] \
                            price=${:.4} | size={} | timestamp_ms={}",
                            ev.symbol,
                            trade.price,
                            trade.size,
                            trade.timestamp_ms,
                        ),
                    );
                }

                // Update per asset price tracking window.
                let result = self.price_window
                    .update_price_window_extreme_diffs_ts(
                        &ev.symbol,
                        trade.timestamp_ms,
                        trade.price,
                    );

                if let Some((
                    pct_from_min,
                    pct_from_max,
                    elapsed_secs,
                )) = result {

                    // ====================================================
                    // ENTRY THRESHOLD
                    // ====================================================

                    if let Some(entry_percent) = self.entry_percent {

                        if self.debug_mode {
                            print_log(
                                self.debug_mode,
                                false,
                                format_args!(
                                    "[RESEARCH V3 BACKTEST WINDOW][{}] \
                                    price=${:.4} | \
                                    above_min={:.3}% | \
                                    below_max={:.3}% | \
                                    age={}s | \
                                    entry_threshold={:.3}%",
                                    ev.symbol,
                                    trade.price,
                                    pct_from_min,
                                    pct_from_max,
                                    elapsed_secs,
                                    entry_percent,
                                ),
                            );
                        }

                        // ====================================================
                        // TRADING ELIGIBILITY
                        // ====================================================

                        // New entries require:
                        // - Individual asset trading eligibility.
                        // - Market-wide black swan protection is inactive.
                        if can_trade && !self.black_swan_protection.is_halted() {

                            // ====================================================
                            // SHORT ENTRY
                            // ====================================================

                            if pct_from_min >= entry_percent {

                                let intent = TradeIntent {
                                    strategy_id: self.name().to_string(),
                                    symbol: ev.symbol.clone(),
                                    direction: Direction::Short,
                                    signal_px: trade.price,
                                    target_notional: self.trade_amount_usd,
                                    margin_usd: self.trade_amount_usd,
                                    order_type: OrderType::Market,
                                    time_in_force: TimeInForce::Day,
                                    limit_px: None,
                                    stop_px: None,
                                    post_only: false,
                                    take_profit_percent: self.exit_profit_percent,
                                    stop_loss_percent: self.exit_loss_percent,
                                    emergency_stop_percent: self.emergency_exit_percent,
                                    stop_loss_delay_secs: self.min_sl_delay,
                                    trade_duration_secs: self.trade_duration_secs,
                                    trailing_stop: None,
                                    ts_ms: trade.timestamp_ms,
                                };

                                // ====================================================
                                // SHORT ENTRY DEBUG
                                // ====================================================

                                if self.debug_mode {
                                    print_log(
                                        self.debug_mode,
                                        false,
                                        format_args!(
                                            "[RESEARCH V3 BACKTEST SHORT ENTRY][{}] \
                                            price=${:.4} | \
                                            above_min={:.3}% >= {:.3}% | \
                                            amount=${:.2}",
                                            ev.symbol,
                                            trade.price,
                                            pct_from_min,
                                            entry_percent,
                                            self.trade_amount_usd,
                                        ),
                                    );
                                }

                                // ====================================================
                                // SEND TRADE INTENT
                                // ====================================================

                                // TODO: Enable once execution is ready.
                                //
                                // let _ = self.tx_engine.send(
                                //     EngineEvent::OpenTrade(intent)
                                // );

                                let _ = intent;
                            }

                            // ====================================================
                            // LONG ENTRY
                            // ====================================================

                            else if pct_from_max >= entry_percent {

                                let intent = TradeIntent {
                                    strategy_id: self.name().to_string(),
                                    symbol: ev.symbol.clone(),
                                    direction: Direction::Long,
                                    signal_px: trade.price,
                                    target_notional: self.trade_amount_usd,
                                    margin_usd: self.trade_amount_usd,
                                    order_type: OrderType::Market,
                                    time_in_force: TimeInForce::Day,
                                    limit_px: None,
                                    stop_px: None,
                                    post_only: false,
                                    take_profit_percent: self.exit_profit_percent,
                                    stop_loss_percent: self.exit_loss_percent,
                                    emergency_stop_percent: self.emergency_exit_percent,
                                    stop_loss_delay_secs: self.min_sl_delay,
                                    trade_duration_secs: self.trade_duration_secs,
                                    trailing_stop: None,
                                    ts_ms: trade.timestamp_ms,
                                };

                                // ====================================================
                                // LONG ENTRY DEBUG
                                // ====================================================

                                if self.debug_mode {
                                    print_log(
                                        self.debug_mode,
                                        false,
                                        format_args!(
                                            "[RESEARCH V3 BACKTEST LONG ENTRY][{}] \
                                            price=${:.4} | \
                                            below_max={:.3}% >= {:.3}% | \
                                            amount=${:.2}",
                                            ev.symbol,
                                            trade.price,
                                            pct_from_max,
                                            entry_percent,
                                            self.trade_amount_usd,
                                        ),
                                    );
                                }

                                // ====================================================
                                // SEND TRADE INTENT
                                // ====================================================

                                // TODO: Enable once execution is ready.
                                //
                                // let _ = self.tx_engine.send(
                                //     EngineEvent::OpenTrade(intent)
                                // );

                                let _ = intent;
                            }
                        }
                    }
                }
            }

            AppEvent::Quote(quote) => {
                if self.debug_mode {
                    print_log(
                        self.debug_mode,
                        true,
                        format!(
                            "[RESEARCH V3 BACKTEST QUOTE][{}] \
                             bid=${:.4} | ask=${:.4}",
                            ev.symbol,
                            quote.bid_price,
                            quote.ask_price,
                        ),
                    );
                }
            }

            AppEvent::BarClosed(bar) => {
                let timeframe = bar.timeframe;
                let current_ts = bar.end.timestamp_millis();

                // Store completed candle.
                self.candle_history.push(bar);

                self.update_trading_eligibility(
                    &ev.symbol,
                    timeframe,
                    current_ts,
                );

                self.update_black_swan_protection(
                    &ev.symbol,
                    timeframe,
                    current_ts,
                );

                if self.debug_mode {
                    print_log(
                        self.debug_mode,
                        true,
                        format_args!(
                            "[RESEARCH V3 BACKTEST CANDLE][{}][{}s] \
                            stored={}",
                            ev.symbol,
                            timeframe.as_secs(),
                            self.candle_history.len(
                                &ev.symbol,
                                timeframe,
                            ),
                        ),
                    );
                }
            }
        }
    }
}




impl ResearchV3BacktestStrategy {

    // ============================================================
    // UPDATE TRADING ELIGIBILITY
    // ============================================================

    // Updates trading eligibility for each asset using validated 24-hour
    // market statistics. Runs on completed one-minute candles.
    //
    // Checks:
    // - At least 1440 completed one-minute candles (24 hours).
    // - Valid and complete 24-hour candle history.
    // - Valid 24-hour USD trading volume.
    // - Minimum 24-hour USD volume requirement (if enabled).
    // - Valid 24-hour percentage price movement.
    // - Maximum absolute 24-hour price movement (if enabled).
    //
    // Caches the final eligibility result per asset.
    // Trading is only allowed when all requirements pass.
    fn update_trading_eligibility(
        &mut self,
        symbol: &str,
        timeframe: Timeframe,
        current_ts: i64,
    ) {
        // Only recalculate when a completed one-minute candle arrives.
        if timeframe != Timeframe::ONE_MINUTE {
            return;
        }

        // Number of one-minute candles in 24 hours.
        // Change to 30 for testing.
        let period: usize = 1440;

        // Default to false until all requirements pass.
        let mut requirements_met = false;

        // ============================================================
        // GET ONE-MINUTE CANDLE HISTORY
        // ============================================================

        if let Some(bars) = self.candle_history.get(
            symbol,
            Timeframe::ONE_MINUTE,
        ) {
            // Require 1440 completed one-minute candles.
            if bars.len() >= period {

                // Collect the most recent candles in chronological order.
                // VecDeque is not guaranteed to be contiguous.
                let bars_vec: Vec<Bar> = bars
                    .iter()
                    .rev()
                    .take(period)
                    .cloned()
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect();

                // ============================================================
                // VALIDATED 24-HOUR INDICATORS
                // ============================================================

                // USD volume accumulated from completed candles.
                // Internally validates the candle window.
                let volume_usd_24h = volume_usd(
                    &bars_vec,
                    period,
                    current_ts,
                );

                // Percentage price movement over the same window.
                // Internally validates the candle window.
                let price_change_24h = price_pct_change(
                    &bars_vec,
                    period,
                    current_ts,
                );

                // ============================================================
                // MINIMUM 24-HOUR USD VOLUME
                // ============================================================

                // Some(min_volume) = Require minimum USD volume.
                // None = Disable the minimum volume restriction.
                let has_enough_volume: bool = match self.min_volume_usd {
                    Some(min_volume) => {
                        volume_usd_24h.is_some_and(|usd_volume| {
                            usd_volume >= min_volume
                        })
                    }

                    None => true,
                };

                // ============================================================
                // MAXIMUM 24-HOUR PRICE MOVEMENT
                // ============================================================

                // Some(max_pct) = Require absolute price movement
                // to remain within the configured limit.
                // None = Disable the movement restriction.
                let within_24h_move_limit = match self.max_24h_move_pct {
                    Some(max_pct) => {
                        price_change_24h.is_some_and(|change_pct| {
                            change_pct.abs() <= max_pct
                        })
                    }

                    None => true,
                };

                // ============================================================
                // TRADING REQUIREMENTS
                // ============================================================

                // Both indicators must return valid results.
                // If either returns None, trading is disabled.
                requirements_met =
                    has_enough_volume
                    && within_24h_move_limit
                    && volume_usd_24h.is_some()
                    && price_change_24h.is_some();

                // ============================================================
                // MARKET STATISTICS DEBUG
                // ============================================================

                if self.debug_mode {
                    print_log(
                        self.debug_mode,
                        false,
                        format_args!(
                            "[RESEARCH V3 BACKTEST 24H MARKET STATS][{}] \
                             CHANGE={:?}% | \
                             USD_VOLUME={:?} | \
                             ENOUGH_VOLUME={} | \
                             WITHIN_MOVE_LIMIT={} | \
                             CAN_TRADE={}",
                            symbol,
                            price_change_24h,
                            volume_usd_24h,
                            has_enough_volume,
                            within_24h_move_limit,
                            requirements_met,
                        ),
                    );
                }
            }
        }

        // ============================================================
        // CACHE TRADING ELIGIBILITY
        // ============================================================

        // Each asset maintains its own independent eligibility state.
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
                    "[RESEARCH V3 BACKTEST ELIGIBILITY][{}] \
                     can_trade={}",
                    symbol,
                    requirements_met,
                ),
            );
        }
    }
}




impl ResearchV3BacktestStrategy {

    // ============================================================
    // UPDATE BLACK SWAN PROTECTION
    // ============================================================

    // Updates market-wide volatility protection independently
    // of individual asset trading eligibility.
    //
    // Checks:
    // - Only processes completed one-minute candles.
    // - Requires 1440 completed one-minute candles.
    // - Calculates validated 24-hour price movement.
    // - Updates the shared market-wide protection state.
    // - Automatically evaluates trading halt conditions.
    fn update_black_swan_protection(
        &mut self,
        symbol: &str,
        timeframe: Timeframe,
        current_ts: i64,
    ) {
        if timeframe != Timeframe::ONE_MINUTE {
            return;
        }

        // Number of one-minute candles in 24 hours.
        let period: usize = 1440;

        // Default to None until valid statistics are available.
        let mut price_change_24h: Option<f64> = None;

        // ============================================================
        // GET ONE-MINUTE CANDLE HISTORY
        // ============================================================

        if let Some(bars) = self.candle_history.get(
            symbol,
            Timeframe::ONE_MINUTE,
        ) {
            if bars.len() >= period {

                // Collect the latest candles in chronological order.
                let bars_vec: Vec<Bar> = bars
                    .iter()
                    .rev()
                    .take(period)
                    .cloned()
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect();

                // ============================================================
                // VALIDATED 24-HOUR PRICE MOVEMENT
                // ============================================================

                price_change_24h = price_pct_change(
                    &bars_vec,
                    period,
                    current_ts,
                );
            }
        }

        // ============================================================
        // UPDATE BLACK SWAN PROTECTION
        // ============================================================

        self.black_swan_protection.update(
            symbol,
            price_change_24h,
            current_ts,
        );
    }
}