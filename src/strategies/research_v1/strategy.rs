#![allow(dead_code)]

use std::sync::Arc;
use std::collections::HashMap;

use tokio::sync::{
    mpsc::UnboundedSender,
    watch,
};

use crate::{
    core::{
        trades::state::TradeState,
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
        execution::ExitOrderRequest,
        trade_intent::TradeIntent,
        trades::Direction,
        risk::trailing_stop::{
            TrailingStopConfig,
            TrailingStopLevel,
        },
    },

    market::{
        candle::CandleConfig,
        candle_history::CandleHistory,
        timeframe::Timeframe,
    },

    indicators::{
        price_pct_change,
        volume_usd,
        market_volume_expansion,
        market_volume_acceleration,
        market_breadth,
    },

    strategies::Strategy,

    toolbox::MarketRegime,
    toolbox::MultiSync,
    toolbox::price_window_ts::PriceWindowTs,
    toolbox::TradeCooldown,
    toolbox::TradeDurationWatchdog,
    toolbox::black_swan_protection::BlackSwanProtection,
    toolbox::early_profit_taking::{
        EarlyProfitTaking,
        EarlyProfitRule,
    },
    toolbox::multi_sync::{
        MultiSyncConfig,
        MultiSyncSetting,
        MultiSyncProfitProtectionConfig,
        MultiSyncDrawdownProtectionConfig,
    },

    shared::print_log,
};

use super::state::ResearchV1Strategy;




// ============================================================
// STRATEGY DESCRIPTION
// ============================================================
//
// A regime-adaptive trading strategy combining mean reversion
// and momentum-based trend following.
//
// The primary strategy operates on mean-reversion principles,
// identifying entry opportunities when price deviates beyond
// a configurable percentage threshold relative to the extrema
// of a rolling price window. These deviations are treated as
// potential short-term price dislocations, with positions
// established in anticipation of a reversion toward equilibrium.
//
// The strategy also incorporates a trend-following mode designed
// to adapt to market conditions where mean reversion becomes
// ineffective. This mode reverses the directional interpretation
// of entry signals, converting countertrend positions into
// momentum-aligned trades. This provides a mechanism for adapting
// to changing market regimes rather than relying exclusively
// on a single directional trading approach.
// ============================================================




impl ResearchV1Strategy {
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

            max_trades_allowed: 8,
            reference_margin_usd: 1600.0,
            trade_amount_usd: 200.0,

            leverage: None,
            entry_percent: Some(2.0),
            exit_profit_percent: Some(2.0),
            exit_loss_percent: Some(2.0),
            emergency_exit_percent: Some(5.0),
            min_sl_delay: Some(1800), // 30 min
            trade_duration_secs: Some(3_600), // 1 hour
            min_volume_usd: Some(450_000.0), 
            max_24h_move_pct: Some(15.00),

            market_regime: MarketRegime::MeanReversion,

            track_price_window_while_in_trade: false,
            price_window: PriceWindowTs::new(
                1800,  // 30 min rolling window
                false, // Enable or disable debug logging.
            ),

            candle_history: CandleHistory::new(
                candle_config.history_retention,
            ),

            // false = bypass can_trade check, 
            // true = enforce can_trade check.
            enable_trading_eligibility_check: true, 
            can_trade: HashMap::new(),

            black_swan_protection: BlackSwanProtection::new(
                true,   // Enable protection.
                false,   // Enable or disable debug logging.
                15.0,   // Halt when average absolute 24-hour movement reaches 15%.
                8.0,    // Resume when average movement falls to 8%.
                15,     // Minimum number of assets required.
            ),

            trade_cooldown: TradeCooldown::new(200),

            trade_duration_watchdog: TradeDurationWatchdog::new(
                true,   // Enable watchdog.
                10.0,   // Allow 10% beyond trade duration.
                300_000,  // Check every 5 minutes.
            ),

            early_profit_taking: EarlyProfitTaking::new(
                vec![
                    EarlyProfitRule {
                        name: "Standard Early Profit".to_string(),  // Rule identifier.
                        enabled: false,                              // Enable this rule.
                        min_open_trades: 1,                         // Require at least 1 open trade.
                        max_open_trades: None,                      // No maximum open trade limit.
                        min_margin_usd: 100.0,                      // Require at least $100 total basket margin.
                        min_profit_percent: 4.3,                    // Close basket when net profit reaches 4.3%.
                        no_new_trade_for_ms: None,                  // No minimum inactivity period.
                    },

                    EarlyProfitRule {
                        name: "Slow Market Early Profit".to_string(), // Rule identifier.
                        enabled: false,                               // Disable this rule.
                        min_open_trades: 2,                           // Require at least 2 open trades.
                        max_open_trades: Some(4),                     // Allow a maximum of 4 open trades.
                        min_margin_usd: 200.0,                        // Require at least $200 total basket margin.
                        min_profit_percent: 0.9,                      // Close basket when net profit reaches 0.9%.
                        no_new_trade_for_ms: Some(15 * 60 * 1000),    // Require 15 minutes without a new trade.
                    },
                ],
                5_000, // Minimum 5 seconds between early-profit exit requests.
            ),

            multi_sync: MultiSync::new(
                MultiSyncConfig {
                    enabled: true,
                    debug_mode: true,

                    profit_protection: MultiSyncProfitProtectionConfig {
                        enabled: true,

                        default_settings: vec![
                            MultiSyncSetting {
                                threshold_pct: 0.98,
                                window_ms: Some(90 * 60 * 1000), // 90 minutes.
                            },
                        ],
                    },

                    drawdown_protection: MultiSyncDrawdownProtectionConfig {
                        enabled: true,

                        default_settings: vec![
                            MultiSyncSetting {
                                threshold_pct: 0.24,
                                window_ms: Some(240 * 60 * 1000), // 240 minutes.
                            },
                        ],
                    },
                },
                MarketRegime::MeanReversion,
            )
            .expect("Invalid Multi-Sync configuration"),
        }
    }
}


// ============================================================
// FACTORY
// ============================================================

pub fn make_research_v1(
    tx_engine: UnboundedSender<EngineEvent>,
    rx_view: watch::Receiver<Arc<EngineView>>,
) -> Box<dyn Strategy> {
    Box::new(
        ResearchV1Strategy::new(
            tx_engine,
            rx_view,
        )
    )
}


// ============================================================
// STRATEGY IMPLEMENTATION
// ============================================================

#[async_trait::async_trait]
impl Strategy for ResearchV1Strategy {
    fn name(&self) -> &'static str {
        "ResearchV1Strategy"
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
         
                if self.debug_mode {
                    print_log(
                        self.debug_mode,
                        true,
                        format_args!(
                            "[RESEARCH V1 LIVE TRADE][{}] \
                            price=${:.4} | size={} | timestamp_ms={}",
                            ev.symbol,
                            trade.price,
                            trade.size,
                            trade.timestamp_ms,
                        ),
                    );
                }
            }

            AppEvent::Quote(quote) => {

                let mid = (quote.bid_price + quote.ask_price) / 2.0;

                if self.debug_mode {
                    print_log(
                        self.debug_mode,
                        true,
                        format!(
                            "[RESEARCH V1 LIVE QUOTE][{}] \
                            bid=${:.4} | ask=${:.4} | mid=${:.4}",
                            ev.symbol,
                            quote.bid_price,
                            quote.ask_price,
                            mid,
                        ),
                    );
                }

                // ====================================================
                // MULTI-SYNC CLOSED TRADE PROCESSING
                // ====================================================
                //
                // Poll the latest authoritative EngineView for completed
                // trades belonging to this strategy.
                //
                // Multi-Sync:
                // - Processes newly completed trades.
                // - Calculates net realized PnL.
                // - Prevents duplicate trade processing.
                // - Evaluates Profit Protection thresholds.
                // - Evaluates Drawdown Protection thresholds.
                // - Activates or completes protection windows.
                // - Updates the strategy's market regime.
                //
                // The backtest runner synchronizes engine commands,
                // execution commands, and execution events before
                // advancing to the next historical market event.
                {
                    let view = self.rx_view.borrow();

                    self.multi_sync.process_closed_trades(
                        &view.trades,
                        self.name(),
                        self.reference_margin_usd,
                        quote.timestamp_ms,
                        &mut self.market_regime,
                    );
                }

                self.check_trade_duration_watchdog(
                    quote.timestamp_ms,
                );

                self.evaluate_early_profit_taking(
                    quote.timestamp_ms,
                );

                // Read the latest cached trading eligibility check for this symbol.
                // When disabled, all symbols are eligible for trading.
                let can_trade = if self.enable_trading_eligibility_check {
                    self.can_trade
                        .get(&ev.symbol)
                        .copied()
                        .unwrap_or(false)
                } else {
                    true
                };

                // ====================================================
                // ACTIVE STRATEGY TRADE CHECK
                // ====================================================

                // Check whether this strategy already has an active
                // trade for the current symbol.
                let has_active_trade = {
                    let view = self.rx_view.borrow();

                    view.trades.has_active_trade_for_strategy_symbol(
                        self.name(),
                        &ev.symbol,
                    )
                };

                // ====================================================
                // PRICE WINDOW TRACKING DURING ACTIVE TRADES
                // ====================================================

                // Clear the symbol's price history and stop tracking
                // while this strategy has an active trade, if disabled.
                if has_active_trade && !self.track_price_window_while_in_trade {

                    let cleared =
                        self.price_window.clear_symbol(&ev.symbol);

                    if cleared {
                        print_log(
                            self.debug_mode,
                            false,
                            format!(
                                "[RESEARCH V1 LIVE] PRICE WINDOW CLEARED | symbol={} | active_trade=true",
                                ev.symbol,
                            ),
                        );
                    }

                    return;
                }

                // Update per asset price tracking window.
                let result = self.price_window
                    .update_price_window_extreme_diffs_ts(
                        &ev.symbol,
                        quote.timestamp_ms,
                        mid,
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
                                true,
                                format_args!(
                                    "[RESEARCH V1 LIVE WINDOW][{}] \
                                    mid=${:.4} | \
                                    above_min={:.3}% | \
                                    below_max={:.3}% | \
                                    age={}s | \
                                    entry_threshold={:.3}%",
                                    ev.symbol,
                                    mid,
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
                        if can_trade 
                            && !self.black_swan_protection.is_halted() 
                            && !has_active_trade
                            && self.trade_cooldown.can_trade(
                                &ev.symbol,
                                quote.timestamp_ms,
                            )
                        {

                            // ====================================================
                            // SHORT ENTRY / TRENDING LONG ENTRY
                            // ====================================================

                            if pct_from_min >= entry_percent {

                                // Reverse trade direction when the market
                                // regime switches from MeanReversion to Trending.
                                let direction = match self.market_regime {
                                    MarketRegime::MeanReversion => Direction::Short,
                                    MarketRegime::Trending => Direction::Long,
                                };

                                let intent = TradeIntent {
                                    strategy_id: self.name().to_string(),
                                    symbol: ev.symbol.clone(),
                                    direction,
                                    signal_px: mid,
                                    target_notional: self.trade_amount_usd,
                                    margin_usd: self.trade_margin_usd(),
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
                                    ts_ms: quote.timestamp_ms,
                                };

                                // ====================================================
                                // ENTRY DEBUG
                                // ====================================================

                                if self.debug_mode {
                                    print_log(
                                        self.debug_mode,
                                        true,
                                        format_args!(
                                            "[RESEARCH V1 LIVE {:?} ENTRY][{}] \
                                            mid=${:.4} | \
                                            above_min={:.3}% >= {:.3}% | \
                                            amount=${:.2} | regime={:?}",
                                            direction,
                                            ev.symbol,
                                            mid,
                                            pct_from_min,
                                            entry_percent,
                                            self.trade_amount_usd,
                                            self.market_regime,
                                        ),
                                    );
                                }

                                // ====================================================
                                // SEND TRADE INTENT
                                // ====================================================

                                // let _ = self.tx_engine.send(
                                //     EngineEvent::OpenTrade(intent)
                                // );

                                // Record the trade submission cooldown.
                                self.trade_cooldown.record_trade(
                                    &ev.symbol,
                                    quote.timestamp_ms,
                                );
                            }

                            // ====================================================
                            // LONG ENTRY / TRENDING SHORT ENTRY
                            // ====================================================

                            else if pct_from_max >= entry_percent {

                                // Reverse trade direction when the market
                                // regime switches from MeanReversion to Trending.
                                let direction = match self.market_regime {
                                    MarketRegime::MeanReversion => Direction::Long,
                                    MarketRegime::Trending => Direction::Short,
                                };

                                let intent = TradeIntent {
                                    strategy_id: self.name().to_string(),
                                    symbol: ev.symbol.clone(),
                                    direction,
                                    signal_px: mid,
                                    target_notional: self.trade_amount_usd,
                                    margin_usd: self.trade_margin_usd(),
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
                                    ts_ms: quote.timestamp_ms,
                                };

                                // ====================================================
                                // ENTRY DEBUG
                                // ====================================================

                                if self.debug_mode {
                                    print_log(
                                        self.debug_mode,
                                        true,
                                        format_args!(
                                            "[RESEARCH V1 LIVE {:?} ENTRY][{}] \
                                            mid=${:.4} | \
                                            below_max={:.3}% >= {:.3}% | \
                                            amount=${:.2} | regime={:?}",
                                            direction,
                                            ev.symbol,
                                            mid,
                                            pct_from_max,
                                            entry_percent,
                                            self.trade_amount_usd,
                                            self.market_regime,
                                        ),
                                    );
                                }

                                // ====================================================
                                // SEND TRADE INTENT
                                // ====================================================

                                // let _ = self.tx_engine.send(
                                //     EngineEvent::OpenTrade(intent)
                                // );

                                // Record the trade submission cooldown.
                                self.trade_cooldown.record_trade(
                                    &ev.symbol,
                                    quote.timestamp_ms,
                                );
                            }
                        }
                    }
                }
            }

            AppEvent::BarClosed(bar) => {
                let timeframe = bar.timeframe;
                let current_ts = bar.end.timestamp_millis();

                // Store the completed live candle.
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
                            "[RESEARCH V1 LIVE CANDLE][{}][{}s] \
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




impl ResearchV1Strategy {

    // ============================================================
    // CALCULATE TRADE MARGIN
    // ============================================================

    /// Calculates strategy-allocated margin based on
    /// the current requested trade notional.
    ///
    /// Without leverage, margin equals trade amount.
    ///
    /// With leverage, margin equals trade amount / leverage.
    pub fn trade_margin_usd(&self) -> f64 {
        match self.leverage {
            Some(leverage) if leverage > 0.0 && leverage.is_finite() => {
                self.trade_amount_usd / leverage
            }

            _ => self.trade_amount_usd,
        }
    }
}




impl ResearchV1Strategy {

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

                // USD volume accumulated from completed live candles.
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
                            "[RESEARCH V1 LIVE 24H MARKET STATS][{}] \
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
                    "[RESEARCH V1 LIVE ELIGIBILITY][{}] \
                     can_trade={}",
                    symbol,
                    requirements_met,
                ),
            );
        }
    }
}




impl ResearchV1Strategy {

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




impl ResearchV1Strategy {

    // ============================================================
    // TRADE DURATION WATCHDOG
    // ============================================================

    /// Checks this strategy's open trades for overdue durations.
    ///
    /// Only trades belonging to this strategy are evaluated.
    ///
    /// Trades without configured durations are ignored.
    ///
    /// Closure requests are submitted through the engine.
    fn check_trade_duration_watchdog(
        &mut self,
        now_ts_ms: i64,
    ) {
        // ====================================================
        // CHECK INTERVAL
        // ====================================================

        if !self.trade_duration_watchdog.should_check(
            now_ts_ms,
        ) {
            return;
        }


        // ====================================================
        // FIND OVERDUE TRADES
        // ====================================================

        let overdue_trades = {
            let view = self.rx_view.borrow();

            view.trades
                .open_trades_for_strategy(self.name())
                .filter(|trade| {
                    self.trade_duration_watchdog.should_close(
                        trade.opened_ts_ms,
                        trade.trade_duration_secs,
                        now_ts_ms,
                    )
                })
                .map(|trade| trade.id)
                .collect::<Vec<_>>()
        };


        // ====================================================
        // REQUEST CLOSURE
        // ====================================================

        for trade_id in overdue_trades {
            let _ = self.tx_engine.send(
                EngineEvent::CloseTrade {
                    trade_id,
                    execution: ExitOrderRequest::market(),
                    reason: "Trade duration watchdog exceeded".to_string(),
                    ts_ms: now_ts_ms,
                }
            );
        }
    }
}




impl ResearchV1Strategy {

    // ============================================================
    // EVALUATE EARLY PROFIT TAKING
    // ============================================================

    /// Evaluates early-profit-taking rules for trades
    /// belonging exclusively to this strategy.
    ///
    /// Requests market exits when a rule triggers.
    /// Cooldown is managed internally by the toolbox.
    fn evaluate_early_profit_taking(&mut self, ts_ms: i64) {

        // ============================================================
        // CHECK COOLDOWN
        // ============================================================

        if !self.early_profit_taking.cooldown_ready(ts_ms) {
            return;
        }

        let view = self.rx_view.borrow();

        // ============================================================
        // BUILD STRATEGY BASKET
        // ============================================================

        let basket = self.early_profit_taking.basket_snapshot(
            self.name(),
            &view.trades,
            &view.last_px,
        );

        // ============================================================
        // REQUIRE FULLY OPEN BASKET
        // ============================================================

        let fully_open_count = view.trades
            .open_trades_for_strategy(self.name())
            .count();

        if fully_open_count != basket.open_trades {
            return;
        }

        // ============================================================
        // EVALUATE RULES
        // ============================================================

        let Some(rule) = self.early_profit_taking.check(
            &basket,
            ts_ms,
        ) else {
            return;
        };

        // ============================================================
        // CLOSE STRATEGY TRADES
        // ============================================================

        let mut submitted = false;

        for trade in view.trades.open_trades_for_strategy(
            self.name(),
        ) {
            if trade.state != TradeState::Open {
                continue;
            }

            let event = EngineEvent::CloseTrade {
                trade_id: trade.id.clone(),
                execution: ExitOrderRequest::market(),
                reason: format!(
                    "Early profit taking: {} ({:.2}%)",
                    rule.name,
                    basket.net_profit_percent,
                ),
                ts_ms,
            };

            match self.tx_engine.send(event) {
                Ok(()) => submitted = true,

                Err(err) => {
                    eprintln!(
                        "[EarlyProfitTaking] Failed to send close event: {}",
                        err
                    );
                }
            }
        }

        // ============================================================
        // UPDATE COOLDOWN
        // ============================================================

        drop(view);

        if submitted {
            self.early_profit_taking.record_exit_request(ts_ms);
        }
    }
}




