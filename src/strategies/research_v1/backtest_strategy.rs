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

use super::backtest_state::ResearchV1BacktestStrategy;


/* 
// TODO:

* Make a function to get all symbols inside the backtest data and use it for the symbols array to pass it in.


* Test on 3 months of real data to see its performance.
  - Test on a month of volatile trading lke when BTC when from 60k -> 80k, and when you get good results,
    test on a month of non-volatile trading like when BTC was sideways at 60k for a while.
    Then you will be able to decided what works better during which market movement behavior.   
  - Check your AWS bill first to make sure your currect data is not too expensive.
  - Create an engine helper function that grabs all the symbols used inside the backtest data to pass them through the backtest run.
    This way you are able to trade every assest inside the backtest run just like you would in real life.
  - Then get another 3 months for a total of 6 months. 
  - Try to get another different 6 months of older data during a bear market so we can backtest bull and bear markets and see if the bot performs
    we in both markets. 


---------------------------------------

* When you have good initial results, add the weighted randomness to the bot
  for more dynamic behavior accross all settings. 
  - The dynamic randomness can be a mode you turn off and on. 

* Add a toolbox feature to update leverage continously for all assets as a pollers so leverage stays up to date.
  - Have a loop mean you dont have to update leverage again when a new assets qualifies for trading.    
  - Add a helper api function that does this on bot start.
  - The toolbox feature should show a boolean when ready to trade ie the asset already had its leverage updated.
    Put this toolbox feature inside /toolbox/hyperliquid for organization.

* Later you can detect slow markets though one of you 3 market wide indicators and try to dynamically
    make the trade durations shorter. You will need to play around with with. Still only keep the 2 Mean Rev and Tredning regimes
    handling this on the side.

* Add an auto compounding toolbox that takes in the trade_amount_usd etc and performs
  auto compounding as the "real" avenue ccount account is growing. See old bot for logic details.
  - You should be able to pick the compounding interval (1 day, 3 days etc).

* You should try to add remote message for the live version for certain important events inside the strategy.
  This can wait until you actually want to go live though.

* Add the engine database save trade records in collections. 
  - Consider having a Live, Paper, Simulation collection.
  - Also add the option to save the backtest output when everything if fully done.

  (Important!!)
* For the live production version, trade_margin_usd() function below must take into account the assets max_leverage and use that instead.

---------------------------------------

* Consider adding another price window for BTC only and not trading when BTC is making sharp moves.
  - The point is to try to prevent market vacuum. Try other things also etc. 

* Make early profit toolbox only check every 2 seconds for efficiency rather than every tick.

*/




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




impl ResearchV1BacktestStrategy {
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
            trade_amount_usd: 200.0, // maybe delete later in favor of (reference_margin_usd / max_trades_allowed) which devides trades evenly. Double check since it must work with leverage / early profit margit calculations.

            leverage: None,
            entry_percent: Some(0.005),
            exit_profit_percent: Some(2.0),
            exit_loss_percent: Some(2.0),
            emergency_exit_percent: Some(5.0),
            min_sl_delay: None, // 1800 = 30 min
            trade_duration_secs: None, // 600 = 10 minutes, 3600 = 1hr
            min_volume_usd: Some(150_000.0), 
            max_24h_move_pct: Some(15.00),

            market_regime: MarketRegime::MeanReversion,

            track_price_window_while_in_trade: false,
            price_window: PriceWindowTs::new(
                1800, // 30 min rolling window
                false,
            ),

            candle_history: CandleHistory::new(
                candle_config.history_retention,
            ),

            // false = bypass can_trade check, 
            // true = enforce can_trade check.
            enable_trading_eligibility_check: false, 
            can_trade: HashMap::new(),

            black_swan_protection: BlackSwanProtection::new(
                true,   // Enable protection.
                false,  // Enable or disable debug logging.
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
                        name: "Small Basket Early Profit".to_string(),
                        enabled: true,
                        min_open_trades: 2,
                        max_open_trades: Some(4),
                        min_margin_usd: 0.0,
                        min_profit_percent: 2.5, // Margin return
                        // min_profit_percent: 2.5 * 5.0, // Underlying movement (5x)
                        no_new_trade_for_ms: None,
                    },

                    // ====================================================
                    // LARGE BASKET EARLY PROFIT (5–8 TRADES)
                    // ====================================================

                    EarlyProfitRule {
                        name: "Large Basket Early Profit".to_string(),
                        enabled: true,
                        min_open_trades: 5,
                        max_open_trades: None,
                        min_margin_usd: 0.0,
                        min_profit_percent: 2.0, // Margin return
                        // min_profit_percent: 2.0 * 5.0, // Underlying movement (5x)
                        no_new_trade_for_ms: None,
                    },
                ],
                2000,
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

pub fn make_research_v1_backtest(
    tx_engine: UnboundedSender<EngineEvent>,
    rx_view: watch::Receiver<Arc<EngineView>>,
) -> Box<dyn Strategy> {
    Box::new(
        ResearchV1BacktestStrategy::new(
            tx_engine,
            rx_view,
        )
    )
}


// ============================================================
// STRATEGY IMPLEMENTATION
// ============================================================

#[async_trait::async_trait]
impl Strategy for ResearchV1BacktestStrategy {
    fn name(&self) -> &'static str {
        "ResearchV1BacktestStrategy"
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
                            "[RESEARCH V1 BACKTEST TRADE][{}] \
                            price=${:.4} | size={} | timestamp_ms={}",
                            ev.symbol,
                            trade.price,
                            trade.size,
                            trade.timestamp_ms,
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
                        trade.timestamp_ms,
                        &mut self.market_regime,
                    );
                }

                self.check_trade_duration_watchdog(
                    trade.timestamp_ms,
                );

                self.evaluate_early_profit_taking(
                    trade.timestamp_ms,
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

                // Check whether this strategy already has an active
                // trade for the current symbol.
                let symbol_has_active_trade = {
                    let view = self.rx_view.borrow();

                    view.trades.has_active_trade_for_strategy_symbol(
                        self.name(),
                        &ev.symbol,
                    )
                };

                // Count active trades belonging to this strategy.
                // Includes Opening, Open, and Closing trades.
                let active_trade_count = {
                    let view = self.rx_view.borrow();

                    view.trades.count_active_for_strategy(
                        self.name(),
                    )
                };

                // Clear the symbol's price history and stop tracking
                // while this strategy has an active trade, if disabled.
                if symbol_has_active_trade && !self.track_price_window_while_in_trade {

                    let cleared =
                        self.price_window.clear_symbol(&ev.symbol);

                    if cleared {
                        print_log(
                            self.debug_mode,
                            false,
                            format!(
                                "[RESEARCH V1 BACKTEST] PRICE WINDOW CLEARED | symbol={} | active_trade=true",
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
                                    "[RESEARCH V1 BACKTEST WINDOW][{}] \
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

                        if can_trade 
                            && active_trade_count < self.max_trades_allowed
                            && !symbol_has_active_trade
                            && !self.black_swan_protection.is_halted() 
                            && self.trade_cooldown.can_trade(
                                &ev.symbol,
                                trade.timestamp_ms,
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
                                    signal_px: trade.price,
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
                                    // trailing_stop: Some(TrailingStopConfig {
                                    //     levels: vec![
                                    //         // super tight for testing (slow)
                                    //         // TrailingStopLevel {
                                    //         //     profit_percent: 0.0,   // Initial activation threshold.
                                    //         //     trail_percent: 0.010,  // Initial trailing distance.
                                    //         // },
                                    //         // TrailingStopLevel {
                                    //         //     profit_percent: 0.005, // Second activation threshold.
                                    //         //     trail_percent: 0.005,  // Second trailing distance.
                                    //         // },
                                    //         // TrailingStopLevel {
                                    //         //     profit_percent: 0.010, // Third activation threshold.
                                    //         //     trail_percent: 0.002,  // Third trailing distance.
                                    //         // },
                                        
                                    //         // more realistic (fast)
                                    //         TrailingStopLevel {
                                    //             profit_percent: 0.0,  // Initial activation threshold.
                                    //             trail_percent: 3.00,  // Initial trailing distance.
                                    //         },
                                    //         TrailingStopLevel {
                                    //             profit_percent: 0.50, // Second activation threshold.
                                    //             trail_percent: 2.00,  // Second trailing distance.
                                    //         },
                                    //         TrailingStopLevel {
                                    //             profit_percent: 1.00, // Third activation threshold.
                                    //             trail_percent: 1.00,  // Third trailing distance.
                                    //         },
                                    //     ],
                                    // }),
                                    ts_ms: trade.timestamp_ms,
                                };

                                // ====================================================
                                // ENTRY DEBUG
                                // ====================================================

                                if self.debug_mode {
                                    print_log(
                                        self.debug_mode,
                                        false,
                                        format_args!(
                                            "[RESEARCH V1 BACKTEST {:?} ENTRY][{}] \
                                            price=${:.4} | \
                                            above_min={:.3}% >= {:.3}% | \
                                            amount=${:.2} | regime={:?}",
                                            direction,
                                            ev.symbol,
                                            trade.price,
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

                                let _ = self.tx_engine.send(
                                    EngineEvent::OpenTrade(intent)
                                );

                                // Record the trade submission cooldown.
                                self.trade_cooldown.record_trade(
                                    &ev.symbol,
                                    trade.timestamp_ms,
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
                                    signal_px: trade.price,
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
                                    // trailing_stop: Some(TrailingStopConfig {
                                    //     levels: vec![
                                    //         // super tight for testing (slow)
                                    //         // TrailingStopLevel {
                                    //         //     profit_percent: 0.0,   // Initial activation threshold.
                                    //         //     trail_percent: 0.010,  // Initial trailing distance.
                                    //         // },
                                    //         // TrailingStopLevel {
                                    //         //     profit_percent: 0.005, // Second activation threshold.
                                    //         //     trail_percent: 0.005,  // Second trailing distance.
                                    //         // },
                                    //         // TrailingStopLevel {
                                    //         //     profit_percent: 0.010, // Third activation threshold.
                                    //         //     trail_percent: 0.002,  // Third trailing distance.
                                    //         // },
                                        
                                    //         // more realistic (fast)
                                    //         TrailingStopLevel {
                                    //             profit_percent: 0.0,  // Initial activation threshold.
                                    //             trail_percent: 3.00,  // Initial trailing distance.
                                    //         },
                                    //         TrailingStopLevel {
                                    //             profit_percent: 0.50, // Second activation threshold.
                                    //             trail_percent: 2.00,  // Second trailing distance.
                                    //         },
                                    //         TrailingStopLevel {
                                    //             profit_percent: 1.00, // Third activation threshold.
                                    //             trail_percent: 1.00,  // Third trailing distance.
                                    //         },
                                    //     ],
                                    // }),
                                    ts_ms: trade.timestamp_ms,
                                };

                                // ====================================================
                                // ENTRY DEBUG
                                // ====================================================

                                if self.debug_mode {
                                    print_log(
                                        self.debug_mode,
                                        false,
                                        format_args!(
                                            "[RESEARCH V1 BACKTEST {:?} ENTRY][{}] \
                                            price=${:.4} | \
                                            below_max={:.3}% >= {:.3}% | \
                                            amount=${:.2} | regime={:?}",
                                            direction,
                                            ev.symbol,
                                            trade.price,
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

                                let _ = self.tx_engine.send(
                                    EngineEvent::OpenTrade(intent)
                                );

                                // Record the trade submission cooldown.
                                self.trade_cooldown.record_trade(
                                    &ev.symbol,
                                    trade.timestamp_ms,
                                );
                            }
                        }
                    }
                }
            }

            AppEvent::Quote(quote) => {
                if self.debug_mode {
                    print_log(
                        self.debug_mode,
                        false,
                        format!(
                            "[RESEARCH V1 BACKTEST QUOTE][{}] \
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
                        false,
                        format_args!(
                            "[RESEARCH V1 BACKTEST CANDLE][{}][{}s] \
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




impl ResearchV1BacktestStrategy {

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




impl ResearchV1BacktestStrategy {

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
                            "[RESEARCH V1 BACKTEST 24H MARKET STATS][{}] \
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
                    "[RESEARCH V1 BACKTEST ELIGIBILITY][{}] \
                     can_trade={}",
                    symbol,
                    requirements_met,
                ),
            );
        }
    }
}




impl ResearchV1BacktestStrategy {

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




impl ResearchV1BacktestStrategy {

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




impl ResearchV1BacktestStrategy {

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




