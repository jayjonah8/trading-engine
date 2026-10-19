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

    shared::print_log,
};

use super::backtest_state::ResearchV1BacktestStrategy;

// TODO:
// * Set up the can_trade requirements for volume. Should auto update as volume changes.

// * Set up the if pct_from_min >= self.entry_percent {} code blocks for the basic mean
//   setup where you actually send the trade intents.

// * Add a stop trading state when all assets have an average 24h percent move (ie >= 10%) over a certain amount.
//   You may be able to add this as a helper somewhere idk. 

// * Add the logic on when a trade is flipped (trending). Should be simple.

// * Figure out the best way to map a strategy trade identifier to a trade id. See Notion "Current" notes. 

// * Add an early profit tax toolbox feature.
//   - Can have normal profit taking.
//   - Can also have slow version profit taking.
//   - See old bot for implementation details.

// * Start implementing the continuous multi-sync logic as a toolbox feature.

// * Test on 3 months of real data to see its performance.

// * When you have good initial results, add the weighted randomness to the bot
//   for more dynamic behavior accross all settings. 

// * For track_price_window_while_in_trade add helper function
//   inside the toolbool feature that clears the assets window.
//   Do this for both versions of the price window feature.
//   - Also add another helper function to both that clear all
//     assets price windows.

// * Add the runner assets detection logic to catch symbol with large movement during a day.
//   - You can use the r2 and 24hr percent change to see the smoothness and likely trade in whatever 
//     trending direction the asset is moving towards. Can add only a light weight dynamic logic to this also if you want. 

// * Add a toolbox feature to update leverage continously for all assets as a pollers so leverage stays up to date.
//   - Have a loop mean you dont have to update leverage again when a new assets qualifies for trading.    
//   - Add a helper api function that does this on bot start.

// * Add an auto compounding tool box that takes in the trade_amount_usd etc and performs
//   auto compounding as the "real" avenue ccount account is growing. See old bot for logic details.
//   - You should be able to pick the compounding interval (1 day, 3 days etc).


// ============================================================
// CONSTRUCTOR
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

            // ====================================================
            // HISTORICAL TRADE
            // ====================================================

            AppEvent::TapeTrade(trade) => {

                // ============================================================
                // TRADING ELIGIBILITY
                // ============================================================

                // Read the latest cached trading eligibility for this symbol.
                let can_trade = self.can_trade
                    .get(&ev.symbol)
                    .copied()
                    .unwrap_or(false);
         
                // ====================================================
                // TRADE DEBUG
                // ====================================================

                if self.debug_mode {
                    print_log(
                        self.debug_mode,
                        false,
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
                // HISTORICAL PRICE WINDOW
                // ====================================================

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
                        // ENTRY SIGNAL
                        // ====================================================

                        // Research-only signal detection.
                        // No orders are submitted yet.
                        if can_trade && pct_from_min >= entry_percent {
                            if self.debug_mode {
                                print_log(
                                    self.debug_mode,
                                    false,
                                    format_args!(
                                        "[RESEARCH V1 BACKTEST SIGNAL][{}] \
                                        price=${:.4} | \
                                        above_min={:.3}% >= {:.3}%",
                                        ev.symbol,
                                        trade.price,
                                        pct_from_min,
                                        entry_percent,
                                    ),
                                );
                            }
                        }
                    }
                }
            }


            // ====================================================
            // HISTORICAL QUOTE
            // ====================================================

            AppEvent::Quote(quote) => {
                if self.debug_mode {
                    print_log(
                        self.debug_mode,
                        true,
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


            // ====================================================
            // HISTORICAL CANDLE
            // ====================================================

            AppEvent::BarClosed(bar) => {
                let timeframe = bar.timeframe;

                // Historical completed candle timestamp in Unix milliseconds.
                let current_ts = bar.end.timestamp_millis();

                // Store the completed historical candle.
                self.candle_history.push(bar);

                // ============================================================
                // 24-HOUR MARKET STATISTICS
                // ============================================================

                // Recalculate trading eligibility only when a new
                // completed one-minute historical candle becomes available.
                if timeframe == Timeframe::ONE_MINUTE {

                    // Number of one-minute candles in 24 hours (1440).
                    let period: usize = 1440;

                    // Default to false until all requirements pass.
                    let mut requirements_met = false;

                    // ============================================================
                    // GET ONE-MINUTE CANDLE HISTORY
                    // ============================================================

                    if let Some(bars) = self.candle_history.get(
                        &ev.symbol,
                        Timeframe::ONE_MINUTE,
                    ) {

                        // Require 1440 completed historical one-minute candles.
                        if bars.len() >= period {

                            // Collect only the most recent 1440 candles.
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

                            // Exact historical USD volume accumulated
                            // from tape trades within completed candles.
                            //
                            // Internally uses valid_candle_window().
                            let volume_usd_24h = volume_usd(
                                &bars_vec,
                                period,
                                current_ts,
                            );

                            // Historical percentage price movement over
                            // the same validated 24-hour candle window.
                            //
                            // Internally uses valid_candle_window().
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

                            // Some(max_pct) = Require absolute historical
                            // price movement to remain within the limit.
                            //
                            // None = Disable the movement restriction.
                            let within_24h_move_limit =
                                match self.max_24h_move_pct {
                                    Some(max_pct) => {
                                        price_change_24h.is_some_and(
                                            |change_pct| {
                                                change_pct.abs() <= max_pct
                                            }
                                        )
                                    }

                                    None => true,
                                };

                            // ============================================================
                            // TRADING REQUIREMENTS
                            // ============================================================

                            // Both indicators must return valid results.
                            //
                            // If either indicator returns None, the asset
                            // is not eligible for trading.
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
                                    true,
                                    format_args!(
                                        "[RESEARCH V1 BACKTEST 24H MARKET STATS][{}] \
                                        CHANGE={:?}% | \
                                        USD_VOLUME={:?} | \
                                        ENOUGH_VOLUME={} | \
                                        WITHIN_MOVE_LIMIT={} | \
                                        CAN_TRADE={}",
                                        ev.symbol,
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

                    // Store the latest historical trading eligibility.
                    //
                    // Each asset maintains its own independent Boolean.
                    self.can_trade.insert(
                        ev.symbol.clone(),
                        requirements_met,
                    );

                    // ============================================================
                    // ELIGIBILITY DEBUG
                    // ============================================================

                    if self.debug_mode {
                        print_log(
                            self.debug_mode,
                            true,
                            format_args!(
                                "[RESEARCH V1 BACKTEST ELIGIBILITY][{}] \
                                can_trade={}",
                                ev.symbol,
                                requirements_met,
                            ),
                        );
                    }
                }

                // ============================================================
                // CANDLE DEBUG
                // ============================================================

                if self.debug_mode {
                    print_log(
                        self.debug_mode,
                        true,
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