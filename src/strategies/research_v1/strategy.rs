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

use super::state::ResearchV1Strategy;


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

            // ====================================================
            // LIVE TRADE
            // ====================================================

            AppEvent::TapeTrade(trade) => {
                // ====================================================
                // TRADE DEBUG
                // ====================================================

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


            // ====================================================
            // LIVE QUOTE
            // ====================================================

            AppEvent::Quote(quote) => {

                let mid = (quote.bid_price + quote.ask_price) / 2.0;

                // ============================================================
                // TRADING ELIGIBILITY
                // ============================================================

                // Read the latest cached trading eligibility for this symbol.
                let can_trade = self.can_trade
                    .get(&ev.symbol)
                    .copied()
                    .unwrap_or(false);

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
                // TIMESTAMP-BASED PRICE WINDOW
                // ====================================================

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
                        // ENTRY SIGNAL
                        // ====================================================

                        // Research-only signal detection.
                        // No orders are submitted yet.
                        if can_trade && pct_from_min >= entry_percent {
                            if self.debug_mode {
                                print_log(
                                    self.debug_mode,
                                    true,
                                    format_args!(
                                        "[RESEARCH V1 LIVE SIGNAL][{}] \
                                        mid=${:.4} | \
                                        above_min={:.3}% >= {:.3}%",
                                        ev.symbol,
                                        mid,
                                        pct_from_min,
                                        entry_percent,
                                    ),
                                );
                            }
                        }
                    }
                }
            }


            // ============================================================
            // LIVE CANDLE
            // ============================================================

            AppEvent::BarClosed(bar) => {
                let timeframe = bar.timeframe;

                // Current completed candle timestamp in Unix milliseconds.
                let current_ts = bar.end.timestamp_millis();

                // Store the completed candle.
                self.candle_history.push(bar);

                // ============================================================
                // 24-HOUR MARKET STATISTICS
                // ============================================================

                // Recalculate trading eligibility only when a new
                // completed one-minute candle becomes available.
                if timeframe == Timeframe::ONE_MINUTE {

                    // Number of one-minute candles in 24 hours.
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

                        // Require 1440 completed one-minute candles.
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

                            // Exact USD volume from raw tape trade accumulation.
                            //
                            // Internally uses valid_candle_window().
                            let volume_usd_24h = volume_usd(
                                &bars_vec,
                                period,
                                current_ts,
                            );

                            // Percentage price movement over the same
                            // validated 24-hour candle window.
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

                            // Some(max_pct) = Require the absolute 24-hour
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
                                && price_change_24h.is_some();

                            // ============================================================
                            // MARKET STATISTICS DEBUG
                            // ============================================================

                            if self.debug_mode {
                                print_log(
                                    self.debug_mode,
                                    true,
                                    format_args!(
                                        "[RESEARCH V1 24H MARKET STATS][{}] \
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

                    // Store the latest trading eligibility for this symbol.
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
                                "[RESEARCH V1 ELIGIBILITY][{}] \
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
                            "[RESEARCH V1 CANDLE][{}][{}s] \
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