#![allow(dead_code)]

use std::{
    collections::{
        HashMap,
        VecDeque,
    },
    sync::Arc,
};

use chrono::{
    DateTime,
    Utc,
};

use tokio::sync::{
    mpsc::UnboundedSender,
    watch,
};

use crate::core::{
    engine::engine::EngineEvent,
    engine_view::EngineView,
    events::{
        AppEvent,
        AppEventEnvelope,
    },
};

use crate::market::{
    candle::CandleConfig,
    candle_history::CandleHistory,
};

use crate::strategies::Strategy;


// ============================================================
// MEAN REVERSION BACKTEST STRATEGY
// ============================================================

pub struct MeanReversionBacktestStrategy {
    tx_engine: UnboundedSender<EngineEvent>,
    rx_view: watch::Receiver<Arc<EngineView>>,

    pub enable_strategy: bool,
    pub debug_mode: bool,

    // Total historical TapeTrade events processed.
    pub total_trade_events: u64,

    // Rolling historical price window for each symbol.
    //
    // Uses historical DateTime<Utc> so the window advances
    // according to historical market time.
    pub window_prices:
        HashMap<String, VecDeque<(DateTime<Utc>, f64)>>,

    // How much historical time remains in the rolling window.
    pub entry_window_secs: i64,

    // Entry threshold used by this backtest strategy.
    pub entry_percent: f64,

    // Stores closed multi-timeframe candles.
    pub candle_history: CandleHistory,
}


// ============================================================
// CONSTRUCTOR
// ============================================================

impl MeanReversionBacktestStrategy {
    // Creates the historical MeanReversion strategy and initializes
    // its rolling price window and candle history.
    pub fn new(
        tx_engine: UnboundedSender<EngineEvent>,
        rx_view: watch::Receiver<Arc<EngineView>>,
    ) -> Self {
        // Uses the default candle history prune duration.
        let candle_config =
            CandleConfig::default();

        Self {
            tx_engine,
            rx_view,

            // Enabled so we can verify that this strategy receives
            // the full backtest event stream.
            enable_strategy: false,

            debug_mode: true,

            total_trade_events: 0,

            window_prices:
                HashMap::new(),

            entry_window_secs:
                10_800,

            entry_percent:
                0.3,

            candle_history:
                CandleHistory::new(
                    candle_config
                        .history_retention
                ),
        }
    }
}


// ============================================================
// FACTORY
// ============================================================

pub fn make_mean_reversion_backtest(
    tx_engine: UnboundedSender<EngineEvent>,
    rx_view: watch::Receiver<Arc<EngineView>>,
) -> Box<dyn Strategy> {
    Box::new(
        MeanReversionBacktestStrategy::new(
            tx_engine,
            rx_view,
        )
    )
}


// ============================================================
// STRATEGY IMPLEMENTATION
// ============================================================

#[async_trait::async_trait]
impl Strategy for MeanReversionBacktestStrategy {
    fn name(&self) -> &'static str {
        "MeanReversionBacktestStrategy"
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
                self.total_trade_events += 1;


                // ====================================================
                // HISTORICAL EVENT TIME
                // ====================================================

                // TapeTrade now stores canonical Unix milliseconds.
                //
                // Convert timestamp_ms back into DateTime<Utc> because
                // this strategy's rolling historical window currently
                // operates on DateTime<Utc>.
                let Some(timestamp): Option<DateTime<Utc>> =
                    DateTime::from_timestamp_millis(
                        trade.timestamp_ms
                    )
                else {
                    eprintln!(
                        "[MEAN REVERSION BACKTEST][{}] invalid timestamp_ms={}",
                        ev.symbol,
                        trade.timestamp_ms,
                    );

                    return;
                };


                // ====================================================
                // ROLLING HISTORICAL PRICE WINDOW
                // ====================================================

                // Feed the historical price and timestamp into
                // the rolling historical price-window calculation.
                if let Some((
                    pct_from_min,
                    pct_from_max,
                    elapsed_secs,
                )) =
                    self
                        .update_and_get_backtest_window_extreme_diffs(
                            &ev.symbol,
                            timestamp,
                            trade.price,
                        )
                {
                    if self.debug_mode {
                        println!(
                            "[MEAN REVERSION BACKTEST WINDOW][{}] \
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
                            self.entry_percent,
                        );
                    }


                    // Makes it easy to verify that this strategy is
                    // receiving and processing the same trade stream.
                    if pct_from_min >= self.entry_percent {
                        println!(
                            "[MEAN REVERSION BACKTEST ENTRY THRESHOLD][{}] \
                             price=${:.4} | \
                             above_min={:.3}% >= {:.3}%",
                            ev.symbol,
                            trade.price,
                            pct_from_min,
                            self.entry_percent,
                        );
                    }
                }


                // ====================================================
                // TRADE LOG
                // ====================================================

                if self.debug_mode {
                    println!(
                        "[MEAN REVERSION BACKTEST TRADE #{}][{}] \
                         price=${:.4} | size={} | timestamp_ms={}",
                        self.total_trade_events,
                        ev.symbol,
                        trade.price,
                        trade.size,
                        trade.timestamp_ms,
                    );
                }
            }


            // ====================================================
            // HISTORICAL QUOTE
            // ====================================================

            AppEvent::Quote(quote) => {
                if self.debug_mode {
                    println!(
                        "[MEAN REVERSION BACKTEST QUOTE][{}] \
                         bid=${:.4} | ask=${:.4}",
                        ev.symbol,
                        quote.bid_price,
                        quote.ask_price,
                    );
                }
            }


            // ====================================================
            // HISTORICAL CANDLE
            // ====================================================

            AppEvent::BarClosed(bar) => {
                // Candle history may not contain enough data yet.
                // Always check for None / required length before
                // using candles in actual trading decisions.

                if self.debug_mode {
                    println!(
                        "[MEAN REVERSION BACKTEST BAR CLOSED][{}][{}s] \
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
                        "[MEAN REVERSION BACKTEST CANDLES][{}][{}s] stored={}",
                        ev.symbol,
                        timeframe.as_secs(),
                        self.candle_history.len(
                            &ev.symbol,
                            timeframe,
                        ),
                    );
                }
            }
        }
    }
}