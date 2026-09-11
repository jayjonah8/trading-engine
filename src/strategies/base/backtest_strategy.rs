#![allow(dead_code)]

use std::{
    collections::{
        HashMap,
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
    engine::EngineEvent,
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

use crate::strategies::base::strategy_session::{
    strategy_trading_window,
};

use crate::market::session::regular_market_session;

use crate::strategies::base::backtest_state::BaseBacktestStrategy;
use crate::strategies::Strategy;
use crate::core::trade_intent::TradeIntent;
use crate::shared::print_log;

impl BaseBacktestStrategy {
    // Creates the historical version of BaseStrategy and initializes
    // the state needed to evaluate rolling historical price windows.
    pub fn new(
        tx_engine: UnboundedSender<EngineEvent>,
        rx_view: watch::Receiver<Arc<EngineView>>,
    ) -> Self {

        // Uses the default candle setting timeframes and prune duration. 
        // You can change this to a custom config if you want. 
        let candle_config = CandleConfig::default();

        Self {
            tx_engine,
            rx_view,
            enable_strategy: true,
            debug_mode: true,
            total_trade_events: 0,
            window_prices: HashMap::new(),
            entry_window_secs: 10_800,
            trade_amount_usd: 100.0,
            entry_percent: 0.05,
            exit_profit_percent: 1.00,
            exit_loss_percent: 1.05,
            emergency_exit_percent: 3.0,
            min_sl_delay: 7200,
            enable_trade_duration: true,
            trade_duration_secs: Some(14400),
            track_price_window_while_in_trade: false,
            candle_history: CandleHistory::new(
                candle_config.history_retention
            ),
            market_open_delay_minutes: 15,
            market_close_buffer_minutes: 15,
            close_all_before_market_close_minutes: 10,
            close_all_sent_for_session: None,
            track_prices_during_open_delay: false,
            current_session_start_by_symbol:
                HashMap::new(),
        }
    }
}

pub fn make_base_backtest(
    tx_engine: UnboundedSender<EngineEvent>,
    rx_view: watch::Receiver<Arc<EngineView>>,
) -> Box<dyn Strategy> {
    Box::new(
        BaseBacktestStrategy::new(
            tx_engine,
            rx_view,
        )
    )
}

#[async_trait::async_trait]
impl Strategy for BaseBacktestStrategy {
    fn name(&self) -> &'static str {
        "BaseBacktestStrategy"
    }

    async fn on_event(
        &mut self,
        ev: AppEventEnvelope,
    ) {
        if !self.enable_strategy {
            return;
        }

        match ev.event {
            AppEvent::Trade(trade) => {
            self.total_trade_events += 1;

            // Backtest entry flow:
            // Regular-hours filter
            // → Reset state for a new market session
            // → Calculate the strategy trading window
            // → Optionally track prices during the opening delay
            // → Optionally stop tracking prices while already in a trade
            // → Update the rolling price window
            // → Check whether entries are currently allowed
            // → Check the latest EngineView state
            // → Send a short or long trade intent

            // Convert the historical timestamp stored in the normal
            // Trade event back into DateTime<Utc>.
            let timestamp =
                match DateTime::parse_from_rfc3339(
                    &trade.timestamp
                ) {
                    Ok(timestamp) => {
                        timestamp.with_timezone(&Utc)
                    }

                    Err(err) => {
                        eprintln!(
                            "[BASE BACKTEST][{}] invalid timestamp {}: {}",
                            ev.symbol,
                            trade.timestamp,
                            err,
                        );

                        return;
                    }
                };


            // ============================================================
            // REGULAR MARKET SESSION
            // ============================================================

            let Some((
                session_start,
                session_end,
            )) = regular_market_session(
                timestamp
            ) else {
                return;
            };

            // Ignore all premarket and after-hours trades.
            if timestamp < session_start
                || timestamp >= session_end
            {
                return;
            }


            // ============================================================
            // NEW SESSION RESET
            // ============================================================

            let previous_session =
                self.current_session_start_by_symbol
                    .get(&ev.symbol)
                    .copied();

            if previous_session
                != Some(session_start)
            {
                // Remove all rolling prices belonging to the
                // previous trading session for this symbol.
                self.window_prices
                    .remove(&ev.symbol);

                self.current_session_start_by_symbol
                    .insert(
                        ev.symbol.clone(),
                        session_start,
                    );

                print_log(
                    self.debug_mode,
                    false,
                    format!(
                        "[BASE BACKTEST SESSION RESET][{}] session_start={}",
                        ev.symbol,
                        session_start,
                    ),
                );
            }


            // ============================================================
            // STRATEGY TRADING WINDOW
            // ============================================================

            let Some((
                trading_start,
                trading_end,
            )) = strategy_trading_window(
                timestamp,
                self.market_open_delay_minutes,
                self.market_close_buffer_minutes,
            ) else {
                return;
            };


            // ============================================================
            // END-OF-SESSION FORCE CLOSE
            // ============================================================

            // This is separate from trading_end.
            //
            // market_close_buffer_minutes:
            //     Stops NEW entries before market close.
            //
            // close_all_before_market_close_minutes:
            //     Force-closes any trades that are still open.
            //
            // Example:
            //
            //     Market close = 4:00 PM
            //     market_close_buffer_minutes = 15
            //         → Stop new entries at 3:45 PM
            //
            //     close_all_before_market_close_minutes = 10
            //         → Force-close remaining trades at 3:50 PM

            let close_all_time =
                session_end
                    - chrono::Duration::minutes(
                        self.close_all_before_market_close_minutes
                    );


            // Send the close-all request only once for this market session.
            if timestamp >= close_all_time {
                if self.close_all_sent_for_session
                    != Some(session_start)
                {
                    self.close_all_sent_for_session =
                        Some(session_start);

                    print_log(
                        self.debug_mode,
                        true,
                        format!(
                            "[BASE BACKTEST FORCE CLOSE ALL] \
                            now={} | \
                            close_all_time={} | \
                            session_end={} | \
                            close_before_market_close={}min",
                            timestamp,
                            close_all_time,
                            session_end,
                            self.close_all_before_market_close_minutes,
                        ),
                    );

                    let _ =
                        self.tx_engine.send(
                            EngineEvent::CloseAllTradesBacktest {
                                reason: format!(
                                    "Forced close {} minutes before market close",
                                    self.close_all_before_market_close_minutes,
                                ),
                            }
                        );
                }

                // Once the force-close time has been reached,
                // this strategy should do nothing else for the session.
                return;
            }


            // ============================================================
            // OPENING DELAY PRICE TRACKING
            // ============================================================

            // When disabled, completely ignore prices during the
            // opening delay instead of adding them to the rolling window.
            if !self.track_prices_during_open_delay
                && timestamp < trading_start
            {
                return;
            }


            // ============================================================
            // ENGINE VIEW
            // ============================================================

            let view: Arc<EngineView> =
                self.rx_view.borrow().clone();


            // ============================================================
            // OPEN TRADE PRICE-WINDOW TRACKING
            // ============================================================

            // When disabled, stop tracking this symbol's rolling price
            // window while it already has an open backtest trade.
            if !self.track_price_window_while_in_trade
                && view.trades
                    .has_open_trade_for_symbol(&ev.symbol)
            {
                if self.window_prices
                    .remove(&ev.symbol)
                    .is_some()
                {
                    print_log(
                        self.debug_mode,
                        false,
                        format!(
                            "[BASE BACKTEST WINDOW CLEARED][{}] \
                            open trade detected; price window cleared",
                            ev.symbol,
                        ),
                    );
                }

                return;
            }


            // ============================================================
            // ROLLING PRICE WINDOW
            // ============================================================

            if let Some((
                pct_from_min,
                pct_from_max,
                elapsed_secs,
            )) = self
                .update_and_get_backtest_window_extreme_diffs(
                    &ev.symbol,
                    timestamp,
                    trade.price,
                )
            {
                print_log(
                    self.debug_mode,
                    false,
                    format!(
                        "[BASE BACKTEST WINDOW][{}] \
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
                    ),
                );


                // ============================================================
                // ENTRY TIME GATE
                // ============================================================

                // Prices may be collected during the opening delay,
                // but entries cannot be sent until trading_start.
                if timestamp < trading_start {
                    return;
                }

                // Stop generating new entries before the market closes.
                if timestamp >= trading_end {
                    return;
                }


                // ============================================================
                // EXISTING OPEN TRADE GATE
                // ============================================================

                // Do not generate another trade intent when this symbol
                // already has an open position in the engine.
                if view.trades
                    .has_open_trade_for_symbol(
                        &ev.symbol
                    )
                {
                    return;
                }


                print_log(
                    self.debug_mode,
                    false,
                    format!(
                        "[BASE BACKTEST TRADING WINDOW][{}] \
                        now={} | \
                        trading_start={} | \
                        trading_end={}",
                        ev.symbol,
                        timestamp,
                        trading_start,
                        trading_end,
                    ),
                );


                // ============================================================
                // SHORT ENTRY
                // ============================================================

                // Price has risen by the configured percentage from the rolling low.
                // Enter short expecting the upward move to mean-revert downward.
                if pct_from_min >= self.entry_percent {
                    let intent = TradeIntent {
                        strategy: self.name().to_string(),
                        symbol: ev.symbol.clone(),
                        is_long: false,
                        price: trade.price,
                        trade_amount_usd: self.trade_amount_usd,
                        entry_percent: pct_from_min,
                        exit_profit_percent: self.exit_profit_percent,
                        exit_loss_percent: self.exit_loss_percent,
                        emergency_exit_percent: self.emergency_exit_percent,
                        min_sl_delay: self.min_sl_delay,
                        enable_trade_duration: self.enable_trade_duration,
                        trade_duration_secs: self.trade_duration_secs,
                        ts_ms: timestamp.timestamp_millis(),
                    };

                    print_log(
                        self.debug_mode,
                        false,
                        format!(
                            "[BASE BACKTEST SHORT ENTRY][{}] \
                            price=${:.4} | \
                            above_min={:.3}% >= {:.3}% | \
                            amount=${:.2}",
                            ev.symbol,
                            trade.price,
                            pct_from_min,
                            self.entry_percent,
                            self.trade_amount_usd,
                        ),
                    );

                    let _ = self.tx_engine.send(
                        EngineEvent::OpenMarketOrderBacktest(
                            intent
                        )
                    );
                }


                // ============================================================
                // LONG ENTRY
                // ============================================================

                // Price has fallen by the configured percentage from the rolling high.
                // Enter long expecting the downward move to mean-revert upward.
                else if pct_from_max >= self.entry_percent {
                    let intent = TradeIntent {
                        strategy: self.name().to_string(),
                        symbol: ev.symbol.clone(),
                        is_long: true,
                        price: trade.price,
                        trade_amount_usd: self.trade_amount_usd,
                        entry_percent: pct_from_max,
                        exit_profit_percent: self.exit_profit_percent,
                        exit_loss_percent: self.exit_loss_percent,
                        emergency_exit_percent: self.emergency_exit_percent,
                        min_sl_delay: self.min_sl_delay,
                        enable_trade_duration: self.enable_trade_duration,
                        trade_duration_secs: self.trade_duration_secs,
                        ts_ms: timestamp.timestamp_millis(),
                    };

                    print_log(
                        self.debug_mode,
                        false,
                        format!(
                            "[BASE BACKTEST LONG ENTRY][{}] \
                            price=${:.4} | \
                            below_max={:.3}% >= {:.3}% | \
                            amount=${:.2}",
                            ev.symbol,
                            trade.price,
                            pct_from_max,
                            self.entry_percent,
                            self.trade_amount_usd,
                        ),
                    );

                    let _ = self.tx_engine.send(
                        EngineEvent::OpenMarketOrderBacktest(
                            intent
                        )
                    );
                }
            }
        }

            // Backtest does not receive quote events
            AppEvent::Quote(quote) => {
                print_log(
                    self.debug_mode,
                    false,
                    format!(
                        "[BASE BACKTEST QUOTE][{}] bid=${:.4} | ask=${:.4}",
                        ev.symbol,
                        quote.bid_price,
                        quote.ask_price,
                    ),
                );
            }

            AppEvent::BarClosed(bar) => {
                // Candle history may not contain enough data yet.
                // Always check for None / required length before using candles.
                // If data is missing, safely skip the trading decision.

                print_log(
                    self.debug_mode,
                    false,
                    format!(
                        "[BASE BACKTEST BAR CLOSED][{}][{}s] \
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
                    ),
                );

                let timeframe =
                    bar.timeframe;

                // Store the newly completed candle in the strategy's candle history.
                self.candle_history
                    .push(bar);

                print_log(
                    self.debug_mode,
                    false,
                    format!(
                        "[BASE BACKTEST CANDLES][{}][{}s] stored={}",
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
