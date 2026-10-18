#![allow(dead_code)]

use std::{
    collections::HashMap,
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

use crate::{
    core::{
        engine::engine::EngineEvent,
        engine_view::EngineView,
        events::{
            AppEvent,
            AppEventEnvelope,
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
        session::regular_market_session,
    },

    shared::print_log,

    strategies::{
        base::{
            backtest_state::BaseBacktestStrategy,
            strategy_session::strategy_trading_window,
        },
        Strategy,
    },
};


// ============================================================
// BASE BACKTEST STRATEGY
// ============================================================

impl BaseBacktestStrategy {
    /// Creates the historical version of `BaseStrategy`.
    ///
    /// Historical strategy logic uses canonical market events and historical
    /// event timestamps rather than wall-clock time.
    pub fn new(
        tx_engine: UnboundedSender<EngineEvent>,
        rx_view: watch::Receiver<Arc<EngineView>>,
    ) -> Self {
        let candle_config = CandleConfig::default();

        Self {
            tx_engine,
            rx_view,
            enable_strategy: true,
            debug_mode: true,
            total_trade_events: 0,
            window_prices: HashMap::new(),
            entry_window_secs: 10_800,
            trade_amount_usd: 250.0,
            entry_percent: 0.05,
            exit_profit_percent: 1.00,
            exit_loss_percent: 2.1,
            emergency_exit_percent: 3.0,
            min_sl_delay: 7200,
            enable_trade_duration: true,
            trade_duration_secs: Some(900), // 900 = 15mins
            track_price_window_while_in_trade: false,
            candle_history: CandleHistory::new(candle_config.history_retention),
            market_open_delay_minutes: 15,
            market_close_buffer_minutes: 15,
            close_all_before_market_close_minutes: 10,
            close_all_sent_for_session: None,
            track_prices_during_open_delay: false,
            current_session_start_by_symbol: HashMap::new(),
        }
    }
}


// ============================================================
// STRATEGY FACTORY
// ============================================================

pub fn make_base_backtest(
    tx_engine: UnboundedSender<EngineEvent>,
    rx_view: watch::Receiver<Arc<EngineView>>,
) -> Box<dyn Strategy> {
    Box::new(BaseBacktestStrategy::new(
        tx_engine,
        rx_view,
    ))
}


// ============================================================
// STRATEGY IMPLEMENTATION
// ============================================================

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
            // ========================================================
            // TAPE TRADE
            // ========================================================

            AppEvent::TapeTrade(trade) => {
                self.total_trade_events += 1;

                print_log(
                self.debug_mode,
                true,
                format!(
                    "[BASE BACKTEST TAPE TRADE][{}] price=${:.4} | size={} | timestamp_ms={}",
                    ev.symbol,
                    trade.price,
                    trade.size,
                    trade.timestamp_ms,
                ),
            );

   
                // Backtest entry flow:
                //
                // Historical TapeTrade
                // → restore DateTime<Utc> from timestamp_ms
                // → regular-hours filter
                // → reset state for a new market session
                // → calculate strategy trading window
                // → optionally track prices during opening delay
                // → optionally stop tracking while already in a trade
                // → update rolling price window
                // → check entry gates
                // → inspect EngineView
                // → construct canonical TradeIntent

                // ====================================================
                // HISTORICAL EVENT TIME
                // ====================================================

                let Some(timestamp): Option<DateTime<Utc>> =
                    DateTime::from_timestamp_millis(trade.timestamp_ms)
                else {
                    eprintln!(
                        "[BASE BACKTEST][{}] invalid timestamp_ms={}",
                        ev.symbol,
                        trade.timestamp_ms,
                    );

                    return;
                };


                // // ====================================================
                // // REGULAR MARKET SESSION
                // // ====================================================

                // let Some((session_start, session_end)) =
                //     regular_market_session(timestamp)
                // else {
                //     return;
                // };

                // if timestamp < session_start || timestamp >= session_end {
                //     return;
                // }


                // // ====================================================
                // // NEW SESSION RESET
                // // ====================================================

                // let previous_session =
                //     self.current_session_start_by_symbol
                //         .get(&ev.symbol)
                //         .copied();

                // if previous_session != Some(session_start) {
                //     self.window_prices.remove(&ev.symbol);

                //     self.current_session_start_by_symbol.insert(
                //         ev.symbol.clone(),
                //         session_start,
                //     );

                //     print_log(
                //         self.debug_mode,
                //         false,
                //         format!(
                //             "[BASE BACKTEST SESSION RESET][{}] session_start={}",
                //             ev.symbol,
                //             session_start,
                //         ),
                //     );
                // }


                // // ====================================================
                // // STRATEGY TRADING WINDOW
                // // ====================================================

                // let Some((trading_start, trading_end)) =
                //     strategy_trading_window(
                //         timestamp,
                //         self.market_open_delay_minutes,
                //         self.market_close_buffer_minutes,
                //     )
                // else {
                //     return;
                // };


                // // ====================================================
                // // END-OF-SESSION FORCE CLOSE
                // // ====================================================

                // // market_close_buffer_minutes:
                // // Stops new entries before market close.
                // //
                // // close_all_before_market_close_minutes:
                // // Requests closure of remaining trades.
                // let close_all_time =
                //     session_end
                //         - chrono::Duration::minutes(
                //             self.close_all_before_market_close_minutes
                //         );

                // if timestamp >= close_all_time {
                //     if self.close_all_sent_for_session != Some(session_start) {
                //         self.close_all_sent_for_session = Some(session_start);

                //         print_log(
                //             self.debug_mode,
                //             true,
                //             format!(
                //                 "[BASE BACKTEST FORCE CLOSE ALL] \
                //                 now={} | \
                //                 close_all_time={} | \
                //                 session_end={} | \
                //                 close_before_market_close={}min",
                //                 timestamp,
                //                 close_all_time,
                //                 session_end,
                //                 self.close_all_before_market_close_minutes,
                //             ),
                //         );

                //         // TODO:
                //         // Re-enable through the canonical engine path once
                //         // historical execution is wired.
                //         //
                //         // Backtest should eventually use the same:
                //         //
                //         // EngineEvent::CloseAllTrades
                //         //
                //         // as live/simulation.
                //         //
                //         // let _ = self.tx_engine.send(
                //         //     EngineEvent::CloseAllTrades {
                //         //         reason: format!(
                //         //             "Forced close {} minutes before market close",
                //         //             self.close_all_before_market_close_minutes,
                //         //         ),
                //         //     }
                //         // );
                //     }

                //     return;
                // }


                // // ====================================================
                // // OPENING DELAY PRICE TRACKING
                // // ====================================================

                // if !self.track_prices_during_open_delay
                //     && timestamp < trading_start
                // {
                //     return;
                // }


                // ====================================================
                // ENGINE VIEW
                // ====================================================

                let view: Arc<EngineView> =
                    self.rx_view.borrow().clone();


                // ====================================================
                // OPEN TRADE PRICE-WINDOW TRACKING
                // ====================================================

                if !self.track_price_window_while_in_trade
                    && view.trades.has_active_trade_for_symbol(&ev.symbol)
                {
                    if self.window_prices.remove(&ev.symbol).is_some() {
                        print_log(
                            self.debug_mode,
                            false,
                            format!(
                                "[BASE BACKTEST WINDOW CLEARED][{}] \
                                active trade detected; price window cleared",
                                ev.symbol,
                            ),
                        );
                    }

                    return;
                }


                // ====================================================
                // ROLLING PRICE WINDOW
                // ====================================================

                if let Some((
                    pct_from_min,
                    pct_from_max,
                    elapsed_secs,
                )) = self.update_and_get_backtest_window_extreme_diffs(
                    &ev.symbol,
                    timestamp,
                    trade.price,
                ) {
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


                    // // ====================================================
                    // // ENTRY TIME GATE
                    // // ====================================================

                    // if timestamp < trading_start {
                    //     return;
                    // }

                    // if timestamp >= trading_end {
                    //     return;
                    // }


                    // ====================================================
                    // EXISTING ACTIVE TRADE GATE
                    // ====================================================

                    if view.trades.has_active_trade_for_symbol(&ev.symbol) {
                        return;
                    }


                    // print_log(
                    //     self.debug_mode,
                    //     false,
                    //     format!(
                    //         "[BASE BACKTEST TRADING WINDOW][{}] \
                    //         now={} | \
                    //         trading_start={} | \
                    //         trading_end={}",
                    //         ev.symbol,
                    //         timestamp,
                    //         trading_start,
                    //         trading_end,
                    //     ),
                    // );


                    // ====================================================
                    // SHORT ENTRY
                    // ====================================================

                    if pct_from_min >= self.entry_percent {
                        let intent = TradeIntent {
                            strategy_id: self.name().to_string(),
                            symbol: ev.symbol.clone(),
                            direction: Direction::Short,
                            signal_px: trade.price,
                            target_notional: self.trade_amount_usd,
                            order_type: OrderType::Market,
                            time_in_force: TimeInForce::Day,
                            limit_px: None,
                            stop_px: None,
                            post_only: false,
                            take_profit_percent: Some(self.exit_profit_percent),
                            stop_loss_percent: Some(self.exit_loss_percent),
                            emergency_stop_percent: Some(self.emergency_exit_percent),
                            stop_loss_delay_secs: Some(self.min_sl_delay),
                            trade_duration_secs: if self.enable_trade_duration {
                                self.trade_duration_secs
                            } else {
                                None
                            },
                            ts_ms: trade.timestamp_ms,
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

                        // TODO:
                        // Enable once canonical historical execution is wired.
                        //
                        let _ = self.tx_engine.send(
                            EngineEvent::OpenTrade(intent)
                        );

                        let _ = intent;
                    }


                    // ====================================================
                    // LONG ENTRY
                    // ====================================================

                    else if pct_from_max >= self.entry_percent {
                        let intent = TradeIntent {
                            strategy_id: self.name().to_string(),
                            symbol: ev.symbol.clone(),
                            direction: Direction::Long,
                            signal_px: trade.price,
                            target_notional: self.trade_amount_usd,
                            order_type: OrderType::Market,
                            time_in_force: TimeInForce::Day,
                            limit_px: None,
                            stop_px: None,
                            post_only: false,
                            take_profit_percent: Some(self.exit_profit_percent),
                            stop_loss_percent: Some(self.exit_loss_percent),
                            emergency_stop_percent: Some(self.emergency_exit_percent),
                            stop_loss_delay_secs: Some(self.min_sl_delay),
                            trade_duration_secs: if self.enable_trade_duration {
                                self.trade_duration_secs
                            } else {
                                None
                            },
                            ts_ms: trade.timestamp_ms,
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

                        // TODO:
                        // Enable once canonical historical execution is wired.
                        //
                        let _ = self.tx_engine.send(
                            EngineEvent::OpenTrade(intent)
                        );

                        let _ = intent;
                    }
                }
            }


            // ========================================================
            // QUOTE
            // ========================================================

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


            // ========================================================
            // BAR CLOSED
            // ========================================================

            AppEvent::BarClosed(bar) => {
                print_log(
                    self.debug_mode,
                    true,
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

                let timeframe = bar.timeframe;

                self.candle_history.push(bar);

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