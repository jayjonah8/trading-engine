#![allow(dead_code)]

use std::{
    collections::HashMap,
    sync::Arc,
    time::{
        Duration,
        Instant,
    },
};

use tokio::sync::{
    mpsc::UnboundedSender as TokioUnboundedSender,
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
        execution::ExitOrderRequest,
        execution::order::{
            OrderType,
            TimeInForce,
        },
        trade_intent::TradeIntent,
        trades::{
            Direction,
            TradeId,
            TradeState,
        },
    },
    shared::print_log,
    strategies::Strategy,
};


// ============================================================================
// MARKET ORDER TEST STRATEGY
// ============================================================================
//
// Simple live strategy used to test market-order entries and exits.
//
// For each configured asset, the strategy:
//
//     1. Waits for live market data.
//     2. Submits a market-order trade using the configured USD notional.
//     3. Waits for the trade to be confirmed Open.
//     4. Leaves the position open for a configured amount of time.
//     5. Submits a market-order close.
//     6. Waits for the trade to be confirmed Closed.
//     7. Waits for a configured repeat delay.
//     8. Repeats until the configured number of cycles is complete.
//
// Each asset runs independently, allowing multiple market-order trade
// lifecycles to run concurrently across different symbols.
//
// Example:
//
//     receive BTC market data
//         ↓
//     submit market entry
//         ↓
//     wait for trade to open
//         ↓
//     wait 15 seconds
//         ↓
//     submit market close
//         ↓
//     wait for trade to close
//         ↓
//     wait 15 seconds
//         ↓
//     repeat
//
// The strategy does not use take profit, stop loss, emergency stop, or maximum
// duration rules. It controls when trades are closed using its own close delay.
//
// This is a test strategy only and is not intended to represent a profitable
// or production trading strategy.
// ============================================================================



// ============================================================
// ASSET STATE
// ============================================================

#[derive(Debug, Clone)]
enum MarketOrderAssetPhase {
    WaitingToOpen,
    WaitingForOpen {
        trade_id: Option<TradeId>,
    },
    WaitingToClose {
        trade_id: TradeId,
        opened_at: Instant,
    },
    WaitingForClose {
        trade_id: TradeId,
    },
    WaitingToRepeat {
        repeat_at: Instant,
    },
    Finished,
}


#[derive(Debug, Clone)]
struct MarketOrderAssetState {
    phase: MarketOrderAssetPhase,

    /// Number of completed open -> close cycles.
    completed_cycles: usize,
}


// ============================================================
// MARKET ORDER STRATEGY
// ============================================================

pub struct MarketOrderStrategy {
    pub enable_strategy: bool,
    pub debug_mode: bool,

    tx_engine: TokioUnboundedSender<EngineEvent>,
    rx_view: watch::Receiver<Arc<EngineView>>,

    // ========================================================
    // TEST CONFIGURATION
    // ========================================================

    /// Assets this test strategy is allowed to trade.
    ///
    /// This is strategy-level filtering only.
    /// Engine Config / risk checks remain authoritative.
    allowed_assets: Vec<String>,

    /// USD notional requested for every test trade.
    trade_amount_usd: f64,

    /// Direction used for the test orders.
    direction: Direction,

    /// Delay after strategy creation before the first trade may be submitted.
    first_trade_delay: Duration,

    /// Delay after a trade is confirmed Open before requesting its close.
    close_delay: Duration,

    /// Delay after a completed close before starting another cycle.
    repeat_delay: Duration,

    /// Maximum number of complete open -> close cycles per asset.
    ///
    /// Example:
    ///
    /// max_cycles_per_asset = 3
    ///
    /// BTC:
    ///     open -> close
    ///     open -> close
    ///     open -> close
    ///     finished
    max_cycles_per_asset: usize,

    started_at: Instant,

    asset_state: HashMap<String, MarketOrderAssetState>,
}


// ============================================================
// CONSTRUCTOR
// ============================================================

impl MarketOrderStrategy {
    pub fn new(
        tx_engine: TokioUnboundedSender<EngineEvent>,
        rx_view: watch::Receiver<Arc<EngineView>>,
    ) -> Self {
        let allowed_assets = vec![
            // "BTC".to_string(),
            // "ETH".to_string(),
            // "SOL".to_string(),
            // "HYPE".to_string(),
            // "ZEC".to_string(),

            // Stocks
            "AAPL".to_string(),
            "MSFT".to_string(),
            "NVDA".to_string(),
        ];

        let asset_state = allowed_assets
            .iter()
            .map(|symbol| {
                (
                    symbol.clone(),
                    MarketOrderAssetState {
                        phase: MarketOrderAssetPhase::WaitingToOpen,
                        completed_cycles: 0,
                    },
                )
            })
            .collect();

        Self {
            enable_strategy: true,
            debug_mode: true,
            tx_engine,
            rx_view,
            allowed_assets,
            trade_amount_usd: 500.0,
            direction: Direction::Long,
            first_trade_delay: Duration::from_secs(10),
            close_delay: Duration::from_secs(15),
            repeat_delay: Duration::from_secs(15),
            max_cycles_per_asset: 3,
            started_at: Instant::now(),
            asset_state,
        }
    }


    // ============================================================
    // SYMBOL ALLOWLIST
    // ============================================================

    fn is_allowed_asset(
        &self,
        symbol: &str,
    ) -> bool {
        self.allowed_assets
            .iter()
            .any(|allowed| allowed == symbol)
    }


    // ============================================================
    // OPEN MARKET TRADE
    // ============================================================

    fn send_open(
        &self,
        symbol: &str,
        signal_px: f64,
        ts_ms: i64,
    ) -> bool {
        let intent = TradeIntent {
            strategy_id: self.name().to_string(),
            symbol: symbol.to_string(),
            direction: self.direction,
            signal_px,
            target_notional: self.trade_amount_usd,
            margin_usd: self.trade_amount_usd,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::Day,
            limit_px: None,
            stop_px: None,
            post_only: false,

            // This strategy controls its own close timing.
            take_profit_percent: None,
            stop_loss_percent: None,
            emergency_stop_percent: None,
            stop_loss_delay_secs: None,
            trade_duration_secs: None,

            trailing_stop: None,

            ts_ms,
        };

        print_log(
            self.debug_mode,
            true,
            format!(
                "[MarketOrderStrategy][{}] OPEN MARKET TRADE REQUEST | direction={:?} | notional=${:.2} | signal_px={}",
                symbol,
                self.direction,
                self.trade_amount_usd,
                signal_px,
            ),
        );

        self.tx_engine
            .send(
                EngineEvent::OpenTrade(
                    intent
                )
            )
            .is_ok()
    }
}


// ============================================================
// STRATEGY IMPLEMENTATION
// ============================================================

#[async_trait::async_trait]
impl Strategy for MarketOrderStrategy {
    fn name(&self) -> &'static str {
        "MarketOrderStrategy"
    }

    async fn on_event(
        &mut self,
        ev: AppEventEnvelope,
    ) {
        if !self.enable_strategy {
            return;
        }


        // ============================================================
        // ALLOWED ASSET
        // ============================================================

        if !self.is_allowed_asset(&ev.symbol) {
            return;
        }


        // ============================================================
        // MARKET PRICE / TIMESTAMP
        // ============================================================

        let (market_px, ts_ms) =
            match ev.event {
                AppEvent::TapeTrade(trade) => {
                    (
                        trade.price,
                        trade.timestamp_ms,
                    )
                }

                AppEvent::Quote(quote) => {
                    (
                        (quote.bid_price + quote.ask_price) / 2.0,
                        quote.timestamp_ms,
                    )
                }

                AppEvent::BarClosed(_) => {
                    return;
                }
            };


        // ============================================================
        // ENGINE VIEW
        // ============================================================

        let view =
            self.rx_view
                .borrow()
                .clone();


        // ============================================================
        // CURRENT STATE
        // ============================================================

        let Some(current_state) =
            self.asset_state
                .get(&ev.symbol)
                .cloned()
        else {
            return;
        };


        // ============================================================
        // STATE MACHINE
        // ============================================================

        match current_state.phase {

            // ========================================================
            // WAITING TO OPEN
            // ========================================================

            MarketOrderAssetPhase::WaitingToOpen => {
                if current_state.completed_cycles >= self.max_cycles_per_asset {
                    if let Some(state) =
                        self.asset_state.get_mut(&ev.symbol)
                    {
                        state.phase =
                            MarketOrderAssetPhase::Finished;
                    }

                    return;
                }


                // The first cycle waits from strategy startup.
                //
                // Later cycles only enter WaitingToOpen after their explicit
                // repeat delay has completed.
                if current_state.completed_cycles == 0
                    && self.started_at.elapsed() < self.first_trade_delay
                {
                    return;
                }


                // Do not submit if this strategy already owns an active trade
                // for this symbol.
                if view.trades.has_active_trade_for_strategy_symbol(
                    self.name(),
                    &ev.symbol,
                ) {
                    return;
                }


                if self.send_open(
                    &ev.symbol,
                    market_px,
                    ts_ms,
                ) {
                    if let Some(state) =
                        self.asset_state.get_mut(&ev.symbol)
                    {
                        state.phase =
                            MarketOrderAssetPhase::WaitingForOpen {
                                trade_id: None,
                            };
                    }
                }
            }


            // ========================================================
            // WAITING FOR ENGINE TO CREATE / OPEN TRADE
            // ========================================================

            MarketOrderAssetPhase::WaitingForOpen {
                trade_id,
            } => {
                let resolved_trade_id =
                    trade_id.or_else(|| {
                        view.trades
                            .active_trade_id_for_strategy_symbol(
                                self.name(),
                                &ev.symbol,
                            )
                    });


                let Some(trade_id) =
                    resolved_trade_id
                else {
                    return;
                };

                let Some(trade) =
                    view.trades.get(trade_id)
                else {
                    return;
                };

                if trade.state == TradeState::Open {
                    print_log(
                        self.debug_mode,
                        true,
                        format!(
                            "[MarketOrderStrategy][{}] TRADE CONFIRMED OPEN | trade_id={} | quantity={} | avg_entry_px={:?}",
                            ev.symbol,
                            trade.id,
                            trade.quantity,
                            trade.avg_entry_px,
                        ),
                    );


                    if let Some(state) =
                        self.asset_state.get_mut(&ev.symbol)
                    {
                        state.phase =
                            MarketOrderAssetPhase::WaitingToClose {
                                trade_id,
                                opened_at: Instant::now(),
                            };
                    }
                } else if let Some(state) =
                    self.asset_state.get_mut(&ev.symbol)
                {
                    state.phase =
                        MarketOrderAssetPhase::WaitingForOpen {
                            trade_id: Some(trade_id),
                        };
                }
            }


            // ========================================================
            // WAIT BEFORE CLOSE
            // ========================================================

            MarketOrderAssetPhase::WaitingToClose {
                trade_id,
                opened_at,
            } => {
                if opened_at.elapsed() < self.close_delay {
                    return;
                }


                let Some(trade) =
                    view.trades.get(trade_id)
                else {
                    return;
                };


                if trade.state != TradeState::Open {
                    return;
                }


                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[MarketOrderStrategy][{}] CLOSE TRADE REQUEST | trade_id={} | open_for={}s",
                        ev.symbol,
                        trade_id,
                        opened_at.elapsed().as_secs(),
                    ),
                );


                if self.tx_engine
                    .send(
                        EngineEvent::CloseTrade {
                            trade_id,
                            execution:
                                ExitOrderRequest::market(),
                            reason:
                                "MarketOrderStrategy live market-order test"
                                    .to_string(),
                            ts_ms,
                        }
                    )
                    .is_ok()
                {
                    if let Some(state) =
                        self.asset_state.get_mut(&ev.symbol)
                    {
                        state.phase =
                            MarketOrderAssetPhase::WaitingForClose {
                                trade_id,
                            };
                    }
                }
            }


            // ========================================================
            // WAIT FOR CONFIRMED CLOSE
            // ========================================================

            MarketOrderAssetPhase::WaitingForClose {
                trade_id,
            } => {
                let trade_closed =
                    view.trades
                        .get(trade_id)
                        .map(|trade| {
                            trade.state == TradeState::Closed
                        })
                        .unwrap_or(false);


                if !trade_closed {
                    return;
                }


                let completed_cycles =
                    current_state.completed_cycles + 1;


                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[MarketOrderStrategy][{}] CYCLE COMPLETE | completed_cycles={} | max_cycles={}",
                        ev.symbol,
                        completed_cycles,
                        self.max_cycles_per_asset,
                    ),
                );


                if let Some(state) =
                    self.asset_state.get_mut(&ev.symbol)
                {
                    state.completed_cycles =
                        completed_cycles;


                    if completed_cycles >= self.max_cycles_per_asset {
                        state.phase =
                            MarketOrderAssetPhase::Finished;

                        print_log(
                            self.debug_mode,
                            true,
                            format!(
                                "[MarketOrderStrategy][{}] TEST FINISHED | completed_cycles={}",
                                ev.symbol,
                                completed_cycles,
                            ),
                        );
                    } else {
                        state.phase =
                            MarketOrderAssetPhase::WaitingToRepeat {
                                repeat_at: Instant::now() + self.repeat_delay,
                            };
                    }
                }
            }


            // ========================================================
            // WAIT BEFORE REPEAT
            // ========================================================

            MarketOrderAssetPhase::WaitingToRepeat {
                repeat_at,
            } => {
                if Instant::now() < repeat_at {
                    return;
                }


                if let Some(state) =
                    self.asset_state.get_mut(&ev.symbol)
                {
                    state.phase =
                        MarketOrderAssetPhase::WaitingToOpen;
                }


                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[MarketOrderStrategy][{}] REPEAT DELAY COMPLETE | next_cycle={}",
                        ev.symbol,
                        current_state.completed_cycles + 1,
                    ),
                );
            }


            // ========================================================
            // FINISHED
            // ========================================================

            MarketOrderAssetPhase::Finished => {
                return;
            }
        }
    }
}