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
        execution::{
            OrderId,
            OrderPurpose,
            OrderState,
        },
        execution::order::{
            OrderType,
            TimeInForce,
        },
        trade_intent::TradeIntent,
        trades::{
            Direction,
            TradeId,
        },
    },
    shared::print_log,
    strategies::Strategy,
};


// ============================================================================
// LIMIT ORDER TEST STRATEGY
// ============================================================================
//
// Simple live strategy used to test resting limit orders.
//
// For each configured asset, the strategy:
//
//     1. Waits for live market data.
//     2. Calculates a buy limit price a configured percentage below market.
//     3. Submits a limit-order trade.
//     4. Waits for the limit order to become Working/resting.
//     5. Leaves the order resting for a configured amount of time.
//     6. Cancels the resting order.
//     7. Waits until the cancellation is confirmed.
//     8. Waits for a configured repeat delay.
//     9. Repeats until the configured number of cycles is complete.
//
// Each asset runs independently, allowing multiple limit orders to be resting
// at the same time across different symbols.
//
// The strategy is intended only for live execution testing. Its limit prices
// are deliberately placed away from the current market so the orders normally
// rest instead of filling immediately.
//
// Example:
//
//     BTC market price = $76,000
//     limit offset     = 5%
//     buy limit        = ~$72,200
//
//     submit limit order
//         ↓
//     wait for order to rest
//         ↓
//     wait 15 seconds
//         ↓
//     cancel order
//         ↓
//     wait for cancellation
//         ↓
//     repeat
//
// This is a test strategy only and is not intended to represent a profitable
// or production trading strategy.
// ============================================================================



// ============================================================
// ASSET STATE
// ============================================================

#[derive(Debug, Clone)]
enum LimitOrderAssetPhase {
    WaitingToOpen,

    WaitingForOrder {
        trade_id: Option<TradeId>,
    },

    WaitingForWorking {
        trade_id: TradeId,
        order_id: OrderId,
    },

    Resting {
        trade_id: TradeId,
        order_id: OrderId,
        working_at: Instant,
    },

    WaitingForCancel {
        trade_id: TradeId,
        order_id: OrderId,
    },

    WaitingToRepeat {
        repeat_at: Instant,
    },

    Finished,
}


#[derive(Debug, Clone)]
struct LimitOrderAssetState {
    phase: LimitOrderAssetPhase,

    /// Number of completed submit -> rest -> cancel cycles.
    completed_cycles: usize,
}


// ============================================================
// LIMIT ORDER STRATEGY
// ============================================================

pub struct LimitOrderStrategy {
    pub enable_strategy: bool,
    pub debug_mode: bool,

    tx_engine: TokioUnboundedSender<EngineEvent>,
    rx_view: watch::Receiver<Arc<EngineView>>,

    // ========================================================
    // TEST CONFIGURATION
    // ========================================================

    /// Assets this test strategy is allowed to trade.
    allowed_assets: Vec<String>,

    /// USD notional requested for every test order.
    trade_amount_usd: f64,

    /// Direction used for the test entry orders.
    direction: Direction,

    /// Percentage below market used for LONG limit orders.
    ///
    /// Example:
    ///
    /// market = $100
    /// offset = 5%
    /// limit  = $95
    ///
    /// This intentionally places the order away from the market so it should
    /// rest rather than immediately execute.
    limit_offset_percent: f64,

    /// Delay after strategy creation before the first order may be submitted.
    first_trade_delay: Duration,

    /// Amount of time the order should remain Working before cancellation.
    resting_duration: Duration,

    /// Delay after confirmed cancellation before starting another cycle.
    repeat_delay: Duration,

    /// Maximum number of complete resting-order cancellation cycles per asset.
    max_cycles_per_asset: usize,

    started_at: Instant,

    asset_state: HashMap<String, LimitOrderAssetState>,
}


// ============================================================
// CONSTRUCTOR
// ============================================================

impl LimitOrderStrategy {
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
                    LimitOrderAssetState {
                        phase: LimitOrderAssetPhase::WaitingToOpen,
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
            limit_offset_percent: 5.0,
            first_trade_delay: Duration::from_secs(10),
            resting_duration: Duration::from_secs(15),
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
    // LIMIT PRICE
    // ============================================================

    fn calculate_limit_px(
        &self,
        market_px: f64,
    ) -> f64 {
        match self.direction {
            // Buy below the current market.
            Direction::Long => {
                market_px
                    * (1.0 - self.limit_offset_percent / 100.0)
            }

            // Sell above the current market.
            Direction::Short => {
                market_px
                    * (1.0 + self.limit_offset_percent / 100.0)
            }
        }
    }


    // ============================================================
    // OPEN LIMIT TRADE
    // ============================================================

    fn send_open(
        &self,
        symbol: &str,
        market_px: f64,
        ts_ms: i64,
    ) -> bool {
        let limit_px =
            self.calculate_limit_px(
                market_px
            );

        let intent = TradeIntent {
            strategy_id: self.name().to_string(),
            symbol: symbol.to_string(),
            direction: self.direction,
            signal_px: market_px,
            target_notional: self.trade_amount_usd,

            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Gtc,

            limit_px: Some(limit_px),
            stop_px: None,

            // We do not require maker-only behavior for this first lifecycle
            // test. The limit is intentionally placed far from market.
            post_only: false,

            // This strategy is testing the order lifecycle only.
            take_profit_percent: None,
            stop_loss_percent: None,
            emergency_stop_percent: None,
            stop_loss_delay_secs: None,
            max_duration_secs: None,

            ts_ms,
        };


        print_log(
            self.debug_mode,
            true,
            format!(
                "[LimitOrderStrategy][{}] OPEN LIMIT TRADE REQUEST | direction={:?} | notional=${:.2} | market_px={} | limit_px={} | offset={}%",
                symbol,
                self.direction,
                self.trade_amount_usd,
                market_px,
                limit_px,
                self.limit_offset_percent,
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
impl Strategy for LimitOrderStrategy {
    fn name(&self) -> &'static str {
        "LimitOrderStrategy"
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

            LimitOrderAssetPhase::WaitingToOpen => {
                if current_state.completed_cycles >= self.max_cycles_per_asset {
                    if let Some(state) =
                        self.asset_state.get_mut(&ev.symbol)
                    {
                        state.phase =
                            LimitOrderAssetPhase::Finished;
                    }

                    return;
                }


                if current_state.completed_cycles == 0
                    && self.started_at.elapsed() < self.first_trade_delay
                {
                    return;
                }


                // The previous zero-fill canceled Trade should have been
                // removed before another cycle is allowed to begin.
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
                            LimitOrderAssetPhase::WaitingForOrder {
                                trade_id: None,
                            };
                    }
                }
            }


            // ========================================================
            // WAIT FOR TRADE + ENTRY ORDER
            // ========================================================

            LimitOrderAssetPhase::WaitingForOrder {
                trade_id,
            } => {
                let resolved_trade_id =
                    trade_id.or_else(|| {
                        view.active_trade_id_for_strategy_symbol(
                            self.name(),
                            &ev.symbol,
                        )
                    });


                let Some(trade_id) =
                    resolved_trade_id
                else {
                    return;
                };


                // Find the canonical active Entry Order associated with this
                // logical Trade.
                //
                // We can add a dedicated helper later if this lookup becomes
                // common across strategies.
                let Some(order) =
                    view.orders
                        .active_for_trade(trade_id)
                        .find(|order| {
                            order.purpose == OrderPurpose::Entry
                        })
                else {
                    // Preserve the resolved TradeId so we do not need to
                    // rediscover it on every market event.
                    if let Some(state) =
                        self.asset_state.get_mut(&ev.symbol)
                    {
                        state.phase =
                            LimitOrderAssetPhase::WaitingForOrder {
                                trade_id: Some(trade_id),
                            };
                    }

                    return;
                };


                let order_id =
                    order.id;


                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[LimitOrderStrategy][{}] ENTRY ORDER FOUND | trade_id={} | order_id={} | state={:?} | limit_px={:?}",
                        ev.symbol,
                        trade_id,
                        order_id,
                        order.state,
                        order.limit_px,
                    ),
                );


                if let Some(state) =
                    self.asset_state.get_mut(&ev.symbol)
                {
                    state.phase =
                        LimitOrderAssetPhase::WaitingForWorking {
                            trade_id,
                            order_id,
                        };
                }
            }


            // ========================================================
            // WAIT FOR WORKING
            // ========================================================

            LimitOrderAssetPhase::WaitingForWorking {
                trade_id,
                order_id,
            } => {
                let Some(order) =
                    view.orders.get(order_id)
                else {
                    return;
                };


                if order.state == OrderState::Working {
                    print_log(
                        self.debug_mode,
                        true,
                        format!(
                            "[LimitOrderStrategy][{}] LIMIT ORDER WORKING | trade_id={} | order_id={} | limit_px={:?} | qty={}",
                            ev.symbol,
                            trade_id,
                            order_id,
                            order.limit_px,
                            order.quantity,
                        ),
                    );


                    if let Some(state) =
                        self.asset_state.get_mut(&ev.symbol)
                    {
                        state.phase =
                            LimitOrderAssetPhase::Resting {
                                trade_id,
                                order_id,
                                working_at: Instant::now(),
                            };
                    }

                    return;
                }


                // This test expects the limit order to rest without filling.
                // Surface unexpected terminal states rather than silently
                // treating them as a successful test cycle.
                if order.state.is_terminal() {
                    print_log(
                        self.debug_mode,
                        true,
                        format!(
                            "[LimitOrderStrategy][{}] UNEXPECTED TERMINAL ORDER BEFORE WORKING | trade_id={} | order_id={} | state={:?}",
                            ev.symbol,
                            trade_id,
                            order_id,
                            order.state,
                        ),
                    );
                }
            }


            // ========================================================
            // RESTING
            // ========================================================

            LimitOrderAssetPhase::Resting {
                trade_id,
                order_id,
                working_at,
            } => {
                let Some(order) =
                    view.orders.get(order_id)
                else {
                    return;
                };


                // The first test expects zero fills.
                //
                // If the market moves enough to execute the order, do not send
                // the normal test cancellation and pretend the expected
                // resting lifecycle occurred.
                if order.filled_quantity > f64::EPSILON {
                    print_log(
                        self.debug_mode,
                        true,
                        format!(
                            "[LimitOrderStrategy][{}] LIMIT ORDER RECEIVED UNEXPECTED FILL | trade_id={} | order_id={} | filled_qty={} | state={:?}",
                            ev.symbol,
                            trade_id,
                            order_id,
                            order.filled_quantity,
                            order.state,
                        ),
                    );

                    return;
                }


                if order.state != OrderState::Working {
                    return;
                }


                if working_at.elapsed() < self.resting_duration {
                    return;
                }


                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[LimitOrderStrategy][{}] CANCEL ORDER REQUEST | trade_id={} | order_id={} | resting_for={}s",
                        ev.symbol,
                        trade_id,
                        order_id,
                        working_at.elapsed().as_secs(),
                    ),
                );


                if self.tx_engine
                    .send(
                        EngineEvent::CancelOrder {
                            order_id,
                            reason:
                                "LimitOrderStrategy resting limit-order test"
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
                            LimitOrderAssetPhase::WaitingForCancel {
                                trade_id,
                                order_id,
                            };
                    }
                }
            }


            // ========================================================
            // WAIT FOR CONFIRMED CANCELLATION
            // ========================================================

            LimitOrderAssetPhase::WaitingForCancel {
                trade_id,
                order_id,
            } => {
                let Some(order) =
                    view.orders.get(order_id)
                else {
                    return;
                };


                if order.state != OrderState::Canceled {
                    return;
                }


                // For this specific zero-fill test, order_canceled.rs should
                // remove the still-Opening Trade after cancellation.
                let trade_removed =
                    view.trades
                        .get(trade_id)
                        .is_none();


                if !trade_removed {
                    print_log(
                        self.debug_mode,
                        true,
                        format!(
                            "[LimitOrderStrategy][{}] CANCEL CONFIRMED BUT OPENING TRADE STILL EXISTS | trade_id={} | order_id={}",
                            ev.symbol,
                            trade_id,
                            order_id,
                        ),
                    );

                    return;
                }


                let completed_cycles =
                    current_state.completed_cycles + 1;


                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[LimitOrderStrategy][{}] CANCEL CYCLE COMPLETE | trade_id={} | order_id={} | completed_cycles={} | max_cycles={}",
                        ev.symbol,
                        trade_id,
                        order_id,
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
                            LimitOrderAssetPhase::Finished;


                        print_log(
                            self.debug_mode,
                            true,
                            format!(
                                "[LimitOrderStrategy][{}] TEST FINISHED | completed_cycles={}",
                                ev.symbol,
                                completed_cycles,
                            ),
                        );
                    } else {
                        state.phase =
                            LimitOrderAssetPhase::WaitingToRepeat {
                                repeat_at:
                                    Instant::now()
                                        + self.repeat_delay,
                            };
                    }
                }
            }


            // ========================================================
            // WAIT BEFORE REPEAT
            // ========================================================

            LimitOrderAssetPhase::WaitingToRepeat {
                repeat_at,
            } => {
                if Instant::now() < repeat_at {
                    return;
                }


                if let Some(state) =
                    self.asset_state.get_mut(&ev.symbol)
                {
                    state.phase =
                        LimitOrderAssetPhase::WaitingToOpen;
                }


                print_log(
                    self.debug_mode,
                    true,
                    format!(
                        "[LimitOrderStrategy][{}] REPEAT DELAY COMPLETE | next_cycle={}",
                        ev.symbol,
                        current_state.completed_cycles + 1,
                    ),
                );
            }


            // ========================================================
            // FINISHED
            // ========================================================

            LimitOrderAssetPhase::Finished => {
                return;
            }
        }
    }
}