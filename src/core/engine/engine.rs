#![allow(dead_code)]

use std::{
    collections::HashMap,
    sync::Arc,
};

use tokio::sync::{
    mpsc::UnboundedSender,
    oneshot,
    watch,
};

use crate::core::{
    config::Config,
    venue::VenueState,
    engine::{
        shared::{
            get_execution_mode,
            verify_engine_config,
        },
        state::{
            EngineState,
            ExecutionMode,
            LastPrice,
            TradingPhase,
        },
    },
    engine_view::EngineView,
    execution::{
        ExecCommand,
        ExecEvent,
        ExitOrderRequest,
        OrderChanges,
        OrderId,
    },
    general::General,
    risk::RiskLimits,
    trade_intent::TradeIntent,
    trades::TradeId,
};


use crate::notifications::env_helpers::validate_notification_env;


/// Events entering the authoritative engine loop.
///
/// Strategy decisions and market-data updates enter through `EngineEvent`.
///
/// Venue execution lifecycle facts enter separately through `ExecEvent`.
///
/// Backtesting should eventually use this exact same event model. A simulated
/// execution adapter can consume `ExecCommand` and emit canonical `ExecEvent`s
/// without requiring duplicated backtest-specific engine events.
pub enum EngineEvent {
    /// Requests creation of a new strategy trade.
    OpenTrade(TradeIntent),

    /// Requests cancellation of an active engine order.
    CancelOrder { order_id: OrderId, reason: String, ts_ms: i64 },

    /// Requests replacement/modification of an active engine order.
    ReplaceOrder { order_id: OrderId, quantity: Option<f64>, limit_px: Option<f64>, stop_px: Option<f64>, reason: String, ts_ms: i64 },

    /// Requests closure of one specific logical strategy Trade.
    ///
    /// `TradeId` identifies the authoritative Trade being closed.
    ///
    /// `execution` describes HOW the exit should be executed.
    ///
    /// Examples:
    ///
    /// Immediate market exit:
    ///
    ///     ExitOrderRequest::market()
    ///
    /// Resting limit exit:
    ///
    ///     ExitOrderRequest::limit(
    ///         limit_px,
    ///         TimeInForce::Gtc,
    ///         false,
    ///     )
    ///
    /// The Engine still determines:
    ///
    /// - exit side
    /// - remaining exit quantity
    /// - execution venue
    /// - OrderPurpose::Exit
    /// - reduce_only = true
    ///
    /// This prevents callers from using a CloseTrade request to intentionally
    /// increase or reverse exposure.
    ///
    /// `ts_ms` is canonical event time and is used consistently across live
    /// trading, simulation, and historical backtesting.
    CloseTrade { trade_id: TradeId, execution: ExitOrderRequest, reason: String, ts_ms: i64 },

    /// Requests closure of every eligible open Trade owned by this Engine instance.
    ///
    /// This is intentionally engine-wide rather than strategy-specific.
    /// It is used for bot-wide liquidation events such as end-of-session shutdown
    /// or emergency risk reduction.
    CloseAllTrades { reason: String, ts_ms: i64 },

    /// Market-price update used for valuation and strategy-level risk logic.
    ///
    /// `ts_ms` is always event time. During live trading this is live market
    /// time; during backtesting this is historical replay time.
    PriceTick { symbol: String, px: f64, ts_ms: i64 },

    /// Historical market-price update used during backtesting.
    ///
    /// Uses the original historical timestamp so risk checks and
    /// trade behavior remain tied to replay time instead of wall time.
    PriceTickBacktest { symbol: String, px: f64, ts_ms: i64 },

    // TODO add TradeClosed

    /// Backtest synchronization barrier.
    ///
    /// Confirms that all EngineEvents queued before this event
    /// have been processed before the backtest continues.
    BacktestBarrier { done: oneshot::Sender<()> },
}

/// Main authoritative trading engine.
///
/// The engine owns canonical trading state and is the only layer responsible
/// for applying state transitions.
///
/// Exchange adapters, strategies, market-data providers, simulators, and
/// notification systems communicate with the engine through commands/events
/// rather than directly mutating engine state.
pub struct Engine {
    /// Static/runtime engine configuration.
    pub cfg: Config,

    pub general: General,

    /// Canonical execution, position, trade, and risk state.
    pub state: EngineState,

    /// External venue/account/instrument state.
    ///
    /// This contains normalized account information and instrument execution
    /// metadata supplied by venue-specific runtime components.
    pub venue_state: VenueState,

    /// Most recent known price by symbol.
    pub last_px: HashMap<String, LastPrice>,

    /// Published read-only strategy view.
    pub tx_view: watch::Sender<Arc<EngineView>>,

    /// Channel for feeding events back into the authoritative engine loop.
    pub tx_engine: UnboundedSender<EngineEvent>,

    /// Venue-independent execution command channel.
    pub tx_exec: UnboundedSender<ExecCommand>,

    /// Live / simulation / backtest execution mode.
    ///
    /// Core state transitions should remain the same regardless of this mode.
    pub execution_mode: ExecutionMode,

    /// Current high-level engine phase.
    pub trading_phase: TradingPhase,

    pub debug_mode: bool,
}

impl Engine {
    /// Creates a new trading engine.
    ///
    /// Account/equity discovery is intentionally NOT performed here.
    ///
    /// Exchange adapters or application bootstrap should obtain venue account
    /// state and pass canonical account updates into the engine separately.
    /// This keeps the engine independent from Alpaca, IBKR, Hyperliquid, or
    /// any future venue.
    pub fn new(
        cfg: Config,
        tx_view: watch::Sender<Arc<EngineView>>,
        tx_engine: UnboundedSender<EngineEvent>,
        tx_exec: UnboundedSender<ExecCommand>,
    ) -> Self {
        verify_engine_config(&cfg);
        validate_notification_env(&cfg);

        let execution_mode = get_execution_mode(&cfg);


        // ============================================================
        // RISK LIMITS
        // ============================================================

        let risk_limits =
            RiskLimits {
                max_active_trades:
                    cfg.max_active_trades,

                max_active_assets:
                    cfg.max_active_assets,

                max_active_trades_per_symbol:
                    cfg.max_active_trades_per_symbol,

                max_active_trades_per_strategy:
                    cfg.max_active_trades_per_strategy,

                max_order_notional:
                    cfg.max_order_notional,

                max_gross_notional:
                    cfg.max_gross_notional,

                max_symbol_notional:
                    cfg.max_symbol_notional,
            };


        // ============================================================
        // INITIALIZE GENERAL STATE
        // ============================================================
        let general =
            General {
                realized_pnl: 0.0,
                sim_realized_pnl: 0.0,
                account_balance: 0.0, // @todo must get acount balance based on the venue / exchange
            };


        // ============================================================
        // ENGINE
        // ============================================================

        let engine = Self {
            cfg,
            general,
            state: EngineState::new(risk_limits),
            venue_state: VenueState::new(),
            last_px: HashMap::new(),
            tx_view,
            tx_engine,
            tx_exec,
            execution_mode,
            trading_phase: TradingPhase::Normal,
            // owner_by_symbol: HashMap::new(),
            // last_decision_ts: HashMap::new(),
            debug_mode: true,
        };

        engine.publish_view();
        engine
    }


    // ============================================================
    // ENGINE VIEW
    // ============================================================

    /// Publishes the latest read-only engine state to strategies.
    pub(crate) fn publish_view(&self) {
        let view = EngineView::from_parts(
            &self.cfg,
            &self.general,
            &self.state,
            &self.venue_state,
        );

        let _ = self.tx_view.send_replace(view);
    }

    
    // ============================================================
    // ENGINE EVENTS
    // ============================================================

    /// Central entry point for non-execution events processed by the engine.
    ///
    /// This is the authoritative strategy / market-data event dispatcher.
    pub fn on_event(&mut self, event: EngineEvent) {
        match event {
            EngineEvent::OpenTrade(intent) => {
                self.handle_open_trade(intent);
            }

            EngineEvent::CancelOrder { order_id, reason, ts_ms } => {
                self.handle_cancel_order(order_id, reason, ts_ms);
            }

            EngineEvent::ReplaceOrder { order_id, quantity, limit_px, stop_px, reason, ts_ms } => {
                let changes = OrderChanges { quantity, limit_px, stop_px };
                self.handle_replace_order(order_id, changes, reason, ts_ms);
            }

            EngineEvent::CloseTrade { trade_id, execution, reason, ts_ms} => {
                self.handle_close_trade(trade_id, execution, reason, ts_ms);
            }

            EngineEvent::CloseAllTrades { reason, ts_ms } => {
                self.handle_close_all_trades(reason, ts_ms);
            }

            EngineEvent::PriceTick { symbol, px, ts_ms } => {
                self.handle_price_tick(symbol, px, ts_ms);
            }

            // TODO: delete this later (all will go through PriceTick)
            EngineEvent::PriceTickBacktest { symbol, px, ts_ms } => {
                self.handle_price_tick(symbol, px, ts_ms);
            }

            EngineEvent::BacktestBarrier { done } => {
                let _ = done.send(());
            }
        }

        self.publish_view();
    }

    // ============================================================
    // EXECUTION EVENTS
    // ============================================================

    /// Processes canonical execution facts reported by an execution adapter.
    ///
    /// Live venues and the future historical execution simulator should both
    /// emit these same events.
    pub fn on_exec_event(
        &mut self,
        event: ExecEvent,
    ) {
        match event {
            ExecEvent::OrderAccepted { order_id, venue_order_id, ts_ms } => {
                self.handle_order_accepted(order_id, venue_order_id, ts_ms);
            }

            ExecEvent::OrderWorking { order_id, ts_ms } => {
                self.handle_order_working(order_id, ts_ms);
            }

            ExecEvent::Fill { fill } => {
                self.handle_exec_fill(fill);
            }

            ExecEvent::OrderCanceled { order_id, ts_ms } => {
                self.handle_order_canceled(order_id, ts_ms);
            }

            ExecEvent::OrderExpired { order_id, ts_ms } => {
                self.handle_order_expired(order_id, ts_ms);
            }

            ExecEvent::OrderRejected { order_id, reason, ts_ms } => {
                self.handle_order_rejected(order_id, reason, ts_ms);
            }

            ExecEvent::OrderReplaced { order_id, venue_order_id, ts_ms } => {
                self.handle_order_replaced(order_id, venue_order_id, ts_ms);
            }

            ExecEvent::Error { order_id, message, ts_ms } => {
                self.handle_exec_error(order_id, message, ts_ms);
            }
        }

        self.publish_view();
    }
}