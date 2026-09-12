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
    config::Config, engine::{
        shared::{
            get_execution_mode,
            verify_engine_config,
        },
        state::{
            EngineState,
            ExecutionMode,
        },
    }, engine_view::EngineView, execution::{
        ExecCommand,
        ExecEvent,
        OrderChanges,
        OrderId,
    }, general::General, risk::RiskLimits, trade_intent::TradeIntent,
};

use crate::notifications::env_helpers::validate_notification_env;

/// Current high-level operating phase of the engine.
///
/// TODO: Expand this later if the engine gains explicit startup,
/// reconciliation, shutdown, or recovery phases.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradingPhase {
    Normal,
}

/// Most recent market price known by the engine for a symbol.
///
/// TODO: Replace primitive types with canonical `Symbol` and `Price`.
#[derive(Debug, Clone, Copy)]
pub struct LastPrice {
    pub px: f64,
    pub ts_ms: i64,
}

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
    CancelOrder { order_id: OrderId, reason: String },

    /// Requests replacement/modification of an active engine order.
    ReplaceOrder { order_id: OrderId, quantity: Option<f64>, limit_px: Option<f64>, stop_px: Option<f64>, reason: String },

    /// Requests closure of one strategy trade associated with a symbol.
    ///
    /// TODO: Eventually prefer `TradeId` once strategies and EngineView expose
    /// canonical trade identity directly.
    CloseSingleTrade { symbol: String, reason: String },

    /// Requests closure of all active directional trades.
    CloseAllTrades { reason: String },

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

        // TODO:
        // Move this construction into the Config
        let risk_limits = RiskLimits {
            max_active_trades: Some(cfg.max_gross_positions),
            max_order_notional: None,
            max_gross_notional: None,
            max_symbol_notional: None,
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
    ///
    /// TODO:
    /// Update `EngineView::from_parts()` so it consumes canonical EngineState
    /// directly instead of depending on legacy state structures.
    fn publish_view(&self) {
        let view = EngineView::from_parts(
            &self.cfg,
            &self.general,
            &self.state,
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
                // self.handle_open_trade(intent);
            }

            EngineEvent::CancelOrder { order_id, reason } => {
                // self.handle_cancel_order(order_id, reason);
            }

            EngineEvent::ReplaceOrder { order_id, quantity, limit_px, stop_px, reason } => {
                let changes = OrderChanges { quantity, limit_px, stop_px };
                // self.handle_replace_order(order_id, changes, reason);
            }

            EngineEvent::CloseSingleTrade { symbol, reason } => {
                // self.handle_close_single_trade(symbol, reason);
            }

            EngineEvent::CloseAllTrades { reason } => {
                // self.handle_close_all_trades(reason);
            }

            EngineEvent::PriceTick { symbol, px, ts_ms } => {
                self.last_px.insert(symbol.clone(), LastPrice { px, ts_ms });
                // self.check_risk_for_symbol(&symbol, px, ts_ms);
            }

            EngineEvent::PriceTickBacktest { symbol, px, ts_ms } => {
                let sym = symbol.clone();

                self.last_px.insert(symbol, LastPrice { px, ts_ms });
                // self.check_risk_for_backtest_symbol(&sym, px, ts_ms);
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
            ExecEvent::OrderAccepted { .. } => {
                // self.handle_order_accepted(event);
            }

            ExecEvent::OrderWorking { .. } => {
                // self.handle_order_working(event);
            }

            ExecEvent::Fill { .. } => {
                // self.handle_exec_fill(event);
            }

            ExecEvent::OrderCanceled { .. } => {
                // self.handle_order_canceled(event);
            }

            ExecEvent::OrderExpired { .. } => {
                // self.handle_order_expired(event);
            }

            ExecEvent::OrderRejected { .. } => {
                // self.handle_order_rejected(event);
            }

            ExecEvent::OrderReplaced { .. } => {
                // self.handle_order_replaced(event);
            }

            ExecEvent::Error { .. } => {
                // self.handle_exec_error(event);
            }
        }

        self.publish_view();
    }
}