use std::sync::Arc;
use std::collections::HashMap;

use tokio::sync::{
    mpsc::UnboundedSender,
    watch,
};

use crate::core::{
    config::Config,
    engine::EngineEvent,
    engine_view::EngineView,
    general::General,
    trades::{Trades, PendingOpens, PendingCloses},
    exec::{ExecCommand}
};


// ============================================================
// MAIN ENGINE CONFIG
// ============================================================


#[derive(Debug)]
pub struct Engine {
    /// Global engine configuration.
    pub cfg: Config,

    /// General engine state and settings.
    pub general: General,

    /// Fully opened live trades.
    pub trades: Trades,

    /// Entry orders that have not received their final fill yet, keyed by open cid.
    pub pending_opens: PendingOpens,

    /// Exit orders that have not received their final fill yet, keyed by close cid.
    pub pending_closes: PendingCloses,

    /// Most recently observed market price for each symbol.
    pub last_px: HashMap<String, LastPrice>,

    /// Publishes read-only Engine snapshots.
    pub tx_view: watch::Sender<Arc<EngineView>>,

    /// Sends execution commands to the exchange adapter.
    pub tx_exec: UnboundedSender<ExecCommand>, 

    /// Lets the engine send messages back into its own event queue.
    pub tx_engine: UnboundedSender<EngineEvent>,

    /// Tracks which strategy owns each symbol.
    pub owner_by_symbol: HashMap<String, String>, 

    /// Stores the last decision timestamp for each symbol.
    pub last_decision_ts: HashMap<String, i64>,   

    /// Tracks the number of open trades for each symbol.
    pub open_count_by_symbol: HashMap<String, usize>,

    /// Controls whether execution is simulated, paper, or live.
    pub execution_mode: ExecutionMode,

    /// Tracks the current trading phase of the engine.
    pub trading_phase: TradingPhase,

    /// Master on/off switch for all engine print logs.
    pub debug_mode: bool,
}


// ============================================================
// CURRENT TRADING MODE
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    Live,
    Backtest, 
    Simulation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradingPhase {
    Normal,
    ClosingTrades 
}

// ============================================================
// last_px HASH VALUE
// ============================================================
#[derive(Debug, Clone, Copy)]
pub struct LastPrice {
    pub px: f64,
    pub ts_ms: i64,
}