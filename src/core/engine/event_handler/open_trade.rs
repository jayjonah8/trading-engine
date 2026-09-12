use crate::core::{
    engine::engine::Engine,
    trade_intent::TradeIntent,
};

impl Engine {
    /// Handles a strategy request to create a new directional trade.
    ///
    /// TradeIntent
    ///     -> validation
    ///     -> engine/risk checks
    ///     -> quantity calculation
    ///     -> Trade + Order ID allocation
    ///     -> Trade creation (Opening)
    ///     -> RiskReservation creation
    ///     -> Order creation (PendingSubmit)
    ///     -> authoritative state insertion
    ///     -> ExecCommand::SubmitOrder
    pub fn handle_open_trade(
        &mut self,
        intent: TradeIntent,
    ) {
        // TODO: Implement open-trade lifecycle.
    }
}