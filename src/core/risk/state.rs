use crate::core::risk::{
    limits::RiskLimits,
    reservations::RiskReservations,
};

/// Authoritative risk state owned by the trading engine.
///
/// `RiskState` combines:
///
/// - configured risk policy through `RiskLimits`
/// - temporary committed capacity through `RiskReservations`
///
/// Actual positions and trades are intentionally not duplicated here.
/// Risk checks should inspect the engine's authoritative `Positions`,
/// `Trades`, and execution state alongside this risk state.
///
/// This keeps each piece of engine state responsible for one source of truth.
#[derive(Debug)]
pub struct RiskState {
    /// Configured limits enforced by the engine.
    pub limits: RiskLimits,

    /// Capacity currently reserved by active execution orders.
    pub reservations: RiskReservations,
}

impl RiskState {
    /// Creates risk state using the provided engine risk limits.
    pub fn new(limits: RiskLimits) -> Self {
        Self {
            limits,
            reservations: RiskReservations::new(),
        }
    }

    /// Returns the total notional currently reserved for entry orders.
    pub fn reserved_entry_notional(&self) -> f64 {
        self.reservations.total_entry_notional()
    }

    /// Returns the number of active risk reservations.
    pub fn active_reservation_count(&self) -> usize {
        self.reservations.len()
    }
}

impl Default for RiskState {
    fn default() -> Self {
        Self::new(RiskLimits::default())
    }
}