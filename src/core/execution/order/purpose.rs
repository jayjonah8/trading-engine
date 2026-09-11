#![allow(dead_code)]

/// Describes why an order exists from the engine's perspective.
///
/// `OrderPurpose` is engine-owned metadata and is not necessarily sent to the
/// execution venue. It allows the engine to understand the role of an order
/// without encoding concepts such as "open" or "close" into `ExecEvent`.
///
/// This is especially important because multiple orders may exist for the same
/// symbol and trade at the same time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderPurpose {
    /// Opens or increases a strategy trade.
    Entry,

    /// Closes or reduces a strategy trade.
    Exit,

    /// Protective stop-loss order.
    StopLoss,

    /// Profit-taking order.
    TakeProfit,

    /// Emergency risk-reduction or liquidation order.
    EmergencyExit,

    /// Market-making or other liquidity-providing quote.
    Quote,

    /// Reduces or offsets exposure created elsewhere.
    Hedge,
}