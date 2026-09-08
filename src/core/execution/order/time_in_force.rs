/// Controls how long an order remains active and how unfilled quantity
/// should be handled by the execution venue.
///
/// This is intentionally separate from `OrderType`. For example, a limit
/// order may be submitted as GTC, IOC, FOK, or Day depending on the venue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimeInForce {
    /// Remains active until completely filled or explicitly canceled.
    Gtc,

    /// Immediately fills as much quantity as possible and cancels
    /// any remaining unfilled quantity.
    Ioc,

    /// Must immediately fill the entire quantity or the whole order
    /// is canceled.
    Fok,

    /// Remains active for the venue's current trading session and
    /// expires if not filled before the session ends.
    Day,
}