/// Execution side of an order.
///
/// `Side` describes what the order does at the venue:
/// - `Buy` acquires/increases the asset.
/// - `Sell` disposes/decreases the asset.
///
/// This is intentionally separate from trade `Direction` (`Long` / `Short`).
/// An order has a side, while a strategy trade or position has a direction.
/// For example, closing a long trade requires a `Sell` order even though the
/// trade itself remains `Long`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    /// Returns the opposite execution side.
    pub const fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }
}