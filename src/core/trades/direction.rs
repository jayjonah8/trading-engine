/// Direction of a strategy trade.
///
/// `Direction` is intentionally separate from execution `Side`.
///
/// For example, a long trade may use:
///
/// - `Side::Buy` to enter
/// - `Side::Sell` to exit
///
/// while its `Direction` remains `Long` throughout the entire lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    Long,
    Short,
}