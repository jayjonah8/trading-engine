use tokio::sync::oneshot;

use crate::core::events::AppEventEnvelope;

/// A historical market event sent to a backtest strategy.
///
/// The strategy must acknowledge `done` after it has completely
/// processed the market event.
#[derive(Debug)]
pub struct BacktestStrategyEvent {
    pub env: AppEventEnvelope,
    pub done: oneshot::Sender<()>,
}