#![allow(dead_code)]

use crate::core::trades::{
    direction::Direction,
    id::TradeId,
    state::TradeState,
};

use crate::core::risk::TrailingStopState;


// Trade should represent the strategy lifecycle, 
// while orders/fills/positions handle execution state.

/// Logical strategy-level representation of a directional trade.
///
/// A `Trade` represents the lifecycle and risk plan of a strategy trade.
/// It does NOT represent an exchange order or the engine's authoritative
/// venue position.
///
/// Execution details belong to:
///
/// - `Order`    -> what the engine asked a venue to execute
/// - `Fill`     -> what actually executed
/// - `Position` -> actual venue/account exposure
/// - `Trade`    -> why the strategy holds that exposure and how it manages it
///
/// A single trade may create multiple orders throughout its lifetime.
///
/// TODO: Replace temporary primitive types with canonical strongly typed values:
///
/// - `String` strategy ID -> `StrategyId`
/// - `String` symbol -> `Symbol`
/// - `f64` quantity -> `Quantity`
/// - `f64` prices -> `Price`
/// - `f64` monetary values -> `Money`
#[derive(Debug, Clone)]
pub struct Trade {
    /// Internal engine-owned trade identifier.
    pub id: TradeId,

    /// Strategy that owns this trade.
    ///
    /// TODO: Replace with `StrategyId`.
    pub strategy_id: String,

    /// Instrument being traded.
    ///
    /// TODO: Replace with canonical `Symbol`.
    pub symbol: String,

    /// Directional exposure intended by the strategy.
    pub direction: Direction,

    /// Current logical trade lifecycle state.
    pub state: TradeState,

    /// Current remaining open quantity for this trade.
    ///
    /// This value is trade-specific and is always represented as a positive
    /// magnitude. Direction is stored separately.
    ///
    /// Confirmed entry fills increase this quantity. Confirmed exit fills decrease
    /// it until the trade reaches zero and is fully closed.
    ///
    /// This value also represents the previously entered quantity while the trade
    /// is being opened, allowing it to be used when calculating entry VWAP.
    /// Once exits begin, it represents only the remaining open quantity and must
    /// NOT be used as the historical weight for exit VWAP. `exited_quantity`
    /// tracks that separately.
    ///
    ///     quantity        = current remaining OPEN quantity
    ///     exited_quantity = cumulative quantity already EXITED
    ///
    /// TODO: Replace with `Quantity`.
    pub quantity: f64,

    /// Total quantity that has been exited through confirmed fills.
    ///
    /// This is cumulative and is used to calculate the volume-weighted average
    /// exit execution price across partial fills and multiple exit orders.
    ///
    /// Unlike `quantity`, which decreases as the trade is exited, this value
    /// only increases as confirmed exit fills are applied.
    ///
    /// quantity        = currently OPEN quantity
    /// exited_quantity = total quantity already EXITED
    ///
    /// TODO: Replace with `Quantity`.
    pub exited_quantity: f64,

    /// Average entry execution price accumulated from confirmed entry fills.
    ///
    /// TODO: Replace with `Option<Price>`.
    pub avg_entry_px: Option<f64>,

    /// Average exit execution price accumulated from confirmed exit fills.
    ///
    /// TODO: Replace with `Option<Price>`.
    pub avg_exit_px: Option<f64>,

    /// Strategy market price when the entry intent was created.
    ///
    /// TODO: Replace with `Price`.
    pub entry_signal_px: f64,

    /// Market price that triggered the strategy's exit decision.
    ///
    /// TODO: Replace with `Option<Price>`.
    pub exit_signal_px: Option<f64>,

    /// Requested notional size of the trade.
    ///
    /// TODO: Replace with `Money`.
    pub target_notional: f64,

    /// Strategy-defined capital allocated to this trade.
    ///
    /// Used for basket profitability and early profit taking.
    /// Does not represent actual venue margin requirements.
    ///
    /// TODO: Adjust proportionally for partial fills and exits.
    pub margin_usd: f64,

    /// Optional take-profit percentage.
    pub take_profit_percent: Option<f64>,

    /// Optional stop-loss percentage.
    pub stop_loss_percent: Option<f64>,

    /// Optional emergency stop percentage.
    pub emergency_stop_percent: Option<f64>,

    /// Optional delay before the normal stop-loss becomes active.
    pub stop_loss_delay_secs: Option<u64>,

    /// Optional maximum lifetime of the trade.
    ///
    /// `None` means no duration-based exit is configured.
    pub trade_duration_secs: Option<u64>,

    /// Optional engine-managed trailing-stop state.
    ///
    /// None:
    ///     Trailing-stop protection is disabled.
    ///
    /// Some(state):
    ///     This Trade maintains its own independent:
    ///
    ///     - Trailing-stop configuration.
    ///     - Most favorable market price.
    ///     - Maximum unrealized PnL percentage.
    ///     - Current protective stop price.
    ///
    /// Trailing-stop state is initialized using the
    /// confirmed average entry price.
    ///
    /// Supports both fixed and dynamically tightening
    /// trailing stops for Long and Short trades.
    pub trailing_stop: Option<TrailingStopState>,

    /// Total fees accumulated across confirmed entry fills.
    /// Includes all partial entry executions.
    /// Negative values are permitted for maker rebates.
    pub entry_fees: f64,

    /// Total fees accumulated across confirmed exit fills.
    /// Includes all partial exit executions.
    /// Negative values are permitted for maker rebates.
    pub exit_fees: f64,

    /// Entry fees already allocated to realized PnL.
    /// Entry fees are allocated proportionally as quantities exit.
    /// This prevents double-counting entry fees across
    /// multiple partial exits.
    pub allocated_entry_fees: f64,

    /// Serializable timestamp when the trade was created.
    pub created_ts_ms: i64,

    /// Timestamp of the first confirmed entry fill.
    pub opened_ts_ms: Option<i64>,

    /// Timestamp when the final remaining exposure was closed.
    pub closed_ts_ms: Option<i64>,
}




impl Trade {

    // ============================================================
    // CALCULATE UNREALIZED PNL
    // ============================================================

    /// Calculates a single trades gross unrealized PnL using the 
    /// latest market price.
    ///
    /// Supports:
    /// - Long trades
    /// - Short trades
    /// - Partial fills
    /// - Partial exits
    /// - Multiple trades on the same symbol
    ///
    /// Uses remaining open quantity.
    ///
    /// Does not deduct execution fees or estimated closing costs.
    pub fn unrealized_pnl(&self, current_px: f64) -> f64 {
        let Some(entry_px) = self.avg_entry_px else {
            return 0.0;
        };

        match self.direction {
            Direction::Long => {
                (current_px - entry_px) * self.quantity
            }

            Direction::Short => {
                (entry_px - current_px) * self.quantity
            }
        }
    }


    // ============================================================
    // CALCULATE NET UNREALIZED PNL
    // ============================================================

    /// Calculates a single trade's net unrealized PnL.
    ///
    /// Net Unrealized PnL:
    ///
    ///     Gross Unrealized PnL
    ///     - Remaining Entry Fees
    ///     - Estimated Exit Fees
    ///
    /// Remaining entry fees exclude fees already allocated
    /// to realized PnL through partial exits.
    ///
    /// Estimated exit fees are assumed equal to the remaining
    /// entry fees.
    ///
    /// Supports:
    /// - Long and short trades
    /// - Partial fills
    /// - Partial exits
    /// - Multiple trades on the same symbol
    ///
    /// Does not include funding or slippage.
    pub fn net_unrealized_pnl(&self, current_px: f64) -> f64 {
        // Gross unrealized PnL.
        let gross_pnl = self.unrealized_pnl(current_px);

        // Entry fees attributable to remaining open quantity.
        let remaining_entry_fees =
            (self.entry_fees - self.allocated_entry_fees)
                .max(0.0);

        // Estimate exit fees equal to remaining entry fees.
        let estimated_exit_fees = remaining_entry_fees;

        // Net unrealized PnL after estimated round-trip fees.
        gross_pnl
            - remaining_entry_fees
            - estimated_exit_fees
    }
}

