#![allow(dead_code)]

// ============================================================
// TRAILING STOP LOSS
// ============================================================
//
// Engine-managed trailing stop-loss protection.
//
// Supports:
//
// - Long and short trades.
// - Fixed trailing stops.
// - Dynamic trailing stops with unlimited PnL thresholds.
// - Automatic tightening as maximum unrealized PnL increases.
// - Independent trailing-stop state for every Trade.
// - Live, Paper, Simulation, and Backtest execution.
//
// All percentages represent unleveraged price movements
// relative to the Trade's confirmed average entry price.
//
// Trailing stops are evaluated using canonical market-price
// events rather than exchange-native trailing orders.
//
// The engine maintains the highest favorable price reached
// by a Long trade or the lowest favorable price reached by
// a Short trade.
//
// As maximum unrealized PnL crosses configured thresholds,
// the trailing distance can tighten.
//
// The protective stop only moves in the direction that
// reduces risk. It never loosens.
//
// When the stop is breached, the engine requests a canonical
// CloseTrade event using market execution.
//
// ============================================================


// ============================================================
// TRAILING STOP LEVEL
// ============================================================

/// Defines the trailing distance activated after reaching
/// a specified maximum unrealized PnL percentage.
#[derive(Debug, Clone)]
pub struct TrailingStopLevel {
    /// Maximum unrealized PnL threshold required to activate
    /// this trailing distance.
    pub profit_percent: f64,

    /// Maximum permitted price reversal from the most
    /// favorable price reached.
    pub trail_percent: f64,
}


// ============================================================
// TRAILING STOP CONFIG
// ============================================================

/// Strategy-defined trailing-stop configuration.
///
/// One level creates a conventional trailing stop.
///
/// Multiple levels create a dynamic trailing stop that
/// progressively tightens as the trade becomes profitable.
#[derive(Debug, Clone)]
pub struct TrailingStopConfig {
    /// Ordered trailing-stop levels.
    ///
    /// Thresholds are expressed as unleveraged trade PnL %.
    pub levels: Vec<TrailingStopLevel>,
}


// ============================================================
// TRAILING STOP STATE
// ============================================================

/// Mutable trailing-stop state belonging to one logical Trade.
///
/// Each Trade independently tracks its own favorable price,
/// maximum unrealized PnL, and protective stop.
#[derive(Debug, Clone)]
pub struct TrailingStopState {
    /// Strategy-defined trailing-stop configuration.
    pub config: TrailingStopConfig,

    /// Highest favorable price for Long trades.
    ///
    /// Lowest favorable price for Short trades.
    pub best_px: Option<f64>,

    /// Highest unrealized PnL percentage observed.
    pub max_pnl_percent: f64,

    /// Current protective stop price.
    ///
    /// Long: stop increases as price rises.
    ///
    /// Short: stop decreases as price falls.
    pub stop_px: Option<f64>,
}



// ============================================================
// TRAILING STOP CONFIG VALIDATION
// ============================================================

impl TrailingStopConfig {

    /// Validates a strategy-defined trailing-stop configuration.
    ///
    /// Supports:
    ///
    /// - Fixed trailing stops with one level.
    /// - Dynamic trailing stops with multiple levels.
    /// - Immediate activation at 0% PnL.
    /// - Delayed activation at a positive PnL threshold.
    ///
    /// All percentages represent unleveraged price movements.
    ///
    /// Validation failures return descriptive errors.
    /// They do not panic or terminate the engine.
    pub fn validate(&self) -> Result<(), String> {

        // ========================================================
        // VALIDATE LEVEL COUNT
        // ========================================================

        // At least one trailing-stop level is required.
        //
        // An empty configuration cannot establish a
        // protective stop or calculate trailing distances.
        if self.levels.is_empty() {
            return Err(
                "Trailing stop requires at least one level".to_string()
            );
        }


        // ========================================================
        // VALIDATE INDIVIDUAL LEVELS
        // ========================================================

        for (index, level) in self.levels.iter().enumerate() {

            // ====================================================
            // VALIDATE FINITE PROFIT THRESHOLD
            // ====================================================

            // Reject NaN and positive/negative infinity.
            //
            // Non-finite thresholds cannot be reliably compared
            // against the maximum unrealized PnL.
            if !level.profit_percent.is_finite() {
                return Err(
                    format!(
                        "Trailing stop level {} has invalid profit_percent={}",
                        index,
                        level.profit_percent,
                    )
                );
            }


            // ====================================================
            // VALIDATE NONNEGATIVE PROFIT THRESHOLD
            // ====================================================

            // Important: The first activation threshold is allowed to be greater than zero. 
            // That lets a strategy activate trailing protection only after reaching a specified profit.

            // Profit activation thresholds must be >= 0%.
            //
            // 0%:
            //     Activates trailing protection immediately.
            //
            // Positive:
            //     Activates after reaching the specified
            //     maximum favorable PnL percentage.
            if level.profit_percent < 0.0 {
                return Err(
                    format!(
                        "Trailing stop level {} has negative profit_percent={}",
                        index,
                        level.profit_percent,
                    )
                );
            }


            // ====================================================
            // VALIDATE FINITE TRAILING DISTANCE
            // ====================================================

            // Reject NaN and positive/negative infinity.
            //
            // Non-finite trailing distances could produce
            // invalid protective stop prices.
            if !level.trail_percent.is_finite() {
                return Err(
                    format!(
                        "Trailing stop level {} has invalid trail_percent={}",
                        index,
                        level.trail_percent,
                    )
                );
            }


            // ====================================================
            // VALIDATE POSITIVE TRAILING DISTANCE
            // ====================================================

            // A trailing distance must be greater than 0%.
            //
            // Zero or negative distances could place the stop
            // at or beyond the most favorable price.
            if level.trail_percent <= 0.0 {
                return Err(
                    format!(
                        "Trailing stop level {} must have trail_percent > 0, got {}",
                        index,
                        level.trail_percent,
                    )
                );
            }


            // ====================================================
            // VALIDATE MAXIMUM TRAILING DISTANCE
            // ====================================================

            // Trailing distances must remain below 100%.
            //
            // For Long trades, distances >= 100% would
            // produce a zero or negative protective stop.
            //
            // Apply the same restriction to Short trades
            // for consistent configuration validation.
            if level.trail_percent >= 100.0 {
                return Err(
                    format!(
                        "Trailing stop level {} must have trail_percent < 100, got {}",
                        index,
                        level.trail_percent,
                    )
                );
            }


            // ====================================================
            // VALIDATE LEVEL ORDERING
            // ====================================================

            // The first level has no previous level.
            //
            // Ordering and tightening checks begin
            // with the second configured level.
            if index == 0 {
                continue;
            }

            let previous = &self.levels[index - 1];


            // ====================================================
            // VALIDATE INCREASING PROFIT THRESHOLDS
            // ====================================================

            // Activation thresholds must strictly increase.
            //
            // Valid:
            //     0%, 1%, 2%, 3%
            //
            // Invalid:
            //     0%, 2%, 1%, 3%
            //
            // Duplicate thresholds are also rejected.
            if level.profit_percent <= previous.profit_percent {
                return Err(
                    format!(
                        "Trailing stop level {} profit_percent={} must be greater than previous level profit_percent={}",
                        index,
                        level.profit_percent,
                        previous.profit_percent,
                    )
                );
            }


            // ====================================================
            // VALIDATE NON-INCREASING TRAILING DISTANCES
            // ====================================================

            // Trailing distances must decrease or remain equal.
            //
            // Valid:
            //     1.0%, 0.9%, 0.75%
            //
            // Also valid:
            //     1.0%, 1.0%, 0.75%
            //
            // Invalid:
            //     1.0%, 1.5%, 0.75%
            //
            // This prevents later profit levels from
            // requesting a wider trailing distance.
            if level.trail_percent > previous.trail_percent {
                return Err(
                    format!(
                        "Trailing stop level {} trail_percent={} cannot exceed previous level trail_percent={}",
                        index,
                        level.trail_percent,
                        previous.trail_percent,
                    )
                );
            }
        }


        // ========================================================
        // VALID CONFIGURATION
        // ========================================================

        Ok(())
    }
}