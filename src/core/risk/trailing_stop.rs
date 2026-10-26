#![allow(dead_code)]

use crate::core::trades::Direction;


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




// ============================================================
// TRAILING STOP RUNTIME STATE IMPLEMENTATION 
// ============================================================

impl TrailingStopState {

    // ========================================================
    // INITIALIZE TRAILING STOP STATE
    // ========================================================

    /// Initializes trailing-stop protection using the
    /// Trade's confirmed average entry price.
    ///
    /// A 0% activation threshold enables protection
    /// immediately.
    ///
    /// A positive first threshold delays activation
    /// until sufficient unrealized profit is reached.
    pub fn new(
        config: TrailingStopConfig,
        direction: Direction,
        entry_px: f64,
    ) -> Result<Self, String> {

        // ====================================================
        // VALIDATE CONFIGURATION
        // ====================================================

        config.validate()?;

        // ====================================================
        // VALIDATE ENTRY PRICE
        // ====================================================

        if !entry_px.is_finite() || entry_px <= 0.0 {
            return Err(format!(
                "Invalid trailing-stop entry price: {}",
                entry_px,
            ));
        }

        // ====================================================
        // INITIALIZE STATE
        // ====================================================

        let mut state = Self {
            config,
            best_px: Some(entry_px),
            max_pnl_percent: 0.0,
            stop_px: None,
        };

        // ====================================================
        // ACTIVATE INITIAL STOP WHEN APPLICABLE
        // ====================================================

        state.advance_stop(direction, entry_px, entry_px)?;

        Ok(state)
    }


    // ========================================================
    // PROCESS MARKET PRICE
    // ========================================================

    /// Processes a canonical market-price event.
    ///
    /// Returns:
    ///
    /// Ok(true):
    ///     The existing trailing stop was breached.
    ///
    /// Ok(false):
    ///     No trailing-stop exit is required.
    ///
    /// Err(...):
    ///     Invalid price or trailing-stop state.
    ///
    /// The existing protective stop is checked BEFORE
    /// updating the favorable-price watermark.
    ///
    /// A triggered stop does not update state.
    pub fn update(
        &mut self,
        direction: Direction,
        entry_px: f64,
        market_px: f64,
    ) -> Result<bool, String> {

        // ====================================================
        // VALIDATE ENTRY PRICE
        // ====================================================

        if !entry_px.is_finite() || entry_px <= 0.0 {
            return Err(format!(
                "Invalid trailing-stop entry price: {}",
                entry_px,
            ));
        }

        // ====================================================
        // VALIDATE MARKET PRICE
        // ====================================================

        if !market_px.is_finite() || market_px <= 0.0 {
            return Err(format!(
                "Invalid trailing-stop market price: {}",
                market_px,
            ));
        }

        // ====================================================
        // VALIDATE FAVORABLE PRICE WATERMARK
        // ====================================================

        let best_px = self.best_px.ok_or_else(|| {
            "Trailing-stop state has no favorable-price watermark".to_string()
        })?;

        if !best_px.is_finite() || best_px <= 0.0 {
            return Err(format!(
                "Invalid trailing-stop favorable price: {}",
                best_px,
            ));
        }

        // ====================================================
        // CHECK EXISTING STOP BEFORE UPDATING STATE
        // ====================================================

        if let Some(stop_px) = self.stop_px {

            if !stop_px.is_finite() || stop_px <= 0.0 {
                return Err(format!(
                    "Invalid trailing-stop price: {}",
                    stop_px,
                ));
            }

            let breached = match direction {
                Direction::Long => market_px <= stop_px,
                Direction::Short => market_px >= stop_px,
            };

            if breached {
                return Ok(true);
            }
        }

        // ====================================================
        // UPDATE FAVORABLE PRICE AND PROTECTIVE STOP
        // ====================================================

        self.advance_stop(direction, entry_px, market_px)?;

        Ok(false)
    }


    // ========================================================
    // ADVANCE FAVORABLE PRICE AND PROTECTIVE STOP
    // ========================================================

    /// Updates the most favorable price, maximum
    /// unrealized PnL, and protective stop.
    ///
    /// This method does not evaluate stop breaches.
    ///
    /// Callers should use update() for market events
    /// so an existing stop is checked first.
    fn advance_stop(
        &mut self,
        direction: Direction,
        entry_px: f64,
        market_px: f64,
    ) -> Result<(), String> {

        // ====================================================
        // CALCULATE FAVORABLE PRICE WATERMARK
        // ====================================================

        let previous_best = self.best_px.ok_or_else(|| {
            "Trailing-stop state has no favorable-price watermark".to_string()
        })?;

        let best_px = match direction {
            Direction::Long => previous_best.max(market_px),
            Direction::Short => previous_best.min(market_px),
        };

        // ====================================================
        // CALCULATE MAXIMUM UNREALIZED PNL
        // ====================================================

        let pnl_percent = match direction {
            Direction::Long => {
                ((best_px - entry_px) / entry_px) * 100.0
            }

            Direction::Short => {
                ((entry_px - best_px) / entry_px) * 100.0
            }
        };

        if !pnl_percent.is_finite() {
            return Err(
                "Trailing-stop PnL calculation produced a non-finite value"
                    .to_string()
            );
        }

        let max_pnl_percent = self.max_pnl_percent.max(pnl_percent);

        // ====================================================
        // SELECT ACTIVE TRAILING LEVEL
        // ====================================================

        // Select the highest configured profit threshold
        // reached by the Trade.
        //
        // If no threshold has been reached, trailing-stop
        // protection remains inactive.

        let active_level = self
            .config
            .levels
            .iter()
            .rev()
            .find(|level| {
                max_pnl_percent >= level.profit_percent
            });

        // ====================================================
        // CALCULATE PROTECTIVE STOP
        // ====================================================

        let next_stop_px = if let Some(level) = active_level {

            let trail_fraction = level.trail_percent / 100.0;

            let candidate = match direction {
                Direction::Long => {
                    best_px * (1.0 - trail_fraction)
                }

                Direction::Short => {
                    best_px * (1.0 + trail_fraction)
                }
            };

            // ====================================================
            // VALIDATE CALCULATED STOP PRICE
            // ====================================================

            if !candidate.is_finite() || candidate <= 0.0 {
                return Err(format!(
                    "Trailing-stop calculation produced invalid stop price: {}",
                    candidate,
                ));
            }

            // ====================================================
            // PREVENT PROTECTIVE STOP FROM LOOSENING
            // ====================================================

            // Long:
            //     Stop price may only increase.
            //
            // Short:
            //     Stop price may only decrease.

            Some(match self.stop_px {
                Some(previous_stop) => match direction {
                    Direction::Long => previous_stop.max(candidate),
                    Direction::Short => previous_stop.min(candidate),
                },

                None => candidate,
            })

        } else {

            // ====================================================
            // TRAILING STOP NOT YET ACTIVATED
            // ====================================================

            self.stop_px
        };

        // ====================================================
        // COMMIT UPDATED TRAILING STOP STATE
        // ====================================================

        // State is committed only after calculations
        // and stop-price validation succeed.

        self.best_px = Some(best_px);
        self.max_pnl_percent = max_pnl_percent;
        self.stop_px = next_stop_px;

        Ok(())
    }
}