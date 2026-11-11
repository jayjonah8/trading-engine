use crate::market::timeframe::Timeframe;


// ====================================================
// PRICE LEVELS DATA STRUCTURES
// ====================================================
//
// Defines the data structures used to represent detected
// support and resistance levels within the Price Levels toolbox.
//
// Each detected level contains:
// - Representative price of the support or resistance zone.
// - Upper and lower boundaries defining the price zone.
// - Support, resistance, or combined level classification.
// - Normalized significance score ranging from 0 to 100.
// - Number of independent price reactions.
// - Average reversal magnitude following price reactions.
// - Relative trading volume associated with reactions.
// - Timestamp of the most recent confirmed reaction.
//
// Provides helper methods for retrieving important levels,
// including the nearest support, nearest resistance,
// strongest level, and all levels above or below a given price.
//
// These structures allow trading strategies to evaluate
// market structure and nearby price barriers before
// submitting trade orders.
//
// TODO: Extend level metadata to support multi-timeframe
// confluence, breakouts, retests, and level invalidation.


// ====================================================
// PRICE LEVEL TYPE
// ====================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriceLevelType {
    Support,
    Resistance,
    SupportResistance,
}

// ====================================================
// PRICE LEVEL
// ====================================================

#[derive(Debug, Clone)]
pub struct PriceLevel {
    // Representative price of the detected zone.
    pub price: f64,

    // Lower and upper boundaries of the zone.
    pub lower_bound: f64,
    pub upper_bound: f64,

    // Classification relative to current market price.
    pub level_type: PriceLevelType,

    // Normalized significance score (0-100).
    pub strength: f64,

    // Number of independent swing reactions.
    pub touches: usize,

    // Average reversal magnitude of the reactions.
    pub average_reversal_pct: f64,

    // Average relative volume at the reactions.
    pub relative_volume: f64,

    // Most recent confirmed reaction timestamp.
    pub last_touch_ts: i64,
}

// ====================================================
// PRICE LEVELS
// ====================================================

#[derive(Debug, Clone)]
pub struct PriceLevels {
    pub symbol: String,
    pub timeframe: Timeframe,
    pub calculated_ts: i64,
    pub levels: Vec<PriceLevel>,
}

impl PriceLevels {

    // ====================================================
    // NEAREST SUPPORT
    // ====================================================

    pub fn nearest_support(
        &self,
        current_price: f64,
    ) -> Option<&PriceLevel> {

        self.levels
            .iter()
            .filter(|level| {
                level.upper_bound < current_price
            })
            .max_by(|a, b| {
                a.upper_bound.total_cmp(&b.upper_bound)
            })
    }

    // ====================================================
    // NEAREST RESISTANCE
    // ====================================================

    pub fn nearest_resistance(
        &self,
        current_price: f64,
    ) -> Option<&PriceLevel> {

        self.levels
            .iter()
            .filter(|level| {
                level.lower_bound > current_price
            })
            .min_by(|a, b| {
                a.lower_bound.total_cmp(&b.lower_bound)
            })
    }

    // ====================================================
    // STRONGEST LEVEL
    // ====================================================

    pub fn strongest_level(&self) -> Option<&PriceLevel> {

        self.levels
            .iter()
            .max_by(|a, b| {
                a.strength.total_cmp(&b.strength)
            })
    }

    // ====================================================
    // LEVELS ABOVE PRICE
    // ====================================================

    pub fn levels_above(
        &self,
        current_price: f64,
    ) -> Vec<&PriceLevel> {

        let mut levels: Vec<&PriceLevel> = self.levels
            .iter()
            .filter(|level| {
                level.lower_bound > current_price
            })
            .collect();

        levels.sort_by(|a, b| {
            a.price.total_cmp(&b.price)
        });

        levels
    }

    // ====================================================
    // LEVELS BELOW PRICE
    // ====================================================

    pub fn levels_below(
        &self,
        current_price: f64,
    ) -> Vec<&PriceLevel> {

        let mut levels: Vec<&PriceLevel> = self.levels
            .iter()
            .filter(|level| {
                level.upper_bound < current_price
            })
            .collect();

        levels.sort_by(|a, b| {
            b.price.total_cmp(&a.price)
        });

        levels
    }
}