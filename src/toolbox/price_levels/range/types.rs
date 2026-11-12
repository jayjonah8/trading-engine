
#![allow(dead_code)]

use crate::market::timeframe::Timeframe;

// ====================================================
// RANGE STATUS
// ====================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeStatus {
    Active,
    BrokenAbove,
    BrokenBelow,
}

// ====================================================
// PRICE RANGE
// ====================================================

#[derive(Debug, Clone)]
pub struct PriceRange {
    pub lower_boundary: f64,
    pub upper_boundary: f64,

    pub lowest_price: f64,
    pub highest_price: f64,

    pub midpoint: f64,
    pub width_pct: f64,

    pub support_strength: f64,
    pub resistance_strength: f64,

    pub support_touches: usize,
    pub resistance_touches: usize,

    pub completed_oscillations: usize,
    pub containment_pct: f64,

    pub strength: f64,

    pub status: RangeStatus,

    pub start_ts: i64,
    pub end_ts: i64,
    pub confirmed_ts: i64,
    pub last_touch_ts: i64,
}

// ====================================================
// PRICE RANGES
// ====================================================

#[derive(Debug, Clone)]
pub struct PriceRanges {
    pub symbol: String,
    pub timeframe: Timeframe,
    pub calculated_ts: i64,
    pub ranges: Vec<PriceRange>,
}



