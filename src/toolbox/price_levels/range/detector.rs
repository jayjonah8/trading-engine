#![allow(dead_code)]

use std::collections::HashMap;

use crate::{
    core::events::Bar,
    market::{
        candle_history::CandleHistory,
        timeframe::Timeframe,
    },
    toolbox::price_levels::{
        types::{
            PriceLevel,
            PriceLevelType,
            PriceLevels,
        },
        range::{
            config::RangeConfig,
            types::{
                PriceRange,
                PriceRanges,
                RangeStatus,
            },
        },
    },
};


// ====================================================
// RANGE DETECTOR
// ====================================================
//
// Identifies established trading ranges using:
//
// 1. Existing support/resistance levels.
// 2. Completed historical OHLCV candles.
// 3. Confirmed boundary reversals.
// 4. Actual price movement between boundaries.
// 5. Historical range segmentation.
// 6. Price containment.
// 7. Observed price extremes.
// 8. Historical breakout classification.
//
// Supports historical and latest-candle analysis.
//
// Future:
// - Multiple independent timeframe configurations.
// - Persistent range lifecycle tracking.
// - Multi-timeframe range confluence.

pub struct RangeDetector {
    pub config: RangeConfig,

    cache: HashMap<(String, Timeframe), PriceRanges>,
}


// ====================================================
// BOUNDARY
// ====================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Boundary {
    Lower,
    Upper,
}


// ====================================================
// BOUNDARY REACTION
// ====================================================

#[derive(Debug, Clone, Copy)]
struct BoundaryReaction {
    boundary: Boundary,

    // Timestamp when the reversal was confirmed.
    timestamp_ms: i64,

    // Actual extreme observed during the reversal.
    extreme_price: f64,
}


// ====================================================
// REVERSAL STATE
// ====================================================

#[derive(Debug, Clone, Copy)]
struct ReversalState {
    boundary: Boundary,
    extreme_price: f64,
}


// ====================================================
// RANGE SEGMENT
// ====================================================

#[derive(Debug, Clone, Copy)]
struct RangeSegment {
    start_index: usize,
    end_index: usize,
}


// ====================================================
// RANGE DETECTOR IMPLEMENTATION
// ====================================================

impl RangeDetector {

    // ====================================================
    // NEW
    // ====================================================

    pub fn new(config: RangeConfig) -> Self {
        Self {
            config,
            cache: HashMap::new(),
        }
    }


    // ====================================================
    // UPDATE
    // ====================================================

    pub fn update(
        &mut self,
        symbol: &str,
        timeframe: Timeframe,
        current_ts: i64,
        candle_history: &CandleHistory,
        price_levels: &PriceLevels,
    ) -> Option<&PriceRanges> {

        let key = (symbol.to_string(), timeframe);

        if !self.config.enabled
            || timeframe != self.config.timeframe
        {
            self.cache.remove(&key);
            return None;
        }

        // ====================================================
        // VALIDATE PRICE LEVEL SNAPSHOT
        // ====================================================

        if price_levels.symbol != symbol
            || price_levels.timeframe != timeframe
            || price_levels.calculated_ts > current_ts
        {
            self.cache.remove(&key);
            return None;
        }

        // ====================================================
        // GET CANDLE HISTORY
        // ====================================================

        let Some(history) =
            candle_history.get(symbol, timeframe)
        else {
            self.cache.remove(&key);
            return None;
        };

        let lookback_ms = self
            .config
            .lookback_secs
            .saturating_mul(1000)
            .min(i64::MAX as u64) as i64;

        let cutoff_ts =
            current_ts.saturating_sub(lookback_ms);

        let mut bars: Vec<&Bar> = history
            .iter()
            .filter(|bar| {
                let ts = bar.end.timestamp_millis();

                ts >= cutoff_ts
                    && ts <= current_ts
                    && bar.high.is_finite()
                    && bar.low.is_finite()
                    && bar.close.is_finite()
                    && bar.low > 0.0
                    && bar.high >= bar.low
                    && bar.close >= bar.low
                    && bar.close <= bar.high
            })
            .collect();

        bars.sort_by_key(|bar| {
            bar.end.timestamp_millis()
        });

        let mut ranges = Vec::new();

        // ====================================================
        // GENERATE CANDIDATE RANGES
        // ====================================================

        if bars.len() >= 3 {

            for support in &price_levels.levels {

                if !matches!(
                    support.level_type,
                    PriceLevelType::Support
                        | PriceLevelType::SupportResistance
                ) {
                    continue;
                }

                if support.touches
                    < self.config.min_boundary_touches
                {
                    continue;
                }

                for resistance in &price_levels.levels {

                    if !matches!(
                        resistance.level_type,
                        PriceLevelType::Resistance
                            | PriceLevelType::SupportResistance
                    ) {
                        continue;
                    }

                    if resistance.touches
                        < self.config.min_boundary_touches
                    {
                        continue;
                    }

                    if support.price >= resistance.price {
                        continue;
                    }

                    let lower_boundary = support.price;
                    let upper_boundary = resistance.price;

                    if !lower_boundary.is_finite()
                        || !upper_boundary.is_finite()
                        || lower_boundary <= 0.0
                    {
                        continue;
                    }

                    let width_pct =
                        ((upper_boundary - lower_boundary)
                            / lower_boundary)
                            * 100.0;

                    if !width_pct.is_finite()
                        || width_pct
                            < self.config.min_range_width_pct
                        || width_pct
                            > self.config.max_range_width_pct
                    {
                        continue;
                    }

                    // ====================================================
                    // IDENTIFY HISTORICAL SEGMENTS
                    // ====================================================

                    let segments = self.find_range_segments(
                        &bars,
                        lower_boundary,
                        upper_boundary,
                    );

                    // ====================================================
                    // VALIDATE EACH SEGMENT
                    // ====================================================

                    for segment in segments {

                        let segment_bars = &bars[
                            segment.start_index
                                ..=segment.end_index
                        ];

                        if let Some(mut range) = self.validate_range(
                            segment_bars,
                            support,
                            resistance,
                            lower_boundary,
                            upper_boundary,
                            width_pct,
                            current_ts,
                        ) {

                            // ====================================================
                            // CLASSIFY RANGE STATUS
                            // ====================================================

                            range.status = self.classify_range_status(
                                &bars,
                                segment.end_index,
                                lower_boundary,
                                upper_boundary,
                            );

                            ranges.push(range);
                        }
                    }
                }
            }
        }

        // ====================================================
        // SORT AND LIMIT
        // ====================================================

        ranges.sort_by(|a, b| {
            b.strength.total_cmp(&a.strength)
        });

        ranges.truncate(self.config.max_ranges);

        // ====================================================
        // UPDATE CACHE
        // ====================================================

        self.cache.insert(
            key.clone(),
            PriceRanges {
                symbol: symbol.to_string(),
                timeframe,
                calculated_ts: current_ts,
                ranges,
            },
        );

        self.cache.get(&key)
    }


    // ====================================================
    // FIND HISTORICAL RANGE SEGMENTS
    // ====================================================

    // Separates periods of price containment from periods
    // where price closes beyond the permitted breakout buffer.
    //
    // Uses completed candle closes.
    //
    // Temporary intrabar wicks do not terminate a segment.

    fn find_range_segments(
        &self,
        bars: &[&Bar],
        lower_boundary: f64,
        upper_boundary: f64,
    ) -> Vec<RangeSegment> {

        let mut segments = Vec::new();

        if bars.is_empty() {
            return segments;
        }

        let buffer =
            self.config.boundary_break_buffer_pct / 100.0;

        if !buffer.is_finite() || buffer < 0.0 {
            return segments;
        }

        let lower_breakout =
            lower_boundary * (1.0 - buffer);

        let upper_breakout =
            upper_boundary * (1.0 + buffer);

        let mut segment_start: Option<usize> = None;

        // ====================================================
        // PROCESS CANDLES CHRONOLOGICALLY
        // ====================================================

        for (index, bar) in bars.iter().enumerate() {

            let inside =
                bar.close >= lower_breakout
                    && bar.close <= upper_breakout;

            if inside {

                if segment_start.is_none() {
                    segment_start = Some(index);
                }

            } else if let Some(start) =
                segment_start.take()
            {
                if index > start {
                    segments.push(RangeSegment {
                        start_index: start,
                        end_index: index - 1,
                    });
                }
            }
        }

        // ====================================================
        // PRESERVE FINAL SEGMENT
        // ====================================================

        if let Some(start) = segment_start {

            segments.push(RangeSegment {
                start_index: start,
                end_index: bars.len() - 1,
            });
        }

        segments
    }


    // ====================================================
    // CLASSIFY RANGE STATUS
    // ====================================================

    // Examines completed candles following the historical
    // range segment.
    //
    // The first close beyond the breakout buffer determines
    // the historical breakout direction.
    //
    // A subsequent return inside the original boundaries
    // does not reset the breakout classification.
    //
    // This is calculated independently for each segment.
    //
    // No persistent lifecycle state is required.

    fn classify_range_status(
        &self,
        bars: &[&Bar],
        segment_end_index: usize,
        lower_boundary: f64,
        upper_boundary: f64,
    ) -> RangeStatus {

        let buffer =
            self.config.boundary_break_buffer_pct / 100.0;

        if !buffer.is_finite() || buffer < 0.0 {
            return RangeStatus::Active;
        }

        let lower_breakout =
            lower_boundary * (1.0 - buffer);

        let upper_breakout =
            upper_boundary * (1.0 + buffer);

        // ====================================================
        // CHECK SUBSEQUENT COMPLETED CANDLES
        // ====================================================

        for bar in bars.iter().skip(segment_end_index + 1) {

            if bar.close > upper_breakout {
                return RangeStatus::BrokenAbove;
            }

            if bar.close < lower_breakout {
                return RangeStatus::BrokenBelow;
            }
        }

        // ====================================================
        // NO CONFIRMED BREAKOUT
        // ====================================================

        RangeStatus::Active
    }


    // ====================================================
    // VALIDATE RANGE
    // ====================================================

    fn validate_range(
        &self,
        bars: &[&Bar],
        support: &PriceLevel,
        resistance: &PriceLevel,
        lower_boundary: f64,
        upper_boundary: f64,
        width_pct: f64,
        current_ts: i64,
    ) -> Option<PriceRange> {

        if bars.len() < 3 {
            return None;
        }

        let range_width =
            upper_boundary - lower_boundary;

        if !range_width.is_finite()
            || range_width <= 0.0
        {
            return None;
        }

        // ====================================================
        // VALIDATE CONFIGURATION
        // ====================================================

        if !self.config.min_reversal_fraction.is_finite()
            || self.config.min_reversal_fraction <= 0.0
            || self.config.min_reversal_fraction > 1.0
            || !self.config.boundary_proximity_pct.is_finite()
            || !(0.0..=50.0).contains(
                &self.config.boundary_proximity_pct
            )
            || !self.config.min_containment_pct.is_finite()
            || !(0.0..=100.0).contains(
                &self.config.min_containment_pct
            )
        {
            return None;
        }

        // ====================================================
        // VALIDATE OSCILLATIONS
        // ====================================================

        let reactions = self.validate_oscillations(
            bars,
            support,
            resistance,
            lower_boundary,
            upper_boundary,
        );

        if reactions.is_empty() {
            return None;
        }

        // ====================================================
        // COUNT CONFIRMED REACTIONS
        // ====================================================

        let mut support_touches = 0usize;
        let mut resistance_touches = 0usize;

        let mut completed_oscillations = 0usize;

        let mut last_boundary: Option<Boundary> = None;

        let mut structural_confirmed_ts = None;

        for reaction in &reactions {

            match reaction.boundary {

                Boundary::Lower => {
                    support_touches += 1;
                }

                Boundary::Upper => {
                    resistance_touches += 1;
                }
            }

            if let Some(previous) = last_boundary {

                if previous != reaction.boundary {
                    completed_oscillations += 1;
                }
            }

            last_boundary = Some(reaction.boundary);

            if structural_confirmed_ts.is_none()
                && support_touches
                    >= self.config.min_boundary_touches
                && resistance_touches
                    >= self.config.min_boundary_touches
                && completed_oscillations
                    >= self.config.min_confirmed_oscillations
            {
                structural_confirmed_ts =
                    Some(reaction.timestamp_ms);
            }
        }

        let _structural_confirmed_ts =
            structural_confirmed_ts?;

        // ====================================================
        // RANGE OBSERVATION WINDOW
        // ====================================================

        let start_ts = bars
            .first()?
            .end
            .timestamp_millis();

        let end_ts = bars
            .last()?
            .end
            .timestamp_millis();

        let last_touch_ts = reactions
            .last()?
            .timestamp_ms;

        // ====================================================
        // OBSERVED EXTREMES AND CONTAINMENT
        // ====================================================

        let mut lowest_price = f64::INFINITY;
        let mut highest_price = f64::NEG_INFINITY;

        let mut contained_candles = 0usize;

        // Include the full segment, not just candles
        // between the first and last boundary reactions.

        for bar in bars {

            lowest_price =
                lowest_price.min(bar.low);

            highest_price =
                highest_price.max(bar.high);

            // Allow candles to wick into the established
            // support and resistance zones.

            if bar.close >= support.lower_bound
                && bar.close <= resistance.upper_bound
            {
                contained_candles += 1;
            }
        }

        let containment_pct =
            (contained_candles as f64
                / bars.len() as f64)
                * 100.0;

        if containment_pct
            < self.config.min_containment_pct
        {
            return None;
        }

        // ====================================================
        // RANGE STRENGTH
        // ====================================================

        let strength = self.calculate_strength(
            support.strength,
            resistance.strength,
            completed_oscillations,
            containment_pct,
            start_ts,
            end_ts,
        );

        // ====================================================
        // CONFIRMATION TIMESTAMP
        // ====================================================

        // The supplied price levels represent a snapshot
        // calculated at the current analysis timestamp.
        //
        // Without historical level snapshots, we cannot
        // establish that those levels were available when
        // the earlier structural reversals occurred.
        //
        // Therefore, use the current analysis timestamp
        // as the conservative availability timestamp.

        let confirmed_ts = current_ts;

        Some(PriceRange {
            lower_boundary,
            upper_boundary,

            lowest_price,
            highest_price,

            midpoint:
                (lower_boundary + upper_boundary) / 2.0,

            width_pct,

            support_strength: support.strength,
            resistance_strength: resistance.strength,

            support_touches,
            resistance_touches,

            completed_oscillations,
            containment_pct,

            strength,

            status: RangeStatus::Active,

            start_ts,
            end_ts,
            confirmed_ts,
            last_touch_ts,
        })
    }


    // ====================================================
    // VALIDATE OSCILLATIONS
    // ====================================================

    // Adapted from ResearchV4's reversal-confirmation
    // state machine.
    //
    // Tracks actual price extremes and requires a
    // completed candle close to confirm each reversal.
    //
    // Prevents consecutive confirmed reversals from
    // the same boundary without an opposite reversal.

    fn validate_oscillations(
        &self,
        bars: &[&Bar],
        support: &PriceLevel,
        resistance: &PriceLevel,
        lower_boundary: f64,
        upper_boundary: f64,
    ) -> Vec<BoundaryReaction> {

        let mut reactions = Vec::new();

        let range_width =
            upper_boundary - lower_boundary;

        let required_reversal =
            range_width * self.config.min_reversal_fraction;

        let proximity =
            range_width
                * (self.config.boundary_proximity_pct / 100.0);

        // ====================================================
        // CALCULATE BOUNDARY ZONES
        // ====================================================

        let lower_zone =
            (lower_boundary + proximity)
                .max(support.upper_bound);

        let upper_zone =
            (upper_boundary - proximity)
                .min(resistance.lower_bound);

        if lower_zone >= upper_zone {
            return reactions;
        }

        // ====================================================
        // TRACK REVERSAL STATE
        // ====================================================

        let mut active: Option<ReversalState> = None;

        let mut last_confirmed: Option<Boundary> = None;

        // ====================================================
        // PROCESS COMPLETED CANDLES
        // ====================================================

        for bar in bars {

            let near_lower =
                bar.low <= lower_zone;

            let near_upper =
                bar.high >= upper_zone;

            // ====================================================
            // UPDATE ACTIVE REVERSAL
            // ====================================================

            if let Some(state) = active.as_mut() {

                match state.boundary {

                    Boundary::Lower => {

                        state.extreme_price =
                            state.extreme_price.min(bar.low);

                        if bar.close - state.extreme_price
                            >= required_reversal
                        {
                            reactions.push(BoundaryReaction {
                                boundary: Boundary::Lower,
                                timestamp_ms:
                                    bar.end.timestamp_millis(),
                                extreme_price:
                                    state.extreme_price,
                            });

                            last_confirmed =
                                Some(Boundary::Lower);

                            active = None;
                        }
                    }

                    Boundary::Upper => {

                        state.extreme_price =
                            state.extreme_price.max(bar.high);

                        if state.extreme_price - bar.close
                            >= required_reversal
                        {
                            reactions.push(BoundaryReaction {
                                boundary: Boundary::Upper,
                                timestamp_ms:
                                    bar.end.timestamp_millis(),
                                extreme_price:
                                    state.extreme_price,
                            });

                            last_confirmed =
                                Some(Boundary::Upper);

                            active = None;
                        }
                    }
                }

                continue;
            }

            // ====================================================
            // DETECT NEW BOUNDARY INTERACTION
            // ====================================================

            // Intrabar ordering is unknown when a candle
            // touches both boundaries.

            if near_lower && near_upper {
                continue;
            }

            if near_lower
                && last_confirmed != Some(Boundary::Lower)
            {
                active = Some(ReversalState {
                    boundary: Boundary::Lower,
                    extreme_price: bar.low,
                });

            } else if near_upper
                && last_confirmed != Some(Boundary::Upper)
            {
                active = Some(ReversalState {
                    boundary: Boundary::Upper,
                    extreme_price: bar.high,
                });
            }
        }

        reactions
    }


    // ====================================================
    // CALCULATE RANGE STRENGTH
    // ====================================================

    fn calculate_strength(
        &self,
        support_strength: f64,
        resistance_strength: f64,
        completed_oscillations: usize,
        containment_pct: f64,
        start_ts: i64,
        last_touch_ts: i64,
    ) -> f64 {

        const BOUNDARY_WEIGHT: f64 = 30.0;
        const OSCILLATION_WEIGHT: f64 = 30.0;
        const CONTAINMENT_WEIGHT: f64 = 25.0;
        const DURATION_WEIGHT: f64 = 15.0;

        // ====================================================
        // BOUNDARY STRENGTH
        // ====================================================

        let boundary_score =
            ((support_strength + resistance_strength) / 2.0)
                .clamp(0.0, 100.0)
                / 100.0
                * BOUNDARY_WEIGHT;

        // ====================================================
        // OSCILLATION SCORE
        // ====================================================

        let oscillation_score =
            (completed_oscillations as f64 / 5.0)
                .min(1.0)
                * OSCILLATION_WEIGHT;

        // ====================================================
        // CONTAINMENT SCORE
        // ====================================================

        let containment_score =
            (containment_pct / 100.0)
                .clamp(0.0, 1.0)
                * CONTAINMENT_WEIGHT;

        // ====================================================
        // DURATION SCORE
        // ====================================================

        let duration_ms =
            last_touch_ts.saturating_sub(start_ts).max(0);

        let lookback_ms =
            (self.config.lookback_secs as f64) * 1000.0;

        let duration_score = if lookback_ms > 0.0 {
            (duration_ms as f64 / lookback_ms)
                .clamp(0.0, 1.0)
                * DURATION_WEIGHT
        } else {
            0.0
        };

        (
            boundary_score
                + oscillation_score
                + containment_score
                + duration_score
        )
        .clamp(0.0, 100.0)
    }


    // ====================================================
    // GET RANGES
    // ====================================================

    pub fn get_ranges(
        &self,
        symbol: &str,
        timeframe: Timeframe,
    ) -> Option<&PriceRanges> {
        self.cache.get(&(symbol.to_string(), timeframe))
    }


    // ====================================================
    // CLEAR SYMBOL
    // ====================================================

    pub fn clear_symbol(
        &mut self,
        symbol: &str,
    ) {
        self.cache.retain(|(cached_symbol, _), _| {
            cached_symbol != symbol
        });
    }
}






