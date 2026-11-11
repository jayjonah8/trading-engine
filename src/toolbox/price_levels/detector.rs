use std::collections::HashMap;

use crate::{
    core::events::Bar,
    market::{
        candle_history::CandleHistory,
        timeframe::Timeframe,
    },
};

use super::{
    config::PriceLevelConfig,
    types::{
        PriceLevel,
        PriceLevelType,
        PriceLevels,
    },
};


// ====================================================
// PRICE LEVEL DETECTOR
// ====================================================
//
// Reusable market structure analysis component that identifies
// significant support and resistance zones using completed
// historical OHLCV candles.
//
// The detector operates independently of individual trading
// strategies and uses the existing CandleHistory infrastructure
// without maintaining duplicate historical candle data.
//
// Detection process:
// 1. Retrieve completed candles for the configured timeframe
//    and historical lookback period.
// 2. Identify confirmed swing highs and lows using surrounding
//    candles to locate meaningful price reversals.
// 3. Measure reversal magnitude and relative trading volume
//    associated with each detected swing.
// 4. Cluster nearby swing prices into support/resistance zones
//    using a configurable percentage tolerance.
// 5. Require independent price reactions to reduce noise
//    and avoid counting the same reversal multiple times.
// 6. Calculate a normalized significance score based on
//    reaction frequency, reversal strength, volume, and recency.
// 7. Rank detected levels by significance and retain the
//    strongest levels according to the configured maximum.
// 8. Cache results by symbol and timeframe for efficient
//    retrieval by trading strategies.
//
// Detection runs when completed candles become available,
// avoiding expensive recalculations on every market tick.
//
// Swing confirmation uses only completed historical candles,
// preventing future market information from influencing
// earlier backtest decisions.
//
// The detector provides market structure information only.
// Individual strategies determine whether detected levels
// should influence entries, exits, or risk management.
//
// TODO:
// - Combine levels across multiple candle timeframes.
// - Detect support/resistance confluence across timeframes.
// - Track confirmed breakouts and support/resistance flips.
// - Identify retests, reclaims, and invalidated levels.
// - Add incremental updates to improve performance when
//   analyzing large numbers of assets.


// ====================================================
// INTERNAL SWING POINT
// ====================================================

#[derive(Debug, Clone)]
struct SwingPoint {
    price: f64,
    timestamp_ms: i64,
    index: usize,
    is_high: bool,
    reversal_pct: f64,
    relative_volume: f64,
}

// ====================================================
// INTERNAL PRICE CLUSTER
// ====================================================

#[derive(Debug)]
struct PriceCluster {
    points: Vec<SwingPoint>,
}

// ====================================================
// PRICE LEVEL DETECTOR
// ====================================================

pub struct PriceLevelDetector {
    pub config: PriceLevelConfig,

    // Cached results by symbol and timeframe.
    cache: HashMap<(String, Timeframe), PriceLevels>,
}

impl PriceLevelDetector {

    // ====================================================
    // NEW
    // ====================================================

    pub fn new(config: PriceLevelConfig) -> Self {
        Self {
            config,
            cache: HashMap::new(),
        }
    }

    // ====================================================
    // UPDATE LEVELS
    // ====================================================

    pub fn update(
        &mut self,
        symbol: &str,
        timeframe: Timeframe,
        current_ts: i64,
        candle_history: &CandleHistory,
    ) {

        // ====================================================
        // CONFIGURATION
        // ====================================================

        if !self.config.enabled {
            return;
        }

        if timeframe != self.config.timeframe {
            return;
        }

        if self.config.lookback_secs == 0
            || self.config.swing_strength == 0
            || self.config.min_reactions == 0
            || self.config.max_levels == 0
            || !self.config.cluster_tolerance_pct.is_finite()
            || self.config.cluster_tolerance_pct < 0.0
        {
            return;
        }

        // ====================================================
        // GET CANDLE HISTORY
        // ====================================================

        let Some(history) = candle_history.get(
            symbol,
            timeframe,
        ) else {
            return;
        };

        let lookback_ms =
            (self.config.lookback_secs as i64)
                .saturating_mul(1000);

        let cutoff = current_ts.saturating_sub(lookback_ms);

        let bars: Vec<&Bar> = history
            .iter()
            .filter(|bar| {
                let ts = bar.end.timestamp_millis();

                ts >= cutoff && ts <= current_ts
            })
            .collect();

        let required =
            self.config.swing_strength * 2 + 1;

        if bars.len() < required {
            return;
        }

        // ====================================================
        // DETECT SWING POINTS
        // ====================================================

        let swings = self.detect_swings(&bars);

        // ====================================================
        // CLUSTER PRICE LEVELS
        // ====================================================

        let clusters = self.cluster_swings(swings);

        // ====================================================
        // BUILD PRICE LEVELS
        // ====================================================

        let current_price = bars
            .last()
            .map(|bar| bar.close)
            .unwrap_or(0.0);

        let mut levels = Vec::new();

        for cluster in clusters {

            if cluster.points.len() < self.config.min_reactions {
                continue;
            }

            let count = cluster.points.len();

            let price = cluster.points
                .iter()
                .map(|point| point.price)
                .sum::<f64>()
                / count as f64;

            let lower_bound = cluster.points
                .iter()
                .map(|point| point.price)
                .fold(f64::INFINITY, f64::min);

            let upper_bound = cluster.points
                .iter()
                .map(|point| point.price)
                .fold(f64::NEG_INFINITY, f64::max);

            let average_reversal_pct = cluster.points
                .iter()
                .map(|point| point.reversal_pct)
                .sum::<f64>()
                / count as f64;

            let relative_volume = cluster.points
                .iter()
                .map(|point| point.relative_volume)
                .sum::<f64>()
                / count as f64;

            let last_touch_ts = cluster.points
                .iter()
                .map(|point| point.timestamp_ms)
                .max()
                .unwrap_or(current_ts);

            // ====================================================
            // LEVEL CLASSIFICATION
            // ====================================================

            let level_type = if upper_bound < current_price {
                PriceLevelType::Support
            } else if lower_bound > current_price {
                PriceLevelType::Resistance
            } else {
                PriceLevelType::SupportResistance
            };

            // ====================================================
            // STRENGTH SCORE
            // ====================================================

            let strength = self.calculate_strength(
                count,
                average_reversal_pct,
                relative_volume,
                last_touch_ts,
                current_ts,
            );

            levels.push(PriceLevel {
                price,
                lower_bound,
                upper_bound,
                level_type,
                strength,
                touches: count,
                average_reversal_pct,
                relative_volume,
                last_touch_ts,
            });
        }

        // ====================================================
        // SORT BY STRENGTH
        // ====================================================

        levels.sort_by(|a, b| {
            b.strength.total_cmp(&a.strength)
        });

        levels.truncate(self.config.max_levels);

        // ====================================================
        // CACHE RESULTS
        // ====================================================

        self.cache.insert(
            (symbol.to_string(), timeframe),
            PriceLevels {
                symbol: symbol.to_string(),
                timeframe,
                calculated_ts: current_ts,
                levels,
            },
        );
    }

    // ====================================================
    // DETECT SWING HIGHS AND LOWS
    // ====================================================

    fn detect_swings(
        &self,
        bars: &[&Bar],
    ) -> Vec<SwingPoint> {

        let strength = self.config.swing_strength;

        let mut swings = Vec::new();

        if bars.len() < strength * 2 + 1 {
            return swings;
        }

        // ====================================================
        // SWING DETECTION
        // ====================================================

        for i in strength..(bars.len() - strength) {

            let current = bars[i];

            let left = i - strength;
            let right = i + strength;

            let mut is_high = true;
            let mut is_low = true;

            for j in left..=right {

                if j == i {
                    continue;
                }

                if bars[j].high >= current.high {
                    is_high = false;
                }

                if bars[j].low <= current.low {
                    is_low = false;
                }
            }

            if !is_high && !is_low {
                continue;
            }

            // ====================================================
            // RELATIVE VOLUME
            // ====================================================

            let average_volume = bars[left..=right]
                .iter()
                .map(|bar| bar.volume)
                .sum::<f64>()
                / (right - left + 1) as f64;

            let relative_volume = if average_volume > 0.0 {
                current.volume / average_volume
            } else {
                1.0
            };

            // ====================================================
            // SWING HIGH
            // ====================================================

            if is_high {

                let following_low = bars[(i + 1)..=right]
                    .iter()
                    .map(|bar| bar.low)
                    .fold(f64::INFINITY, f64::min);

                let reversal_pct =
                    if current.high > 0.0 {
                        ((current.high - following_low)
                            / current.high)
                            * 100.0
                    } else {
                        0.0
                    };

                swings.push(SwingPoint {
                    price: current.high,
                    timestamp_ms: current.end.timestamp_millis(),
                    index: i,
                    is_high: true,
                    reversal_pct,
                    relative_volume,
                });
            }

            // ====================================================
            // SWING LOW
            // ====================================================

            if is_low {

                let following_high = bars[(i + 1)..=right]
                    .iter()
                    .map(|bar| bar.high)
                    .fold(f64::NEG_INFINITY, f64::max);

                let reversal_pct =
                    if current.low > 0.0 {
                        ((following_high - current.low)
                            / current.low)
                            * 100.0
                    } else {
                        0.0
                    };

                swings.push(SwingPoint {
                    price: current.low,
                    timestamp_ms: current.end.timestamp_millis(),
                    index: i,
                    is_high: false,
                    reversal_pct,
                    relative_volume,
                });
            }
        }

        swings
    }

    // ====================================================
    // CLUSTER SWING POINTS
    // ====================================================

    fn cluster_swings(
        &self,
        mut swings: Vec<SwingPoint>,
    ) -> Vec<PriceCluster> {

        // Sort by price so nearby levels can be grouped.
        swings.sort_by(|a, b| {
            a.price.total_cmp(&b.price)
        });

        let mut clusters: Vec<PriceCluster> = Vec::new();

        // ====================================================
        // BUILD CLUSTERS
        // ====================================================

        for swing in swings {

            let mut matched = false;

            for cluster in &mut clusters {

                let cluster_price = cluster.points
                    .iter()
                    .map(|point| point.price)
                    .sum::<f64>()
                    / cluster.points.len() as f64;

                let difference_pct =
                    ((swing.price - cluster_price).abs()
                        / cluster_price)
                        * 100.0;

                if difference_pct > self.config.cluster_tolerance_pct {
                    continue;
                }

                // Prevent nearby candles belonging to the
                // same reversal from inflating touch counts.
                let independent = cluster.points
                    .iter()
                    .all(|point| {
                        point.index.abs_diff(swing.index)
                            > self.config.swing_strength
                    });

                if !independent {
                    continue;
                }

                cluster.points.push(swing.clone());

                matched = true;
                break;
            }

            if !matched {
                clusters.push(PriceCluster {
                    points: vec![swing],
                });
            }
        }

        clusters
    }

    // ====================================================
    // CALCULATE LEVEL STRENGTH
    // ====================================================

    fn calculate_strength(
        &self,
        touches: usize,
        reversal_pct: f64,
        relative_volume: f64,
        last_touch_ts: i64,
        current_ts: i64,
    ) -> f64 {

        // ====================================================
        // REACTION SCORE (40 POINTS)
        // ====================================================

        let reaction_score =
            (touches as f64 / 5.0)
                .min(1.0)
                * 40.0;

        // ====================================================
        // REVERSAL SCORE (30 POINTS)
        // ====================================================

        let reversal_score =
            (reversal_pct / 2.0)
                .clamp(0.0, 1.0)
                * 30.0;

        // ====================================================
        // VOLUME SCORE (15 POINTS)
        // ====================================================

        let volume_score =
            (relative_volume / 2.0)
                .clamp(0.0, 1.0)
                * 15.0;

        // ====================================================
        // RECENCY SCORE (15 POINTS)
        // ====================================================

        let age_ms =
            current_ts.saturating_sub(last_touch_ts).max(0);

        let lookback_ms =
            (self.config.lookback_secs as f64) * 1000.0;

        let recency_fraction = if lookback_ms > 0.0 {
            1.0 - (age_ms as f64 / lookback_ms)
        } else {
            0.0
        };

        let recency_score =
            recency_fraction.clamp(0.0, 1.0) * 15.0;

        // ====================================================
        // TOTAL STRENGTH
        // ====================================================

        (
            reaction_score
                + reversal_score
                + volume_score
                + recency_score
        )
        .clamp(0.0, 100.0)
    }

    // ====================================================
    // GET CACHED LEVELS
    // ====================================================

    pub fn get_levels(
        &self,
        symbol: &str,
        timeframe: Timeframe,
    ) -> Option<&PriceLevels> {

        self.cache.get(&(
            symbol.to_string(),
            timeframe,
        ))
    }

    // ====================================================
    // CLEAR SYMBOL
    // ====================================================

    pub fn clear_symbol(
        &mut self,
        symbol: &str,
    ) {

        self.cache.retain(|(key_symbol, _), _| {
            key_symbol != symbol
        });
    }
}