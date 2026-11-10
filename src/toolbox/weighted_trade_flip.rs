#![allow(dead_code)]

use crate::core::trades::{
    direction::Direction,
    trades::Trades,
};

// ============================================================
// WEIGHTED TRADE FLIP TOOLBOX
// ============================================================
//
// Provides reusable, configurable probability-based logic
// for reversing trade directions based on consecutive
// profitable trades.
//
// The toolbox analyzes historical closed trades belonging
// exclusively to the specified strategy.
//
// Consecutive profitable trades are determined by their
// closing timestamps, with the most recently closed trade
// evaluated first.
//
// As the number of consecutive profitable trades increases,
// the configured probability of reversing the next trade
// can also increase.
//
// ============================================================
// PROFITABILITY CALCULATION
// ============================================================
//
// Combined profitability is calculated using:
//
//     Net Profit % = (Combined Net Realized PnL
//                     / Combined Trade Margin) * 100
//
// Only fully closed, profitable trades belonging to the
// specified strategy contribute to the winning streak.
//
// A losing or break-even trade resets the winning streak.
//
// ============================================================
// WEIGHTED PROBABILITIES
// ============================================================
//
// Example configuration:
//
//     0-1 consecutive wins = 0% flip probability
//     2 consecutive wins   = 50% flip probability
//     3 consecutive wins   = 75% flip probability
//     4+ consecutive wins  = 90% flip probability
//
// A deterministic random value is generated for each
// new trade signal.
//
// If the random value falls below the configured probability,
// the toolbox recommends reversing the trade direction.
//
// ============================================================
// DETERMINISTIC RANDOMNESS
// ============================================================
//
// A configurable random seed allows reproducible backtests.
//
// Random decisions are derived from the seed and a stable
// signal identifier, avoiding dependence on asynchronous
// event-processing order.
//
// ============================================================
// EXECUTION FLOW
// ============================================================
//
// Strategy
//     -> WeightedTradeFlip::winning_streak_snapshot()
//     -> Historical closed trade evaluation
//     -> Consecutive winning trade calculation
//     -> Combined net profit percentage calculation
//     -> Determine configured flip probability
//     -> WeightedTradeFlip::should_flip()
//     -> Strategy decides final trade direction
//
// The toolbox only evaluates historical trade performance
// and determines whether a trade should be flipped.
//
// It does not submit orders, modify trades, calculate
// take-profit levels, or calculate stop-loss levels.
//
// ============================================================


// ============================================================
// WEIGHTED TRADE FLIP CONFIGURATION
// ============================================================

/// Configurable probability-based trade direction reversal.
///
/// Uses consecutive profitable closed trades to determine
/// the probability of reversing the next trade signal.
///
/// All probabilities are configurable for backtesting.
#[derive(Debug, Clone)]
pub struct WeightedTradeFlipConfig {
    /// Whether weighted trade flipping is enabled.
    pub enabled: bool,

    /// Minimum combined net realized profit percentage
    /// required across the consecutive winning trades.
    ///
    /// Calculated against their combined allocated margin.
    /// Set to 0.0 to disable the additional profit threshold.
    pub min_profit_percent: f64,

    /// Probability of flipping the next trade after
    /// exactly 2 consecutive profitable trades.
    ///
    /// Example: 0.50 = 50% probability.
    pub flip_probability_after_2_wins: f64,

    /// Probability of flipping the next trade after
    /// exactly 3 consecutive profitable trades.
    ///
    /// Example: 0.75 = 75% probability.
    pub flip_probability_after_3_wins: f64,

    /// Probability of flipping the next trade after
    /// 4 or more consecutive profitable trades.
    ///
    /// Example: 0.90 = 90% probability.
    pub flip_probability_after_4_wins: f64,

    /// Deterministic random seed used for probability calculations.
    ///
    /// Identical seeds and signal identifiers produce
    /// reproducible random decisions.
    pub seed: u64,
}


// ============================================================
// WEIGHTED TRADE FLIP
// ============================================================

/// Evaluates consecutive profitable trades and determines
/// the probability of reversing the next trade signal.
#[derive(Debug, Clone)]
pub struct WeightedTradeFlip {
    /// Configurable weighted trade flipping settings.
    pub config: WeightedTradeFlipConfig,
}


// ============================================================
// WINNING STREAK SNAPSHOT
// ============================================================

/// Historical winning-streak statistics for one strategy.
#[derive(Debug, Clone, Default)]
pub struct WinningStreakSnapshot {
    /// Number of consecutive profitable closed trades.
    pub consecutive_wins: usize,

    /// Combined net realized PnL of consecutive winning trades.
    pub total_net_pnl: f64,

    /// Combined allocated margin of consecutive winning trades.
    pub total_margin_usd: f64,

    /// Combined net profit percentage.
    pub net_profit_percent: f64,

    /// Probability of flipping the next trade.
    pub flip_probability: f64,
}


impl WeightedTradeFlip {

    // ============================================================
    // NEW
    // ============================================================

    /// Creates a new weighted trade flip toolbox
    /// using the supplied configuration.
    pub fn new(config: WeightedTradeFlipConfig) -> Self {
        assert!(
            config.min_profit_percent.is_finite()
                && config.min_profit_percent >= 0.0,
            "Minimum profit percentage must be finite and non-negative"
        );

        for probability in [
            config.flip_probability_after_2_wins,
            config.flip_probability_after_3_wins,
            config.flip_probability_after_4_wins,
        ] {
            assert!(
                probability.is_finite()
                    && (0.0..=1.0).contains(&probability),
                "Flip probabilities must be between 0.0 and 1.0"
            );
        }

        Self { config }
    }

    // ============================================================
    // GET FLIP PROBABILITY
    // ============================================================

    /// Returns the configured probability of reversing
    /// the next trade based on consecutive profitable trades.
    ///
    /// Probabilities are expressed between 0.0 and 1.0.
    pub fn flip_probability(&self, consecutive_wins: usize) -> f64 {
        if !self.config.enabled {
            return 0.0;
        }

        match consecutive_wins {
            0 | 1 => 0.0,
            2 => self.config.flip_probability_after_2_wins,
            3 => self.config.flip_probability_after_3_wins,
            _ => self.config.flip_probability_after_4_wins,
        }
    }

    // ============================================================
    // BUILD WINNING STREAK SNAPSHOT
    // ============================================================

    /// Calculates consecutive profitable closed trades
    /// belonging exclusively to the specified strategy.
    ///
    /// Trades are evaluated by closing timestamp, newest first.
    ///
    /// Net realized PnL includes confirmed entry and exit fees.
    ///
    /// Combined profitability is calculated against the total
    /// allocated margin of the consecutive winning trades.
    ///
    /// A losing or break-even trade ends the winning streak.
    pub fn winning_streak_snapshot(
        &self,
        strategy_id: &str,
        trades: &Trades,
    ) -> WinningStreakSnapshot {

        let mut snapshot = WinningStreakSnapshot::default();

        // ========================================================
        // CHECK ENABLED
        // ========================================================

        if !self.config.enabled {
            return snapshot;
        }

        // ========================================================
        // COLLECT CLOSED STRATEGY TRADES
        // ========================================================

        let mut closed_trades: Vec<_> = trades
            .closed_trades_for_strategy(strategy_id)
            .collect();

        // ========================================================
        // SORT BY CLOSING TIMESTAMP
        // ========================================================

        closed_trades.sort_by(|a, b| {
            b.closed_ts_ms
                .cmp(&a.closed_ts_ms)
                .then_with(|| b.created_ts_ms.cmp(&a.created_ts_ms))
        });

        // ========================================================
        // CALCULATE CONSECUTIVE WINNING TRADES
        // ========================================================

        for trade in closed_trades {

            // Require complete finalized trade data.
            let (Some(_closed_ts), Some(entry_px), Some(exit_px)) = (
                trade.closed_ts_ms,
                trade.avg_entry_px,
                trade.avg_exit_px,
            ) else {
                break;
            };

            if !entry_px.is_finite()
                || !exit_px.is_finite()
                || entry_px <= 0.0
                || exit_px <= 0.0
                || !trade.exited_quantity.is_finite()
                || trade.exited_quantity <= 0.0
                || !trade.entry_fees.is_finite()
                || !trade.exit_fees.is_finite()
                || !trade.margin_usd.is_finite()
                || trade.margin_usd <= 0.0
            {
                break;
            }

            // ====================================================
            // CALCULATE GROSS REALIZED PNL
            // ====================================================

            let gross_pnl = match trade.direction {
                Direction::Long => {
                    (exit_px - entry_px) * trade.exited_quantity
                }

                Direction::Short => {
                    (entry_px - exit_px) * trade.exited_quantity
                }
            };

            // ====================================================
            // CALCULATE NET REALIZED PNL
            // ====================================================

            let net_pnl = gross_pnl
                - trade.entry_fees
                - trade.exit_fees;

            // A losing or break-even trade ends the streak.
            if !net_pnl.is_finite() || net_pnl <= 0.0 {
                break;
            }

            // ====================================================
            // ACCUMULATE WINNING TRADE STATISTICS
            // ====================================================

            snapshot.consecutive_wins += 1;
            snapshot.total_net_pnl += net_pnl;
            snapshot.total_margin_usd += trade.margin_usd;
        }

        // ========================================================
        // CALCULATE COMBINED PROFIT PERCENTAGE
        // ========================================================

        if snapshot.total_margin_usd > 0.0 {
            snapshot.net_profit_percent =
                (snapshot.total_net_pnl / snapshot.total_margin_usd)
                    * 100.0;
        }

        // ========================================================
        // DETERMINE FLIP PROBABILITY
        // ========================================================

        if snapshot.net_profit_percent.is_finite()
            && snapshot.net_profit_percent
                >= self.config.min_profit_percent
        {
            snapshot.flip_probability =
                self.flip_probability(snapshot.consecutive_wins);
        }

        snapshot
    }

    // ============================================================
    // DETERMINE WEIGHTED FLIP
    // ============================================================

    /// Determines whether a new trade signal should be reversed.
    ///
    /// Uses a deterministic pseudo-random value derived from
    /// the configured seed and a stable signal identifier.
    ///
    /// Returns true when the generated value falls below
    /// the configured flip probability.
    ///
    /// Identical seeds and signal identifiers produce
    /// identical decisions across backtest runs.
    pub fn should_flip(
        &self,
        snapshot: &WinningStreakSnapshot,
        signal_id: u64,
    ) -> bool {

        if !self.config.enabled
            || !snapshot.flip_probability.is_finite()
            || snapshot.flip_probability <= 0.0
        {
            return false;
        }

        // ========================================================
        // GENERATE DETERMINISTIC RANDOM VALUE
        // ========================================================

        let mut value = self.config.seed ^ signal_id;

        // SplitMix64 mixing.
        value = value.wrapping_add(0x9E3779B97F4A7C15);

        value = (value ^ (value >> 30))
            .wrapping_mul(0xBF58476D1CE4E5B9);

        value = (value ^ (value >> 27))
            .wrapping_mul(0x94D049BB133111EB);

        value ^= value >> 31;

        // Convert to a value in [0.0, 1.0).
        let random_value =
            ((value >> 11) as f64) / ((1u64 << 53) as f64);

        // ========================================================
        // EVALUATE FLIP PROBABILITY
        // ========================================================

        random_value < snapshot.flip_probability
    }
}