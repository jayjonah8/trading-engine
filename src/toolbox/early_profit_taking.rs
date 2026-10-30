#![allow(dead_code)]

use std::collections::HashMap;

use crate::core::{
    engine::state::LastPrice,
    trades::trades::Trades,
};


// WARNING:
// Early Profit Taking relies on the engine retaining trade records
// to accurately determine the latest trade-opening timestamp.
// Premature "Trade" removal or pruning from engine state may cause incorrect behavior.


// ============================================================
// EARLY PROFIT TAKING TOOLBOX
// ============================================================
//
// Provides reusable, configurable early-profit-taking logic
// for closing profitable baskets of trades.
//
// Each strategy owns its own EarlyProfitTaking instance and
// can define multiple independent profit-taking rules.
//
// The toolbox evaluates only trades belonging to the specified
// strategy, preventing interference with other strategies.
//
// ============================================================
// BASKET PROFITABILITY
// ============================================================
//
// Basket profitability is calculated using:
//
//     Basket Profit % = (Net Unrealized PnL / Total Open Margin) * 100
//
// Total Open Margin represents the combined margin assigned
// to all currently open trades belonging to the strategy.
//
// IMPORTANT / WARNING!!:
// This calculation does NOT use the strategy's total reference
// margin, capital allocation, or available account balance.
//
// Instead, profitability is measured against the margin
// currently deployed across the strategy's open positions.
//
// Trade margin represents the capital assigned to each trade,
// which may differ from the actual margin required by the
// execution venue.
//
// ============================================================
// AUTOMATIC SCALING
// ============================================================
//
// Profit thresholds automatically scale with the combined
// margin of currently open trades.
//
// As positions open or close, total basket margin changes,
// and the dollar profit required to satisfy a rule adjusts
// proportionally.
//
// Increasing individual trade sizes also increases the
// required dollar profit without changing rule thresholds.
//
// This allows a configured profit percentage to represent
// a consistent return on deployed margin, regardless of
// the number or size of currently open positions.
//
// EXAMPLE: 1.5% PROFIT THRESHOLD
// ------------------------------------------------------------
//
// Open Trades      Total Open Margin      Required Net Profit
// ------------------------------------------------------------
//      2                  $400                   $6
//      4                  $800                  $12
//      6                $1,200                  $18
//      8                $1,600                  $24
//
// The profit threshold remains 1.5%, while the required
// dollar profit scales with the current basket margin.
//
// ============================================================
// EXAMPLE 1: STANDARD EARLY PROFIT
// ============================================================
//
// Open trades:          4
// Total open margin:    $400
// Net unrealized PnL:   $20
// Basket profit:        5.0%
// Profit threshold:     4.3%
//
// Result:
// Rule triggers because basket profit exceeds 4.3%,
// assuming all other configured conditions are satisfied.
//
// ============================================================
// EXAMPLE 2: SLOW MARKET EARLY PROFIT
// ============================================================
//
// Open trades:          3
// Total open margin:    $300
// Net unrealized PnL:   $3
// Basket profit:        1.0%
// Profit threshold:     0.9%
// Last trade opened:    20 minutes ago
// Required inactivity:  15 minutes
//
// Result:
// Rule triggers because all configured conditions
// are satisfied.
//
// ============================================================
// EXAMPLE 3: RULE NOT SATISFIED
// ============================================================
//
// Open trades:          3
// Total open margin:    $300
// Net unrealized PnL:   $6
// Basket profit:        2.0%
// Profit threshold:     4.3%
//
// Result:
// Rule does not trigger because basket profitability
// remains below the configured threshold.
//
// ============================================================
// EXECUTION FLOW
// ============================================================
//
// Strategy
//     -> EarlyProfitTaking::basket_snapshot()
//     -> EarlyProfitTaking::check()
//     -> Cooldown validation
//     -> Matching EarlyProfitRule
//     -> Strategy requests closure of its own trades
//     -> EarlyProfitTaking::record_exit_request()
//
// The toolbox only evaluates conditions and manages cooldown.
//
// It does not submit orders, modify trades, or communicate
// directly with execution venues.
//
// ============================================================


// ============================================================
// EARLY PROFIT RULE
// ============================================================

/// Configurable conditions for closing a profitable
/// basket of trades belonging to one strategy.
#[derive(Debug, Clone)]
pub struct EarlyProfitRule {
    /// Rule identifier used for logging.
    pub name: String,

    /// Whether this rule is enabled.
    pub enabled: bool,

    /// Minimum number of open trades required.
    pub min_open_trades: usize,

    /// Optional maximum number of open trades.
    pub max_open_trades: Option<usize>,

    /// Minimum total strategy-allocated margin.
    pub min_margin_usd: f64,

    /// Minimum net basket profit percentage required to trigger early profit taking.
    pub min_profit_percent: f64,

    /// Optional minimum time since the latest confirmed
    /// first-entry fill, expressed in milliseconds.
    pub no_new_trade_for_ms: Option<i64>,
}

// ============================================================
// EARLY PROFIT TAKING
// ============================================================

/// Evaluates configurable early-profit-taking rules
/// against trades owned by a specific strategy.
///
/// Manages its own cooldown to prevent repeated
/// early-profit exit requests on every price update.
#[derive(Debug, Clone, Default)]
pub struct EarlyProfitTaking {
    /// Configurable early-profit-taking rules.
    pub rules: Vec<EarlyProfitRule>,

    /// Minimum interval between successful exit requests.
    ///
    /// Expressed in milliseconds.
    ///
    /// Example:
    /// 30_000 = 30 seconds.
    pub cooldown_ms: i64,

    /// Timestamp of the last successfully submitted
    /// early-profit exit request.
    ///
    /// Uses market-event timestamps so the same logic
    /// works in backtesting and live trading.
    last_exit_ts_ms: Option<i64>,
}

// ============================================================
// BASKET SNAPSHOT
// ============================================================

/// Current basket statistics for one strategy.
#[derive(Debug, Clone)]
pub struct StrategyBasketSnapshot {
    /// Number of currently open trades.
    pub open_trades: usize,

    /// Combined margin of all open trades.
    pub total_margin_usd: f64,

    /// Combined net unrealized PnL of all open trades.
    pub total_net_unrealized_pnl: f64,

    /// Net basket profit as a percentage of total open margin.
    pub net_profit_percent: f64,

    /// Timestamp of the most recently opened trade.
    pub last_open_ts_ms: Option<i64>,
}

impl EarlyProfitTaking {

    // ============================================================
    // NEW
    // ============================================================

    /// Creates a new early-profit-taking toolbox with
    /// configurable rules and a cooldown interval.
    pub fn new(
        rules: Vec<EarlyProfitRule>,
        cooldown_ms: i64,
    ) -> Self {
        assert!(
            cooldown_ms >= 0,
            "Early profit cooldown must be non-negative"
        );

        Self {
            rules,
            cooldown_ms,
            last_exit_ts_ms: None,
        }
    }

    // ============================================================
    // CHECK COOLDOWN
    // ============================================================

    /// Returns true when another early-profit exit
    /// request is allowed.
    ///
    /// Uses market-event timestamps rather than
    /// wall-clock time.
    pub fn cooldown_ready(&self, current_ts_ms: i64) -> bool {
        match self.last_exit_ts_ms {
            Some(last_ts) => {
                current_ts_ms >= last_ts
                    && current_ts_ms - last_ts >= self.cooldown_ms
            }

            None => true,
        }
    }

    // ============================================================
    // RECORD EXIT REQUEST
    // ============================================================

    /// Records when the strategy successfully submits
    /// at least one early-profit CloseTrade event.
    ///
    /// The cooldown starts only after successful
    /// submission, not simply when a rule matches.
    pub fn record_exit_request(&mut self, ts_ms: i64) {
        self.last_exit_ts_ms = Some(ts_ms);
    }

    // ============================================================
    // BUILD STRATEGY BASKET
    // ============================================================

    /// Calculates basket statistics exclusively for
    /// trades belonging to the specified strategy.
    pub fn basket_snapshot(
        &self,
        strategy_id: &str,
        trades: &Trades,
        last_px: &HashMap<String, LastPrice>,
    ) -> StrategyBasketSnapshot {

        // TODO: Validate price availability and freshness for
        // every open trade before evaluating basket profitability.

        // TODO: Adjust margin_usd for partial entry fills
        // and partial exits. Currently assumes fully filled trades.

        let open_trades = trades
            .count_with_open_quantity_for_strategy(strategy_id);

        let total_margin_usd = trades
            .total_open_margin_for_strategy(strategy_id);

        let total_net_unrealized_pnl = trades
            .total_net_unrealized_pnl_for_strategy(
                strategy_id,
                last_px,
            );

        let net_profit_percent = if total_margin_usd > 0.0 {
            (total_net_unrealized_pnl / total_margin_usd) * 100.0
        } else {
            0.0
        };

        let last_open_ts_ms = trades
            .last_open_ts_for_strategy(strategy_id);

        StrategyBasketSnapshot {
            open_trades,
            total_margin_usd,
            total_net_unrealized_pnl,
            net_profit_percent,
            last_open_ts_ms,
        }
    }

    // ============================================================
    // CHECK EARLY PROFIT RULES
    // ============================================================

    /// Returns the first enabled rule whose conditions
    /// are satisfied by the strategy's current basket.
    ///
    /// Respects the configured cooldown between
    /// early-profit exit requests.
    pub fn check<'a>(
        &'a self,
        basket: &StrategyBasketSnapshot,
        current_ts_ms: i64,
    ) -> Option<&'a EarlyProfitRule> {

        // TODO: Track the latest confirmed entry fill independently
        // if closed trades are removed from Trades or additional
        // entry fills occur after the first fill.

        // TODO: Add per-trade pending-exit tracking if necessary.
        // Engine-side order validation must prevent duplicate exits.

        // ============================================================
        // COOLDOWN CHECK
        // ============================================================

        if !self.cooldown_ready(current_ts_ms) {
            return None;
        }

        // ============================================================
        // VALIDATE BASKET
        // ============================================================

        if basket.open_trades == 0
            || !basket.total_margin_usd.is_finite()
            || basket.total_margin_usd <= 0.0
            || !basket.total_net_unrealized_pnl.is_finite()
            || !basket.net_profit_percent.is_finite()
        {
            return None;
        }

        // ============================================================
        // EVALUATE RULES
        // ============================================================

        self.rules.iter().find(|rule| {
            if !rule.enabled {
                return false;
            }

            if basket.open_trades < rule.min_open_trades {
                return false;
            }

            if let Some(max) = rule.max_open_trades {
                if basket.open_trades > max {
                    return false;
                }
            }

            if basket.total_margin_usd < rule.min_margin_usd {
                return false;
            }

            if basket.net_profit_percent < rule.min_profit_percent {
                return false;
            }

            if let Some(wait_ms) = rule.no_new_trade_for_ms {
                let Some(last_open_ts_ms) = basket.last_open_ts_ms else {
                    return false;
                };

                if current_ts_ms < last_open_ts_ms
                    || current_ts_ms - last_open_ts_ms < wait_ms
                {
                    return false;
                }
            }

            true
        })
    }
}