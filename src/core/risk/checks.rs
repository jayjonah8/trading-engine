#![allow(dead_code)]

use std::collections::HashSet;

use crate::core::{
    risk::{
        limits::RiskLimits,
        reservations::RiskReservations,
    },
    trades::{
        Trade,
        Trades,
    },
};


// ============================================================
// RISK CHECK ERRORS
// ============================================================

/// Reason a proposed trading action failed an engine risk check.
#[derive(Debug, Clone, PartialEq)]
pub enum RiskCheckError {
    MaxActiveTrades {
        current: usize,
        max: usize,
    },

    MaxActiveAssets {
        current: usize,
        max: usize,
    },

    MaxActiveTradesPerSymbol {
        symbol: String,
        current: usize,
        max: usize,
    },

    MaxActiveTradesPerStrategy {
        strategy_id: String,
        current: usize,
        max: usize,
    },

    MaxOrderNotional {
        requested: f64,
        max: f64,
    },

    MaxGrossNotional {
        projected: f64,
        max: f64,
    },

    MaxSymbolNotional {
        symbol: String,
        projected: f64,
        max: f64,
    },

    InsufficientExitQuantity {
        requested: f64,
        available: f64,
    },

    InvalidQuantity {
        quantity: f64,
    },

    InvalidNotional {
        notional: f64,
    },
}


// ============================================================
// ACTIVE TRADE LIMIT
// ============================================================

/// Checks whether another directional Trade may become active.
///
/// `Opening`, `Open`, and `Closing` Trades count as active.
pub fn check_active_trade_limit(
    limits: &RiskLimits,
    trades: &Trades,
) -> Result<(), RiskCheckError> {
    let Some(max) =
        limits.max_active_trades
    else {
        return Ok(());
    };

    let current =
        trades.count_active();

    if current >= max {
        return Err(
            RiskCheckError::MaxActiveTrades {
                current,
                max,
            }
        );
    }

    Ok(())
}


// ============================================================
// ACTIVE ASSET LIMIT
// ============================================================

/// Checks whether opening a Trade on `symbol` would exceed the maximum
/// number of distinct assets with active Trades.
///
/// Existing active Trades on the same symbol do not consume an additional
/// asset slot.
///
/// Example:
///
/// Active Trades:
///
/// BTC
/// BTC
/// ETH
///
/// Active assets = 2
///
/// Opening another BTC Trade keeps active assets at 2.
///
/// Opening SOL increases active assets to 3.
pub fn check_active_asset_limit(
    limits: &RiskLimits,
    trades: &Trades,
    symbol: &str,
) -> Result<(), RiskCheckError> {
    let Some(max) =
        limits.max_active_assets
    else {
        return Ok(());
    };


    // If this symbol already has an active Trade, opening another Trade on the
    // same symbol does not increase the number of active assets.
    if trades
        .iter_active()
        .any(|trade| trade.symbol == symbol)
    {
        return Ok(());
    }


    let active_assets =
        trades
            .iter_active()
            .map(|trade| trade.symbol.as_str())
            .collect::<HashSet<_>>()
            .len();


    if active_assets >= max {
        return Err(
            RiskCheckError::MaxActiveAssets {
                current: active_assets,
                max,
            }
        );
    }


    Ok(())
}


// ============================================================
// ACTIVE TRADES PER SYMBOL
// ============================================================

/// Checks whether another active Trade may be created for one symbol.
///
/// This limit is engine-wide and includes Trades from all strategies.
pub fn check_active_trades_per_symbol(
    limits: &RiskLimits,
    trades: &Trades,
    symbol: &str,
) -> Result<(), RiskCheckError> {
    let Some(max) =
        limits.max_active_trades_per_symbol
    else {
        return Ok(());
    };


    let current =
        trades
            .iter_active()
            .filter(
                |trade| trade.symbol == symbol
            )
            .count();


    if current >= max {
        return Err(
            RiskCheckError::MaxActiveTradesPerSymbol {
                symbol:
                    symbol.to_string(),
                current,
                max,
            }
        );
    }


    Ok(())
}


// ============================================================
// ACTIVE TRADES PER STRATEGY
// ============================================================

/// Checks whether one strategy may create another active Trade.
///
/// The count spans all symbols owned by that strategy.
pub fn check_active_trades_per_strategy(
    limits: &RiskLimits,
    trades: &Trades,
    strategy_id: &str,
) -> Result<(), RiskCheckError> {
    let Some(max) =
        limits.max_active_trades_per_strategy
    else {
        return Ok(());
    };


    let current =
        trades
            .iter_active()
            .filter(
                |trade| {
                    trade.strategy_id
                        == strategy_id
                }
            )
            .count();


    if current >= max {
        return Err(
            RiskCheckError::MaxActiveTradesPerStrategy {
                strategy_id:
                    strategy_id.to_string(),
                current,
                max,
            }
        );
    }


    Ok(())
}


// ============================================================
// ORDER NOTIONAL LIMIT
// ============================================================

/// Checks the requested notional of a single Order.
pub fn check_order_notional(
    limits: &RiskLimits,
    requested_notional: f64,
) -> Result<(), RiskCheckError> {
    if !requested_notional.is_finite()
        || requested_notional <= 0.0
    {
        return Err(
            RiskCheckError::InvalidNotional {
                notional:
                    requested_notional,
            }
        );
    }


    if let Some(max) =
        limits.max_order_notional
    {
        if requested_notional > max {
            return Err(
                RiskCheckError::MaxOrderNotional {
                    requested:
                        requested_notional,
                    max,
                }
            );
        }
    }


    Ok(())
}


// ============================================================
// GROSS NOTIONAL LIMIT
// ============================================================

/// Checks projected gross Engine exposure.
///
/// `current_gross_notional` should represent established exposure already
/// held by the Engine.
///
/// Active entry reservations are added automatically so pending Orders cannot
/// bypass the gross exposure limit.
///
/// TODO:
/// Once canonical Position valuation exists, calculate current gross notional
/// directly from Positions rather than accepting it as an argument.
pub fn check_gross_notional(
    limits: &RiskLimits,
    reservations: &RiskReservations,
    current_gross_notional: f64,
    requested_notional: f64,
) -> Result<(), RiskCheckError> {
    let Some(max) =
        limits.max_gross_notional
    else {
        return Ok(());
    };


    let projected =
        current_gross_notional
            + reservations.total_entry_notional()
            + requested_notional;


    if projected > max {
        return Err(
            RiskCheckError::MaxGrossNotional {
                projected,
                max,
            }
        );
    }


    Ok(())
}


// ============================================================
// SYMBOL NOTIONAL LIMIT
// ============================================================

/// Checks projected exposure for a single symbol.
///
/// TODO:
/// Once canonical Position valuation exists, derive current symbol exposure
/// directly from Positions.
pub fn check_symbol_notional(
    limits: &RiskLimits,
    symbol: &str,
    current_symbol_notional: f64,
    reserved_symbol_notional: f64,
    requested_notional: f64,
) -> Result<(), RiskCheckError> {
    let Some(max) =
        limits.max_symbol_notional
    else {
        return Ok(());
    };


    let projected =
        current_symbol_notional
            + reserved_symbol_notional
            + requested_notional;


    if projected > max {
        return Err(
            RiskCheckError::MaxSymbolNotional {
                symbol:
                    symbol.to_string(),
                projected,
                max,
            }
        );
    }


    Ok(())
}


// ============================================================
// EXIT QUANTITY
// ============================================================

/// Checks whether an exit Order may reserve the requested Trade quantity.
///
/// Existing exit reservations are subtracted from the Trade's current
/// quantity so multiple working exit Orders cannot reserve the same exposure.
pub fn check_exit_quantity(
    trade: &Trade,
    reservations: &RiskReservations,
    requested_quantity: f64,
) -> Result<(), RiskCheckError> {
    if !requested_quantity.is_finite()
        || requested_quantity <= 0.0
    {
        return Err(
            RiskCheckError::InvalidQuantity {
                quantity:
                    requested_quantity,
            }
        );
    }


    let already_reserved =
        reservations
            .exit_quantity_for_trade(
                trade.id
            );


    let available =
        (
            trade.quantity
                - already_reserved
        )
            .max(0.0);


    if requested_quantity > available {
        return Err(
            RiskCheckError::InsufficientExitQuantity {
                requested:
                    requested_quantity,
                available,
            }
        );
    }


    Ok(())
}