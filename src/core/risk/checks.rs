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

// This file should contain the actual pre-trade risk validation logic, but not mutate state.

/// Reason a proposed trading action failed an engine risk check.
#[derive(Debug, Clone, PartialEq)]
pub enum RiskCheckError {
    MaxActiveTrades {
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

/// Checks whether another directional trade may become active.
///
/// Working entry trades count because an `Opening` trade has already committed
/// engine capacity even if its entry order has not fully filled yet.
pub fn check_active_trade_limit(
    limits: &RiskLimits,
    trades: &Trades,
) -> Result<(), RiskCheckError> {
    let Some(max) = limits.max_active_trades else {
        return Ok(());
    };

    let current = trades.count_active();

    if current >= max {
        return Err(RiskCheckError::MaxActiveTrades {
            current,
            max,
        });
    }

    Ok(())
}

/// Checks the requested notional of a single order.
pub fn check_order_notional(
    limits: &RiskLimits,
    requested_notional: f64,
) -> Result<(), RiskCheckError> {
    if !requested_notional.is_finite() || requested_notional <= 0.0 {
        return Err(RiskCheckError::InvalidNotional {
            notional: requested_notional,
        });
    }

    if let Some(max) = limits.max_order_notional {
        if requested_notional > max {
            return Err(RiskCheckError::MaxOrderNotional {
                requested: requested_notional,
                max,
            });
        }
    }

    Ok(())
}

/// Checks projected gross engine exposure.
///
/// `current_gross_notional` should represent established exposure already
/// held by the engine. Active entry reservations are added automatically so
/// pending orders cannot bypass the gross exposure limit.
///
/// TODO: Once canonical position valuation exists, calculate current gross
/// notional directly from `Positions` rather than accepting it as an argument.
pub fn check_gross_notional(
    limits: &RiskLimits,
    reservations: &RiskReservations,
    current_gross_notional: f64,
    requested_notional: f64,
) -> Result<(), RiskCheckError> {
    let Some(max) = limits.max_gross_notional else {
        return Ok(());
    };

    let projected =
        current_gross_notional
            + reservations.total_entry_notional()
            + requested_notional;

    if projected > max {
        return Err(RiskCheckError::MaxGrossNotional {
            projected,
            max,
        });
    }

    Ok(())
}

/// Checks projected exposure for a single symbol.
///
/// TODO: Once canonical position valuation exists, derive current symbol
/// exposure directly from `Positions`.
pub fn check_symbol_notional(
    limits: &RiskLimits,
    symbol: &str,
    current_symbol_notional: f64,
    reserved_symbol_notional: f64,
    requested_notional: f64,
) -> Result<(), RiskCheckError> {
    let Some(max) = limits.max_symbol_notional else {
        return Ok(());
    };

    let projected =
        current_symbol_notional
            + reserved_symbol_notional
            + requested_notional;

    if projected > max {
        return Err(RiskCheckError::MaxSymbolNotional {
            symbol: symbol.to_string(),
            projected,
            max,
        });
    }

    Ok(())
}

/// Checks whether an exit order may reserve the requested trade quantity.
///
/// Existing exit reservations are subtracted from the trade's current
/// quantity so multiple working exit orders cannot reserve the same exposure.
pub fn check_exit_quantity(
    trade: &Trade,
    reservations: &RiskReservations,
    requested_quantity: f64,
) -> Result<(), RiskCheckError> {
    if !requested_quantity.is_finite() || requested_quantity <= 0.0 {
        return Err(RiskCheckError::InvalidQuantity {
            quantity: requested_quantity,
        });
    }

    let already_reserved =
        reservations.exit_quantity_for_trade(trade.id);

    let available =
        (trade.quantity - already_reserved).max(0.0);

    if requested_quantity > available {
        return Err(RiskCheckError::InsufficientExitQuantity {
            requested: requested_quantity,
            available,
        });
    }

    Ok(())
}