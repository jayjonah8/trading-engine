#![allow(dead_code)]

use crate::core::execution::{Order, OrderType};


// validate_order() validates the final canonical Order after venue/instrument
// normalization and construction.
//
// TradeIntent::validate() separately validates the original strategy-level
// request before the Engine constructs the canonical Order.


// ============================================================================
// ORDER VALIDATION ERROR
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum OrderValidationError {
    InvalidQuantity(f64),
    MissingLimitPrice,
    UnexpectedLimitPrice,
    MissingStopPrice,
    UnexpectedStopPrice,
    InvalidLimitPrice(f64),
    InvalidStopPrice(f64),
    PostOnlyRequiresLimitOrder,
}


// ============================================================================
// VALIDATE ORDER
// ============================================================================

/// Validates the canonical structure of an Order before it is submitted to an
/// execution venue.
///
/// This layer validates rules that apply to the canonical Order itself.
///
/// Venue-specific capability rules such as supported time-in-force values,
/// post-only behavior, reduce-only behavior, and venue-specific order support
/// can be added here as those behaviors are proven by execution testing.
pub fn validate_order(order: &Order) -> Result<(), OrderValidationError> {
    // ============================================================
    // QUANTITY
    // ============================================================

    if !order.quantity.is_finite() || order.quantity <= 0.0 {
        return Err(OrderValidationError::InvalidQuantity(order.quantity));
    }


    // ============================================================
    // ORDER TYPE / PRICE STRUCTURE
    // ============================================================

    match order.order_type {
        OrderType::Market => {
            if order.limit_px.is_some() {
                return Err(OrderValidationError::UnexpectedLimitPrice);
            }

            if order.stop_px.is_some() {
                return Err(OrderValidationError::UnexpectedStopPrice);
            }
        }

        OrderType::Limit => {
            let Some(limit_px) = order.limit_px else {
                return Err(OrderValidationError::MissingLimitPrice);
            };

            if !limit_px.is_finite() || limit_px <= 0.0 {
                return Err(OrderValidationError::InvalidLimitPrice(limit_px));
            }

            if order.stop_px.is_some() {
                return Err(OrderValidationError::UnexpectedStopPrice);
            }
        }

        OrderType::StopMarket => {
            let Some(stop_px) = order.stop_px else {
                return Err(OrderValidationError::MissingStopPrice);
            };

            if !stop_px.is_finite() || stop_px <= 0.0 {
                return Err(OrderValidationError::InvalidStopPrice(stop_px));
            }

            if order.limit_px.is_some() {
                return Err(OrderValidationError::UnexpectedLimitPrice);
            }
        }

        OrderType::StopLimit => {
            let Some(limit_px) = order.limit_px else {
                return Err(OrderValidationError::MissingLimitPrice);
            };

            let Some(stop_px) = order.stop_px else {
                return Err(OrderValidationError::MissingStopPrice);
            };

            if !limit_px.is_finite() || limit_px <= 0.0 {
                return Err(OrderValidationError::InvalidLimitPrice(limit_px));
            }

            if !stop_px.is_finite() || stop_px <= 0.0 {
                return Err(OrderValidationError::InvalidStopPrice(stop_px));
            }
        }
    }


    // ============================================================
    // POST ONLY
    // ============================================================

    if order.post_only && order.order_type != OrderType::Limit {
        return Err(OrderValidationError::PostOnlyRequiresLimitOrder);
    }


    Ok(())
}