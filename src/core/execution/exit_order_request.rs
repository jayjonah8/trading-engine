#![allow(dead_code)]

use crate::core::execution::{
    OrderType,
    TimeInForce,
};


// ============================================================
// EXIT ORDER REQUEST
// ============================================================

/// Canonical execution instructions for closing an existing directional Trade.
///
/// `ExitOrderRequest` describes HOW the Engine should execute an exit.
///
/// It does NOT identify:
///
/// - which Trade is being closed
/// - the exit side
/// - the exit quantity
/// - whether the order is reduce-only
///
/// Those properties are derived by the Engine from the authoritative Trade.
///
/// Examples:
///
/// Immediate market exit:
///
///     ExitOrderRequest::market()
///
/// Resting limit exit:
///
///     ExitOrderRequest::limit(
///         205.50,
///         TimeInForce::Gtc,
///         false,
///     )
///
/// Runtime flow:
///
///     EngineEvent::CloseTrade
///         ↓
///     ExitOrderRequest
///         ↓
///     handle_close_trade()
///         ↓
///     canonical Order
///         ↓
///     ExecCommand::SubmitOrder
///         ↓
///     execution router
///         ↓
///     venue adapter
///
/// IMPORTANT:
///
/// `reduce_only` intentionally does NOT exist on this structure.
///
/// A directional Trade close must always be reduce-only. The Engine owns that
/// invariant and sets:
///
///     reduce_only = true
///
/// when constructing the canonical exit Order.
///
/// This prevents strategies or other callers from accidentally creating an
/// exit request that can increase or reverse exposure.
#[derive(Debug, Clone)]
pub struct ExitOrderRequest {
    /// Execution type used for the exit order.
    pub order_type: OrderType,

    /// Determines how long the exit order may remain active.
    pub time_in_force: TimeInForce,

    /// Limit price when required by the selected order type.
    pub limit_px: Option<f64>,

    /// Stop trigger price when required by the selected order type.
    pub stop_px: Option<f64>,

    /// Requires the order to add liquidity when supported by the venue.
    ///
    /// Only meaningful for limit-style orders.
    pub post_only: bool,
}


// ============================================================
// CONSTRUCTORS
// ============================================================

impl ExitOrderRequest {
    /// Creates an immediate market exit.
    ///
    /// Canonical behavior:
    ///
    ///     order_type    = Market
    ///     time_in_force = IOC
    ///     limit_px      = None
    ///     stop_px       = None
    ///     post_only     = false
    ///
    /// `handle_close_trade()` will additionally enforce:
    ///
    ///     reduce_only = true
    pub const fn market() -> Self {
        Self {
            order_type:
                OrderType::Market,

            time_in_force:
                TimeInForce::Ioc,

            limit_px:
                None,

            stop_px:
                None,

            post_only:
                false,
        }
    }


    /// Creates a limit exit.
    ///
    /// The caller supplies:
    ///
    /// - the desired limit price
    /// - time in force
    /// - whether the order should be post-only
    ///
    /// `handle_close_trade()` remains responsible for determining the
    /// canonical exit side and quantity from the authoritative Trade and
    /// enforcing `reduce_only = true`.
    pub const fn limit(
        limit_px: f64,
        time_in_force: TimeInForce,
        post_only: bool,
    ) -> Self {
        Self {
            order_type:
                OrderType::Limit,

            time_in_force,

            limit_px:
                Some(limit_px),

            stop_px:
                None,

            post_only,
        }
    }
}


// ============================================================
// VALIDATION
// ============================================================

/// Error returned when exit execution instructions are invalid.
#[derive(Debug, Clone, PartialEq)]
pub enum ExitOrderRequestError {
    MissingLimitPrice {
        order_type: OrderType,
    },

    UnexpectedLimitPrice {
        order_type: OrderType,
    },

    InvalidLimitPrice {
        price: f64,
    },

    MissingStopPrice {
        order_type: OrderType,
    },

    UnexpectedStopPrice {
        order_type: OrderType,
    },

    InvalidStopPrice {
        price: f64,
    },

    PostOnlyRequiresLimitOrder {
        order_type: OrderType,
    },
}


impl ExitOrderRequest {
    /// Validates the execution instructions before an exit Order is created.
    ///
    /// This mirrors the same order-shape invariants enforced for entry
    /// `TradeIntent`s.
    pub fn validate(
        &self,
    ) -> Result<(), ExitOrderRequestError> {
        match self.order_type {
            // ========================================================
            // MARKET
            // ========================================================

            OrderType::Market => {
                if self.limit_px.is_some() {
                    return Err(
                        ExitOrderRequestError::UnexpectedLimitPrice {
                            order_type:
                                self.order_type,
                        }
                    );
                }

                if self.stop_px.is_some() {
                    return Err(
                        ExitOrderRequestError::UnexpectedStopPrice {
                            order_type:
                                self.order_type,
                        }
                    );
                }
            }


            // ========================================================
            // LIMIT
            // ========================================================

            OrderType::Limit => {
                let limit_px =
                    self.limit_px.ok_or(
                        ExitOrderRequestError::MissingLimitPrice {
                            order_type:
                                self.order_type,
                        }
                    )?;

                validate_price(
                    limit_px,
                    |price| {
                        ExitOrderRequestError::InvalidLimitPrice {
                            price,
                        }
                    },
                )?;

                if self.stop_px.is_some() {
                    return Err(
                        ExitOrderRequestError::UnexpectedStopPrice {
                            order_type:
                                self.order_type,
                        }
                    );
                }
            }


            // ========================================================
            // STOP MARKET
            // ========================================================

            OrderType::StopMarket => {
                let stop_px =
                    self.stop_px.ok_or(
                        ExitOrderRequestError::MissingStopPrice {
                            order_type:
                                self.order_type,
                        }
                    )?;

                validate_price(
                    stop_px,
                    |price| {
                        ExitOrderRequestError::InvalidStopPrice {
                            price,
                        }
                    },
                )?;

                if self.limit_px.is_some() {
                    return Err(
                        ExitOrderRequestError::UnexpectedLimitPrice {
                            order_type:
                                self.order_type,
                        }
                    );
                }
            }


            // ========================================================
            // STOP LIMIT
            // ========================================================

            OrderType::StopLimit => {
                let limit_px =
                    self.limit_px.ok_or(
                        ExitOrderRequestError::MissingLimitPrice {
                            order_type:
                                self.order_type,
                        }
                    )?;

                let stop_px =
                    self.stop_px.ok_or(
                        ExitOrderRequestError::MissingStopPrice {
                            order_type:
                                self.order_type,
                        }
                    )?;

                validate_price(
                    limit_px,
                    |price| {
                        ExitOrderRequestError::InvalidLimitPrice {
                            price,
                        }
                    },
                )?;

                validate_price(
                    stop_px,
                    |price| {
                        ExitOrderRequestError::InvalidStopPrice {
                            price,
                        }
                    },
                )?;
            }
        }


        // ============================================================
        // POST ONLY
        // ============================================================

        if self.post_only
            && !matches!(
                self.order_type,
                OrderType::Limit
                    | OrderType::StopLimit
            )
        {
            return Err(
                ExitOrderRequestError::PostOnlyRequiresLimitOrder {
                    order_type:
                        self.order_type,
                }
            );
        }


        Ok(())
    }
}


// ============================================================
// HELPERS
// ============================================================

fn validate_price<F>(
    price: f64,
    error: F,
) -> Result<(), ExitOrderRequestError>
where
    F: FnOnce(f64) -> ExitOrderRequestError,
{
    if !price.is_finite()
        || price <= 0.0
    {
        return Err(
            error(price)
        );
    }

    Ok(())
}