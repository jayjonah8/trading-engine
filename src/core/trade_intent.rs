#![allow(dead_code)]

use crate::core::{
    execution::order::{
        OrderType,
        TimeInForce,
    },
    trades::Direction,
    risk::TrailingStopConfig,
};


// TradeIntent::validate() validates the strategy-level request before the Engine
// constructs the final canonical Order.
//
// validate_order() separately validates the final canonical Order after
// venue/instrument normalization and construction.


/// Strategy request to create a new directional trade.
///
/// `TradeIntent` describes:
///
/// - what exposure the strategy wants
/// - how the initial entry should be executed
/// - optional engine-managed trade risk rules
///
/// It is NOT an exchange order.
///
/// The engine converts it through:
///
/// TradeIntent
///     -> Trade
///     -> Risk Checks
///     -> RiskReservation
///     -> Order
///     -> ExecCommand
///
/// TODO: Replace temporary primitive types later with canonical types:
///
/// - `String` strategy ID -> `StrategyId`
/// - `String` symbol -> `Symbol`
/// - `f64` prices -> `Price`
/// - `f64` monetary values -> `Money`
#[derive(Debug, Clone)]
pub struct TradeIntent {
    /// Strategy generating this request.
    pub strategy_id: String,

    /// Instrument the strategy wants to trade.
    pub symbol: String,

    /// Directional exposure requested by the strategy.
    ///
    /// This is intentionally different from execution `Side`.
    ///
    /// Long entry  -> Buy
    /// Short entry -> Sell
    pub direction: Direction,

    /// Market/reference price observed when the strategy generated the signal.
    ///
    /// This is the strategy's signal price, not necessarily the final
    /// execution price.
    pub signal_px: f64,

    /// Requested trade size expressed as notional value.
    ///
    /// Example:
    ///
    /// `100.0` means approximately $100 of exposure.
    pub target_notional: f64,

    /// Strategy-defined capital allocated to this trade.
    ///
    /// Used for basket profitability calculations.
    /// Independent of actual venue margin requirements.
    pub margin_usd: f64,

    // ============================================================
    // ENTRY EXECUTION
    // ============================================================

    /// Execution type used for the initial entry order.
    ///
    /// Examples:
    ///
    /// - Market
    /// - Limit
    /// - StopMarket
    /// - StopLimit
    pub order_type: OrderType,

    /// Determines how long the initial entry order may remain active.
    pub time_in_force: TimeInForce,

    /// Limit price used when required by the selected order type.
    ///
    /// Required for:
    ///
    /// - `OrderType::Limit`
    /// - `OrderType::StopLimit`
    ///
    /// Must be `None` for:
    ///
    /// - `OrderType::Market`
    /// - `OrderType::StopMarket`
    pub limit_px: Option<f64>,

    /// Stop trigger price used when required by the selected order type.
    ///
    /// Required for:
    ///
    /// - `OrderType::StopMarket`
    /// - `OrderType::StopLimit`
    ///
    /// Must be `None` for:
    ///
    /// - `OrderType::Market`
    /// - `OrderType::Limit`
    pub stop_px: Option<f64>,

    /// Requires the entry order to add liquidity when supported by the venue.
    ///
    /// This is only valid for limit-style orders.
    pub post_only: bool,

    // ============================================================
    // TRADE RISK PLAN
    // ============================================================

    /// Optional engine-managed take-profit percentage.
    ///
    /// `None` means the strategy does not want this engine-managed exit rule.
    pub take_profit_percent: Option<f64>,

    /// Optional engine-managed normal stop-loss percentage.
    ///
    /// Respects and works with stop_loss_delay_secs before triggering.
    pub stop_loss_percent: Option<f64>,

    /// Optional engine-managed emergency stop-loss percentage.
    pub emergency_stop_percent: Option<f64>,

    /// Optional delay before the normal stop-loss may activate.
    ///
    /// Only applies to stop_loss_percent.
    /// Does not apply to trailing_stop or emergency_stop_percent.
    ///
    /// Example:
    ///
    /// `Some(30)` means normal stop-loss checks begin 30 seconds after entry.
    pub stop_loss_delay_secs: Option<u64>,

    /// Optional maximum lifetime of the trade.
    ///
    /// `None` means no engine-managed duration exit.
    pub trade_duration_secs: Option<u64>,

    /// Optional engine-managed trailing-stop configuration.
    ///
    /// None disables trailing-stop protection.
    ///
    /// Supports fixed and dynamically tightening trailing stops.
    /// Does not respect or work with stop_loss_delay_secs.
    pub trailing_stop: Option<TrailingStopConfig>,

    /// Event timestamp when the strategy generated the intent.
    ///
    /// Live trading uses live event time.
    /// Historical replay uses historical event time.
    pub ts_ms: i64,
}

/// Error returned when a strategy submits an invalid `TradeIntent`.
///
/// Validation happens before any Trade, Order, or RiskReservation is created,
/// preventing malformed requests from entering authoritative engine state.
#[derive(Debug, Clone, PartialEq)]
pub enum TradeIntentError {
    EmptyStrategyId,
    EmptySymbol,

    InvalidTimestamp {
        ts_ms: i64,
    },

    InvalidSignalPrice {
        price: f64,
    },

    InvalidTargetNotional {
        notional: f64,
    },

    InvalidMargin {
        margin_usd: f64,
    },

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

    InvalidTakeProfitPercent {
        percent: f64,
    },

    InvalidStopLossPercent {
        percent: f64,
    },

    InvalidEmergencyStopPercent {
        percent: f64,
    },

    // ============================================================
    // TRAILING STOP VALIDATION ERROR
    // ============================================================

    /// Invalid engine-managed trailing-stop configuration.
    ///
    /// Contains the specific reason returned by
    /// TrailingStopConfig::validate().
    InvalidTrailingStopConfig {
        reason: String,
    },
}

impl TradeIntent {
    /// Validates the complete trade request before the engine accepts it.
    pub fn validate(&self) -> Result<(), TradeIntentError> {
        self.validate_identity()?;
        self.validate_signal_and_size()?;
        self.validate_entry_order()?;
        self.validate_risk_plan()?;

        Ok(())
    }

    /// Converts the strategy-level direction into the execution side required
    /// for the initial entry order.
    ///
    /// Long  -> Buy
    /// Short -> Sell
    pub const fn entry_side(&self) -> crate::core::execution::Side {
        use crate::core::execution::Side;

        match self.direction {
            Direction::Long => Side::Buy,
            Direction::Short => Side::Sell,
        }
    }

    fn validate_identity(&self) -> Result<(), TradeIntentError> {
        if self.strategy_id.trim().is_empty() {
            return Err(TradeIntentError::EmptyStrategyId);
        }

        if self.symbol.trim().is_empty() {
            return Err(TradeIntentError::EmptySymbol);
        }

        if self.ts_ms <= 0 {
            return Err(
                TradeIntentError::InvalidTimestamp {
                    ts_ms: self.ts_ms,
                }
            );
        }

        Ok(())
    }

    fn validate_signal_and_size(&self) -> Result<(), TradeIntentError> {
        if !valid_positive_number(self.signal_px) {
            return Err(
                TradeIntentError::InvalidSignalPrice {
                    price: self.signal_px,
                }
            );
        }

        if !valid_positive_number(self.target_notional) {
            return Err(
                TradeIntentError::InvalidTargetNotional {
                    notional: self.target_notional,
                }
            );
        }

        if !valid_positive_number(self.margin_usd) {
            return Err(
                TradeIntentError::InvalidMargin {
                    margin_usd: self.margin_usd,
                }
            );
        }

        Ok(())
    }

    fn validate_entry_order(&self) -> Result<(), TradeIntentError> {
        match self.order_type {
            OrderType::Market => {
                // A pure market entry requires neither limit nor stop price.
                if self.limit_px.is_some() {
                    return Err(
                        TradeIntentError::UnexpectedLimitPrice {
                            order_type: self.order_type,
                        }
                    );
                }

                if self.stop_px.is_some() {
                    return Err(
                        TradeIntentError::UnexpectedStopPrice {
                            order_type: self.order_type,
                        }
                    );
                }
            }

            OrderType::Limit => {
                // A limit order must provide its executable limit price.
                let limit_px = self.limit_px.ok_or(
                    TradeIntentError::MissingLimitPrice {
                        order_type: self.order_type,
                    }
                )?;

                validate_limit_price(limit_px)?;

                if self.stop_px.is_some() {
                    return Err(
                        TradeIntentError::UnexpectedStopPrice {
                            order_type: self.order_type,
                        }
                    );
                }
            }

            OrderType::StopMarket => {
                // A stop-market order needs only a trigger price.
                let stop_px = self.stop_px.ok_or(
                    TradeIntentError::MissingStopPrice {
                        order_type: self.order_type,
                    }
                )?;

                validate_stop_price(stop_px)?;

                if self.limit_px.is_some() {
                    return Err(
                        TradeIntentError::UnexpectedLimitPrice {
                            order_type: self.order_type,
                        }
                    );
                }
            }

            OrderType::StopLimit => {
                // A stop-limit order needs both the trigger and limit prices.
                let limit_px = self.limit_px.ok_or(
                    TradeIntentError::MissingLimitPrice {
                        order_type: self.order_type,
                    }
                )?;

                let stop_px = self.stop_px.ok_or(
                    TradeIntentError::MissingStopPrice {
                        order_type: self.order_type,
                    }
                )?;

                validate_limit_price(limit_px)?;
                validate_stop_price(stop_px)?;
            }
        }

        // Post-only only makes sense for an order that can rest on the book.
        if self.post_only &&
            !matches!(
                self.order_type,
                OrderType::Limit | OrderType::StopLimit
            )
        {
            return Err(
                TradeIntentError::PostOnlyRequiresLimitOrder {
                    order_type: self.order_type,
                }
            );
        }

        Ok(())
    }

    fn validate_risk_plan(&self) -> Result<(), TradeIntentError> {
        if let Some(percent) = self.take_profit_percent {
            if !valid_percent(percent) {
                return Err(
                    TradeIntentError::InvalidTakeProfitPercent {
                        percent,
                    }
                );
            }
        }

        if let Some(percent) = self.stop_loss_percent {
            if !valid_percent(percent) {
                return Err(
                    TradeIntentError::InvalidStopLossPercent {
                        percent,
                    }
                );
            }
        }

        if let Some(percent) = self.emergency_stop_percent {
            if !valid_percent(percent) {
                return Err(
                    TradeIntentError::InvalidEmergencyStopPercent {
                        percent,
                    }
                );
            }
        }

        // ============================================================
        // VALIDATE TRAILING STOP LOSS
        // ============================================================

        // Trailing-stop protection is optional.
        //
        // None:
        //     Trailing-stop protection is disabled.
        //
        // Some(config):
        //     Validate all configured profit thresholds
        //     and trailing distances.
        //
        // Invalid configurations return TradeIntentError
        // without panicking or terminating the engine.
        // See core/risk/trailing_stop.rs for specific trailing_stop checks.
        if let Some(trailing_stop) = &self.trailing_stop {
            trailing_stop
                .validate()
                .map_err(|reason| {
                    TradeIntentError::InvalidTrailingStopConfig {
                        reason,
                    }
                })?;
        }

        Ok(())
    }
}

/// Returns true only for finite values greater than zero.
///
/// This rejects:
///
/// - zero
/// - negative values
/// - NaN
/// - positive/negative infinity
fn valid_positive_number(value: f64) -> bool {
    value.is_finite() && value > 0.0
}

fn valid_percent(percent: f64) -> bool {
    valid_positive_number(percent)
}

fn validate_limit_price(
    price: f64,
) -> Result<(), TradeIntentError> {
    if !valid_positive_number(price) {
        return Err(
            TradeIntentError::InvalidLimitPrice {
                price,
            }
        );
    }

    Ok(())
}

fn validate_stop_price(
    price: f64,
) -> Result<(), TradeIntentError> {
    if !valid_positive_number(price) {
        return Err(
            TradeIntentError::InvalidStopPrice {
                price,
            }
        );
    }

    Ok(())
}