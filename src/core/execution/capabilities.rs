use crate::core::execution::order::{
    OrderType,
    TimeInForce,
};

// This eventually allows:
//
// Order
//   ↓
// Risk validation
//   ↓
// Venue capability validation
//   ↓
// ExecCommand::SubmitOrder
//   ↓
// Exchange adapter

// This lets the core/router ask what an execution venue supports without 
// writing if venue == Hyperliquid throughout the engine etc.
//
// For example, Hyperliquid, Alpaca, and IBKR will each construct their 
// own VenueCapabilities. I would not overbuild this yet with every possible 
// exchange feature. We can add capabilities as we actually encounter 
// differences between the three venues.


/// Describes the execution features supported by a venue.
///
/// The core engine remains venue-independent. Each exchange adapter exposes
/// its capabilities so orders can be validated before they are submitted.
///
/// For example, a venue may support limit orders and GTC but not FOK,
/// stop orders, post-only orders, or order replacement.
#[derive(Debug, Clone)]
pub struct VenueCapabilities {
    /// Supported canonical order types.
    pub order_types: Vec<OrderType>,

    /// Supported time-in-force policies.
    pub time_in_force: Vec<TimeInForce>,

    /// Whether the venue supports post-only orders.
    pub supports_post_only: bool,

    /// Whether the venue supports reduce-only orders.
    pub supports_reduce_only: bool,

    /// Whether an active order can be replaced or modified.
    pub supports_replace: bool,

    /// Whether fractional quantities are supported.
    pub supports_fractional_quantity: bool,
}

impl VenueCapabilities {
    /// Returns true if the venue supports the requested order type.
    pub fn supports_order_type(&self, order_type: OrderType) -> bool {
        self.order_types.contains(&order_type)
    }

    /// Returns true if the venue supports the requested time-in-force policy.
    pub fn supports_time_in_force(&self, time_in_force: TimeInForce) -> bool {
        self.time_in_force.contains(&time_in_force)
    }
}