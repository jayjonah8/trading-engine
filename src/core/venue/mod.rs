#![allow(dead_code)]

pub mod account;
pub mod account_details;
pub mod capabilities;
pub mod event;
pub mod instrument;
pub mod state;
pub mod order_validation;


// ============================================================
// ACCOUNT
// ============================================================

pub use account::{
    AccountDetails,
    AccountState,
    VenueAccountKey,
};


// ============================================================
// ACCOUNT DETAILS
// ============================================================

pub use account_details::{
    AlpacaAccountState,
    HyperliquidAccountState,
    IbkrAccountState,
};


// ============================================================
// EVENT
// ============================================================

pub use event::VenueStateEvent;


// ============================================================
// INSTRUMENT
// ============================================================

pub use instrument::{
    InstrumentKey,
    InstrumentState,
};


// ============================================================
// VENUE STATE
// ============================================================

pub use state::VenueState;

pub use capabilities::{
    FractionalQuantityMode,
    VenueCapabilities,
};


// ============================================================
// ORDER VALIDATION
// ============================================================

pub use order_validation::{validate_order, OrderValidationError};