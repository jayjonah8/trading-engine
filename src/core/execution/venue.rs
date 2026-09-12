#![allow(dead_code)]

/// Execution venue supported by the trading engine.
///
/// This is a canonical engine type. Venue-specific API behavior belongs
/// inside the corresponding execution adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Venue {
    Alpaca,
    InteractiveBrokers,
    Hyperliquid,
}