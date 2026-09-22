#![allow(dead_code)]



// TODO: should work with paper trading or live trading
// Eventually we’ll move the address/client ID into your main Config or environment 
// configuration. Hardcoding it here is fine while we prove the adapter.

// ============================================================
// IBKR CONFIG
// ============================================================

/// Interactive Brokers TWS / Gateway connection configuration.
///
/// The paper-trading TWS port is normally 7497.
///
/// `client_id` identifies this API connection to TWS and must not conflict
/// with another active client using the same TWS instance.
#[derive(Debug, Clone)]
pub struct IbkrConfig {
    pub address: String,
    pub client_id: i32,
}

impl Default for IbkrConfig {
    fn default() -> Self {
        Self {
            address: "127.0.0.1:7497".to_string(),
            client_id: 200,
        }
    }
}