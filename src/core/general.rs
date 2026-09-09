// #![allow(dead_code)]

// use crate::exchange::alpaca_utils::AlpacaAccountBalance;


// // ============================================================
// // GENERAL ENGINE STATE
// // ============================================================

// #[derive(Debug, Default, Clone)]
// pub struct General {
//     /// Total realized PnL produced by live broker-executed trades.
//     pub realized_pnl: f64,

//     /// Total realized PnL produced by simulated trades.
//     pub sim_realized_pnl: f64,

//     /// Current account balance used by the engine.
//     ///
//     /// Live and paper trading populate this with account data fetched directly
//     /// from Alpaca. Simulation can use the Alpaca account balance as its starting
//     /// reference, while backtesting can initialize a configurable fake balance
//     /// without making an Alpaca request.
//     pub account_balance: AlpacaAccountBalance,
// }