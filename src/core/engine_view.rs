// #![allow(dead_code)]

// use std::sync::Arc;

// use super::config::Config;
// use super::general::General;

// use crate::core::trades::Trades;

// // Read-only snapshot published by Engine.
// //
// // Strategies and pollers can clone the watch receiver and inspect
// // the latest Engine state without being able to mutate the Engine.
// #[derive(Debug, Clone)]
// pub struct EngineView {
//     pub cfg: Config,
//     pub general: General,
//     pub trades: Trades,
//     // Convenience field so readers don't have to count every time.
//     pub open_trades: usize,
// }

// impl EngineView {
//     /// Build a read-only snapshot from the live Engine state.
//     pub fn from_parts(
//         cfg: &Config,
//         general: &General,
//         trades: &Trades,
//     ) -> Arc<Self> {
//         Arc::new(Self {
//             cfg: cfg.clone(),
//             general: general.clone(),
//             trades: trades.clone(),
//             open_trades: trades.count_open(),
//         })
//     }

//     /// Placeholder snapshot used only when creating the watch channel.
//     ///
//     /// Engine::publish_view() replaces this with the actual runtime
//     /// state once the Engine is initialized.
//     pub fn empty() -> Arc<Self> {

//         Arc::new(Self {
//             cfg: Config::default(),
//             general: General::default(),
//             trades: Trades::default(),
//             open_trades: 0,
//         })
//     }
// }