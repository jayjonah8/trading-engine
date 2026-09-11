// TODO:
// Implement Alpaca `trade_updates` -> canonical `ExecEvent` translation once
// the new engine execution model is finalized.
//
// This module should eventually restore the proven behavior from the older bot,
// including:
//
// - new/resting order acknowledgement
// - partial fills
// - final fills
// - rejected orders
// - canceled orders
// - expired orders
// - execution IDs
// - venue order ID correlation
// - individual fill price / quantity
// - cumulative filled quantity
// - cumulative average fill price
//
// IMPORTANT:
// Keep individual execution data separate from cumulative order state.
//
// For Alpaca:
//
// - `fill_sz` / `fill_px` describe the current execution.
// - `total_sz` / `avg_px` describe cumulative order state.
//
// `total_sz` must never be added to the previously recorded cumulative fill
// quantity because Alpaca already includes all prior fills in that value.
//
// The mapper should remain venue-specific and should not mutate canonical
// engine state directly. Its responsibility is only:
//
// raw Alpaca JSON
//      ↓
// correlation / normalization
//      ↓
// canonical `ExecEvent`
//
// Canonical order, fill, trade, position, and risk state remain owned by the
// engine.
//
// TODO:
// Revisit the temporary Alpaca order-correlation model once canonical engine
// `OrderId`, broker order ID, and order role/purpose are wired end-to-end.