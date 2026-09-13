#![allow(dead_code)]

use crate::{
    core::{
        engine::engine::Engine,
        execution::OrderPurpose,
        trades::{
            TradeId,
            TradeState,
        },
    },
    shared::print_log,
};


// ============================================================
// CLOSE ALL TRADES FLOW
// ============================================================

// CloseAllTrades
//     = request closure of every currently Open logical Trade
//
// This handler does NOT duplicate the single-trade close lifecycle.
//
// Instead:
//
// enumerate eligible TradeIds
//     ↓
// skip trades that are not Open
//     ↓
// skip trades that already have exit reservations/orders
//     ↓
// collect TradeIds first
//     ↓
// call handle_close_trade() for each TradeId
//
// handle_close_trade() remains the single authoritative implementation for:
//
// - creating the Exit Order
// - creating the Exit RiskReservation
// - setting Trade -> Closing
// - sending ExecCommand::SubmitOrder
// - rolling back if execution-router delivery fails
//
// Collecting TradeIds before calling handle_close_trade() is important because
// handle_close_trade() mutates Trades, Orders, and RiskState. We must not mutate
// those collections while iterating directly over them.


// ============================================================
// HANDLE CLOSE ALL TRADES
// ============================================================

impl Engine {
    /// Requests closure of every currently Open logical strategy Trade.
    ///
    /// This handler is intentionally orchestration-only. It determines which
    /// Trades are eligible for closure, then delegates each actual close
    /// lifecycle to `handle_close_trade()`.
    ///
    /// Trades are skipped when:
    ///
    /// - they are not `TradeState::Open`
    /// - an exit reservation already exists
    /// - an active exit-like Order already exists
    ///
    /// `Opening`, `Closing`, and `Closed` Trades are not targeted by this
    /// first implementation.
    pub fn handle_close_all_trades(
        &mut self,
        reason: String,
        ts_ms: i64,
    ) {
        // ============================================================
        // COLLECT ELIGIBLE TRADE IDS
        // ============================================================

        let trade_ids: Vec<TradeId> =
            self.state.trades
                .iter()
                .filter_map(
                    |(trade_id, trade)| {
                        // ====================================================
                        // TRADE STATE
                        // ====================================================

                        if trade.state != TradeState::Open {
                            return None;
                        }


                        // ====================================================
                        // EXISTING EXIT RESERVATION
                        // ====================================================

                        let reserved_exit_quantity =
                            self.state
                                .risk
                                .reservations
                                .exit_quantity_for_trade(
                                    *trade_id
                                );

                        if reserved_exit_quantity > f64::EPSILON {
                            return None;
                        }


                        // ====================================================
                        // ACTIVE EXIT ORDER
                        // ====================================================

                        let has_active_exit_order =
                            self.state
                                .orders
                                .active_for_trade(
                                    *trade_id
                                )
                                .any(
                                    |order| {
                                        matches!(
                                            order.purpose,
                                            OrderPurpose::Exit
                                                | OrderPurpose::StopLoss
                                                | OrderPurpose::TakeProfit
                                                | OrderPurpose::EmergencyExit
                                        )
                                    }
                                );

                        if has_active_exit_order {
                            return None;
                        }


                        Some(
                            *trade_id
                        )
                    }
                )
                .collect();


        // ============================================================
        // NOTHING TO CLOSE
        // ============================================================

        if trade_ids.is_empty() {
            print_log(
                self.debug_mode,
                true,
                format!(
                    "[ENGINE] CLOSE ALL TRADES | no eligible open trades | reason={}",
                    reason,
                ),
            );

            return;
        }


        // ============================================================
        // LOG REQUEST
        // ============================================================

        print_log(
            self.debug_mode,
            true,
            format!(
                "[ENGINE] CLOSE ALL TRADES REQUESTED | eligible_trades={} | reason={}",
                trade_ids.len(),
                reason,
            ),
        );


        // ============================================================
        // REQUEST INDIVIDUAL CLOSES
        // ============================================================

        // TradeIds were collected before this loop because
        // handle_close_trade() mutates authoritative engine state.
        //
        // Each Trade receives its own Exit Order and RiskReservation.
        for trade_id in trade_ids {
            self.handle_close_trade(
                trade_id,
                reason.clone(),
                ts_ms,
            );
        }


        // ============================================================
        // COMPLETE
        // ============================================================

        // This does NOT mean every Trade is already closed.
        //
        // It only means close requests were initiated for every eligible
        // Trade. Actual closure still depends on confirmed execution events.
        print_log(
            self.debug_mode,
            true,
            format!(
                "[ENGINE] CLOSE ALL TRADES DISPATCHED | reason={}",
                reason,
            ),
        );
    }
}