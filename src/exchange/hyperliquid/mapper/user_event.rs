#![allow(dead_code)]

use hyperliquid_rust_sdk::{
    User,
    UserData,
};

use tokio::sync::mpsc::UnboundedSender;

use crate::{
    core::execution::{
        fill::allocate_execution_id,
        ExecEvent,
    },

    exchange::hyperliquid::{
        correlation::HyperliquidOrderCorrelation,

        mapper::fill::{
            map_trade_info_to_fill,
        },
    },

    shared::print_log,
};


// ============================================================
// MAP HYPERLIQUID USER EVENT
// ============================================================

/// Translates one Hyperliquid `User` WebSocket message into canonical
/// execution events.
///
/// Hyperliquid UserEvents currently contains:
///
/// - Fills
/// - Funding
/// - Liquidation
/// - NonUserCancel
///
/// The execution adapter currently consumes `UserData::Fills` because each
/// `TradeInfo` represents one actual order execution.
///
/// Flow:
///
///     Hyperliquid UserEvents
///         ↓
///     UserData::Fills
///         ↓
///     one TradeInfo per execution
///         ↓
///     mapper/fill.rs
///         ↓
///     canonical Fill
///         ↓
///     ExecEvent::Fill
///         ↓
///     Engine
///
/// Order lifecycle events such as Working and Canceled are handled separately
/// through `mapper/order_update.rs`.
pub fn handle_user_event(
    user_event: User,
    correlation: &HyperliquidOrderCorrelation,
    tx_evt: &UnboundedSender<ExecEvent>,
    debug_mode: bool,
) {
    match user_event.data {

        // ========================================================
        // FILLS
        // ========================================================

        UserData::Fills(trades) => {
            for trade in trades {
                // =================================================
                // CANONICAL EXECUTION ID
                // =================================================

                // Allocate a venue-independent Engine execution identity.
                //
                // Hyperliquid's real venue execution identity (`tid`) is NOT
                // used as ExecutionId. It remains stored separately as:
                //
                //     Fill.venue_execution_id
                //
                // This avoids numeric execution-ID collisions between
                // different execution venues.
                let execution_id =
                    allocate_execution_id();


                // =================================================
                // MAP TRADE INFO -> FILL
                // =================================================

                let Some(fill) =
                    map_trade_info_to_fill(
                        trade,
                        correlation,
                        execution_id,
                        debug_mode,
                    )
                else {
                    continue;
                };


                // =================================================
                // LOG
                // =================================================

                print_log(
                    debug_mode,
                    true,
                    format!(
                        "[HYPERLIQUID MAPPER] FILL MAPPED | order_id={} | execution_id={:?} | venue_execution_id={:?} | symbol={} | side={:?} | qty={} | price={} | fee={:?} | ts_ms={}",
                        fill.order_id,
                        fill.execution_id,
                        fill.venue_execution_id,
                        fill.symbol,
                        fill.side,
                        fill.quantity,
                        fill.price,
                        fill.fee,
                        fill.ts_ms,
                    ),
                );


                // =================================================
                // EMIT CANONICAL EXEC EVENT
                // =================================================

                let _ =
                    tx_evt.send(
                        ExecEvent::Fill {
                            fill,
                        }
                    );
            }
        }


        // ========================================================
        // FUNDING
        // ========================================================

        UserData::Funding(funding) => {
            // Funding events are valid Hyperliquid account events but are not
            // part of the canonical order-execution lifecycle yet.
            //
            // If funding accounting is added to Engine state later, map it
            // through its own canonical event rather than treating it as Fill.
            print_log(
                debug_mode,
                true,
                format!(
                    "[HYPERLIQUID MAPPER] FUNDING EVENT IGNORED | event={:?}",
                    funding,
                ),
            );
        }


        // ========================================================
        // LIQUIDATION
        // ========================================================

        UserData::Liquidation(liquidation) => {
            // Liquidation requires its own explicit engine/reconciliation
            // handling if supported later. It must not be disguised as a
            // normal strategy-generated order execution.
            print_log(
                debug_mode,
                true,
                format!(
                    "[HYPERLIQUID MAPPER] LIQUIDATION EVENT IGNORED | event={:?}",
                    liquidation,
                ),
            );
        }


        // ========================================================
        // NON-USER CANCEL
        // ========================================================

        UserData::NonUserCancel(cancels) => {
            // Hyperliquid can report cancellations that were not initiated by
            // the normal user command path.
            //
            // Do not guess their canonical meaning yet. OrderUpdates remains
            // the current authoritative lifecycle stream for cancellations.
            print_log(
                debug_mode,
                true,
                format!(
                    "[HYPERLIQUID MAPPER] NON-USER CANCEL EVENT IGNORED | events={:?}",
                    cancels,
                ),
            );
        }
    }
}