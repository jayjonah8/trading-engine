#![allow(dead_code)]

use hyperliquid_rust_sdk::OrderUpdates;

use tokio::sync::mpsc::UnboundedSender;

use crate::{
    core::execution::{
        ExecEvent,
        OrderId,
    },

    exchange::hyperliquid::{
        correlation::HyperliquidOrderCorrelation,

        mapper::client_order_id::{
            client_order_id_from_ws_cloid,
        },
    },

    shared::print_log,
};


// ============================================================
// MAP ORDER UPDATES
// ============================================================

/// Translates Hyperliquid `OrderUpdates` WebSocket messages into canonical
/// Engine order-lifecycle events.
///
/// Hyperliquid order updates identify our orders through CLOID:
///
///     Hyperliquid CLOID
///         ↓
///     ClientOrderId
///         ↓
///     HyperliquidOrderCorrelation
///         ↓
///     OrderId
///
/// IMPORTANT:
///
/// This mapper handles ORDER LIFECYCLE only.
///
/// It does NOT create canonical `Fill`s from `"filled"` order statuses.
///
/// Actual executions come from:
///
///     UserEvents::Fills
///
/// because those events contain the individual Hyperliquid execution identity
/// (`tid`) together with actual execution price, quantity, fee, etc.
pub fn handle_order_updates(
    order_updates: OrderUpdates,
    correlation: &HyperliquidOrderCorrelation,
    tx_evt: &UnboundedSender<ExecEvent>,
    debug_mode: bool,
) {
    for update in order_updates.data {
        // ========================================================
        // TIMESTAMP
        // ========================================================

        // Hyperliquid exposes WebSocket timestamps as u64.
        //
        // The canonical engine uses i64 millisecond timestamps consistently
        // across execution events, orders, fills, trades, and market data.
        //
        // Convert at the venue boundary rather than leaking venue-specific
        // integer types into the core engine.
        let ts_ms =
            update.status_timestamp as i64;


        // ========================================================
        // RESOLVE ORDER ID
        // ========================================================

        let Some(order_id) =
            resolve_order_id(
                update.order.cloid.as_deref(),
                correlation,
                debug_mode,
            )
        else {
            print_log(
                debug_mode,
                true,
                format!(
                    "[HYPERLIQUID MAPPER] ORDER UPDATE NOT CORRELATED | oid={} | symbol={} | status={} | cloid={:?}",
                    update.order.oid,
                    update.order.coin,
                    update.status,
                    update.order.cloid,
                ),
            );

            continue;
        };


        // ========================================================
        // STATUS
        // ========================================================

        match update.status.as_str() {

            // ====================================================
            // OPEN / WORKING
            // ====================================================

            "open" => {
                print_log(
                    debug_mode,
                    true,
                    format!(
                        "[HYPERLIQUID MAPPER] ORDER WORKING | order_id={} | oid={} | symbol={} | remaining_sz={}",
                        order_id,
                        update.order.oid,
                        update.order.coin,
                        update.order.sz,
                    ),
                );


                // Hyperliquid has acknowledged the order and it is currently
                // active at the venue.
                //
                // OrderAccepted is emitted from the immediate submission
                // response, where we also obtain the Hyperliquid OID.
                //
                // Therefore this asynchronous lifecycle event only needs to
                // establish canonical Working state.
                let _ =
                    tx_evt.send(
                        ExecEvent::OrderWorking {
                            order_id,
                            ts_ms,
                        }
                    );
            }


            // ====================================================
            // FILLED
            // ====================================================

            "filled" => {
                // IMPORTANT:
                //
                // DO NOT emit ExecEvent::Fill here.
                //
                // OrderUpdates tells us that Hyperliquid considers the order
                // filled, but it does not represent the authoritative
                // individual execution record used by our canonical Fill
                // model.
                //
                // UserEvents::Fills gives us:
                //
                // - tid
                // - oid
                // - cloid
                // - execution price
                // - execution quantity
                // - fee
                // - execution timestamp
                //
                // That stream will create canonical Fill events.
                //
                // The Engine's existing fill handler will accumulate those
                // executions and transition the canonical Order to Filled once
                // filled_quantity reaches the requested Order quantity.
                print_log(
                    debug_mode,
                    true,
                    format!(
                        "[HYPERLIQUID MAPPER] ORDER STATUS FILLED | order_id={} | oid={} | symbol={} | source=OrderUpdates | execution_source=UserEvents",
                        order_id,
                        update.order.oid,
                        update.order.coin,
                    ),
                );
            }


            // ====================================================
            // CANCELED
            // ====================================================

            "canceled" => {
                print_log(
                    debug_mode,
                    true,
                    format!(
                        "[HYPERLIQUID MAPPER] ORDER CANCELED | order_id={} | oid={} | symbol={}",
                        order_id,
                        update.order.oid,
                        update.order.coin,
                    ),
                );


                let _ =
                    tx_evt.send(
                        ExecEvent::OrderCanceled {
                            order_id,
                            ts_ms,
                        }
                    );
            }


            // ====================================================
            // OTHER STATUS
            // ====================================================

            other => {
                print_log(
                    debug_mode,
                    true,
                    format!(
                        "[HYPERLIQUID MAPPER] UNHANDLED ORDER STATUS | order_id={} | oid={} | symbol={} | status={}",
                        order_id,
                        update.order.oid,
                        update.order.coin,
                        other,
                    ),
                );


                // Do not guess how an unknown Hyperliquid lifecycle status
                // maps into canonical engine state.
                //
                // Additional statuses such as rejection, expiration, or other
                // terminal venue states should be mapped after their actual
                // Hyperliquid WebSocket representation has been confirmed.
            }
        }
    }
}


// ============================================================
// RESOLVE CANONICAL ORDER ID
// ============================================================

/// Resolves the Hyperliquid CLOID carried by a WebSocket order update into
/// the Engine's canonical OrderId.
///
/// Resolution path:
///
///     CLOID string
///         ↓
///     ClientOrderId
///         ↓
///     HyperliquidOrderCorrelation
///         ↓
///     OrderId
fn resolve_order_id(
    cloid: Option<&str>,
    correlation: &HyperliquidOrderCorrelation,
    debug_mode: bool,
) -> Option<OrderId> {
    // ============================================================
    // CLOID EXISTS
    // ============================================================

    let Some(cloid) =
        cloid
    else {
        print_log(
            debug_mode,
            true,
            "[HYPERLIQUID MAPPER] ORDER UPDATE MISSING CLOID",
        );

        return None;
    };


    // ============================================================
    // CLOID -> CLIENT ORDER ID
    // ============================================================

    // Hyperliquid WebSocket CLOIDs arrive in the form:
    //
    //     0xf8ad4c638f844518b41462960f7332b2
    //
    // Our canonical ClientOrderId contains the equivalent UUID:
    //
    //     f8ad4c63-8f84-4518-b414-62960f7332b2
    //
    // The helper performs that venue-specific representation conversion.
    let Some(client_order_id) =
        client_order_id_from_ws_cloid(
            cloid
        )
    else {
        print_log(
            debug_mode,
            true,
            format!(
                "[HYPERLIQUID MAPPER] INVALID CLOID | cloid={}",
                cloid,
            ),
        );

        return None;
    };


    // ============================================================
    // CLIENT ORDER ID -> ORDER ID
    // ============================================================

    let Some(order_id) =
        correlation.get_by_client_order_id(
            client_order_id
        )
    else {
        print_log(
            debug_mode,
            true,
            format!(
                "[HYPERLIQUID MAPPER] UNKNOWN CLIENT ORDER ID | client_order_id={}",
                client_order_id,
            ),
        );

        return None;
    };


    // ============================================================
    // RESOLVED
    // ============================================================

    Some(order_id)
}