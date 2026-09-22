#![allow(dead_code)]

use chrono::Utc;
use ibapi::{
    orders::{ExecutionData, ExecutionSide, OrderStatus, OrderStatusKind, OrderUpdate},
    subscriptions::SubscriptionItem,
};
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    core::execution::{ExecEvent, OrderId, Side},
    exchange::ibkr::{
        correlation::IbkrOrderCorrelation,
        mapper::fill::map_ibkr_execution_to_fill,
    },
    shared::print_log,
};


// ============================================================
// HANDLE IBKR ORDER UPDATE
// ============================================================

/// Translates one IBKR order-update stream item into canonical Engine
/// execution events.
///
/// Individual executions come exclusively from `ExecutionData`.
///
/// `OrderStatus.filled`, `cumulative_quantity`, and `average_price` are
/// cumulative values and must never be converted into individual Fills.
pub fn handle_order_update(
    item: SubscriptionItem<OrderUpdate>,
    correlation: &IbkrOrderCorrelation,
    tx_evt: &UnboundedSender<ExecEvent>,
    debug_mode: bool,
) {
    match item {
        // ========================================================
        // ORDER DATA
        // ========================================================

        SubscriptionItem::Data(update) => {
            match update {
                OrderUpdate::OpenOrder(data) => {
                    let Some(order_id) = resolve_order_id(data.order_id, correlation, debug_mode) else {
                        return;
                    };

                    print_log(
                        debug_mode,
                        true,
                        format!(
                            "[IBKR MAPPER] OPEN ORDER | order_id={} | ibkr_order_id={} | symbol={} | status={}",
                            order_id, data.order_id, data.contract.symbol, data.order_state.status,
                        ),
                    );

                    // OpenOrder is useful venue state/diagnostic information.
                    // OrderStatus remains authoritative for canonical lifecycle.
                }

                OrderUpdate::OrderStatus(status) => {
                    handle_order_status(status, correlation, tx_evt, debug_mode);
                }

                OrderUpdate::ExecutionData(data) => {
                    handle_execution(data, correlation, tx_evt, debug_mode);
                }

                OrderUpdate::CommissionReport(report) => {
                    print_log(
                        debug_mode,
                        true,
                        format!(
                            "[IBKR MAPPER] COMMISSION REPORT | venue_execution_id={} | commission={} | currency={} | realized_pnl={:?}",
                            report.execution_id, report.commission, report.currency, report.realized_pnl,
                        ),
                    );

                    // TODO:
                    // Correlate CommissionReport.execution_id with
                    // Execution.execution_id if canonical Fill fees are later
                    // updated/accounted for after execution.
                }
            }
        }


        // ========================================================
        // ORDER NOTICE
        // ========================================================

        SubscriptionItem::Notice(notice) => {
            let ts_ms = Utc::now().timestamp_millis();

            let order_id = notice.request_id
                .and_then(|ibkr_order_id| correlation.get_by_ibkr_order_id(ibkr_order_id));

            print_log(
                debug_mode,
                true,
                format!(
                    "[IBKR MAPPER] NOTICE | order_id={:?} | request_id={:?} | code={} | message={}",
                    order_id, notice.request_id, notice.code, notice.message,
                ),
            );

            if notice.is_error() {
                if let Some(order_id) = order_id {
                    let _ = tx_evt.send(ExecEvent::OrderRejected {
                        order_id,
                        reason: format!("IBKR error {}: {}", notice.code, notice.message),
                        ts_ms,
                    });

                    return;
                }

                let _ = tx_evt.send(ExecEvent::Error {
                    order_id: None,
                    message: format!("IBKR error {}: {}", notice.code, notice.message),
                    ts_ms,
                });
            }

            // Warnings/system/informational notices are logged but do not
            // mutate canonical order state.
        }
    }
}


// ============================================================
// HANDLE ORDER STATUS
// ============================================================

fn handle_order_status(
    status: OrderStatus,
    correlation: &IbkrOrderCorrelation,
    tx_evt: &UnboundedSender<ExecEvent>,
    debug_mode: bool,
) {
    let Some(order_id) = resolve_order_id(status.order_id, correlation, debug_mode) else {
        return;
    };

    // Preserve IBKR's venue-side order ID before matching on status.status.
    //
    // OrderStatusKind::Unknown contains its own String value, so using the
    // variable name `status` inside that match arm would otherwise shadow
    // this OrderStatus structure.
    let ibkr_order_id = status.order_id;
    let ts_ms = Utc::now().timestamp_millis();

    print_log(
        debug_mode,
        true,
        format!(
            "[IBKR MAPPER] ORDER STATUS | order_id={} | ibkr_order_id={} | status={} | filled={} | remaining={} | avg_fill_px={:?} | last_fill_px={:?} | why_held={}",
            order_id,
            ibkr_order_id,
            status.status,
            status.filled,
            status.remaining,
            status.average_fill_price,
            status.last_fill_price,
            status.why_held,
        ),
    );

    match status.status {
        // ========================================================
        // PRE-WORKING
        // ========================================================

        OrderStatusKind::ApiPending |
        OrderStatusKind::PendingSubmit => {
            // Engine already owns PendingSubmit.
        }


        // ========================================================
        // WORKING
        // ========================================================

        OrderStatusKind::PreSubmitted |
        OrderStatusKind::Submitted => {
            let _ = tx_evt.send(ExecEvent::OrderWorking { order_id, ts_ms });
        }


        // ========================================================
        // PENDING CANCEL
        // ========================================================

        OrderStatusKind::PendingCancel => {
            // Engine already owns PendingCancel after issuing CancelOrder.
            //
            // Fills may still arrive before cancellation is confirmed.
        }


        // ========================================================
        // CANCELED
        // ========================================================

        OrderStatusKind::ApiCancelled |
        OrderStatusKind::Cancelled => {
            let _ = tx_evt.send(ExecEvent::OrderCanceled { order_id, ts_ms });
        }


        // ========================================================
        // FILLED
        // ========================================================

        OrderStatusKind::Filled => {
            // IMPORTANT:
            //
            // Do NOT create a Fill here.
            //
            // Individual canonical Fills come exclusively from ExecutionData.
            //
            // OrderStatus.filled is cumulative order state and using it here
            // would double count executions already emitted through
            // ExecutionData.
        }


        // ========================================================
        // INACTIVE
        // ========================================================

        OrderStatusKind::Inactive => {
            let _ = tx_evt.send(ExecEvent::OrderRejected {
                order_id,
                reason: "IBKR order became inactive".to_string(),
                ts_ms,
            });
        }


        // ========================================================
        // UNKNOWN
        // ========================================================

        OrderStatusKind::Unknown(unknown_status) => {
            print_log(
                debug_mode,
                true,
                format!(
                    "[IBKR MAPPER] UNKNOWN ORDER STATUS | order_id={} | ibkr_order_id={} | status={}",
                    order_id, ibkr_order_id, unknown_status,
                ),
            );
        }
    }
}


// ============================================================
// HANDLE EXECUTION
// ============================================================

fn handle_execution(
    data: ExecutionData,
    correlation: &IbkrOrderCorrelation,
    tx_evt: &UnboundedSender<ExecEvent>,
    debug_mode: bool,
) {
    let execution = data.execution;

    let Some(order_id) = resolve_order_id(execution.order_id, correlation, debug_mode) else {
        return;
    };


    // ============================================================
    // SIDE
    // ============================================================

    let side = match execution.side {
        ExecutionSide::Bought => Side::Buy,
        ExecutionSide::Sold => Side::Sell,
    };


    // ============================================================
    // TIMESTAMP
    // ============================================================

    // TODO:
    // Capture and inspect a real IBKR `Execution.time` value from TWS.
    //
    // Once its runtime format is confirmed, parse `execution.time` into the
    // canonical venue execution timestamp instead of using local receipt time.
    //
    // Until then, receipt time is intentionally isolated here so replacing it
    // later does not affect Fill mapping or Engine execution accounting.
    let ts_ms = Utc::now().timestamp_millis();


    // ============================================================
    // MAP EXECUTION -> FILL
    // ============================================================

    let Some(fill) = map_ibkr_execution_to_fill(
        order_id,
        data.contract.symbol.to_string(),
        side,
        execution.shares,
        execution.price,
        execution.execution_id.clone(),
        ts_ms,
        debug_mode,
    ) else {
        return;
    };


    // ============================================================
    // LOG
    // ============================================================

    print_log(
        debug_mode,
        true,
        format!(
            "[IBKR MAPPER] FILL MAPPED | order_id={} | ibkr_order_id={} | execution_id={:?} | venue_execution_id={:?} | symbol={} | side={:?} | qty={} | price={} | cumulative_qty={} | cumulative_avg_px={} | ibkr_time={} | ts_ms={}",
            fill.order_id,
            execution.order_id,
            fill.execution_id,
            fill.venue_execution_id,
            fill.symbol,
            fill.side,
            fill.quantity,
            fill.price,
            execution.cumulative_quantity,
            execution.average_price,
            execution.time,
            fill.ts_ms,
        ),
    );


    // ============================================================
    // EMIT CANONICAL FILL
    // ============================================================

    let _ = tx_evt.send(ExecEvent::Fill { fill });
}


// ============================================================
// RESOLVE CANONICAL ORDER ID
// ============================================================

fn resolve_order_id(
    ibkr_order_id: i32,
    correlation: &IbkrOrderCorrelation,
    debug_mode: bool,
) -> Option<OrderId> {
    let Some(order_id) = correlation.get_by_ibkr_order_id(ibkr_order_id) else {
        print_log(
            debug_mode,
            true,
            format!("[IBKR MAPPER] UNKNOWN ORDER | ibkr_order_id={}", ibkr_order_id),
        );

        return None;
    };

    Some(order_id)
}