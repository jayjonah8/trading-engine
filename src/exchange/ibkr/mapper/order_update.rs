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

/// Translates one IBKR order-update stream item into canonical execution events.
///
/// Important IBKR behavior confirmed from live PAPER testing:
///
/// - `OpenOrder` and `OrderStatus` may be emitted repeatedly.
/// - `PreSubmitted` can represent a valid working order, including an order
///   waiting for the market to open.
/// - `OrderStatus.filled` is cumulative and must not create canonical Fills.
/// - Every individual execution arrives through `ExecutionData`.
/// - Partial fills produce separate `ExecutionData` events with unique
///   execution IDs.
/// - `CommissionReport` may arrive after its corresponding execution/order
///   lifecycle event.
pub fn handle_order_update(
    item: SubscriptionItem<OrderUpdate>,
    correlation: &IbkrOrderCorrelation,
    tx_evt: &UnboundedSender<ExecEvent>,
    debug_mode: bool,
) {
    match item {
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

                    // Diagnostic venue state only.
                    // OrderStatus drives canonical order lifecycle events.
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

                    // Commission reports are asynchronous and may arrive after
                    // the execution/order has otherwise completed.
                    //
                    // TODO:
                    // Correlate report.execution_id with the canonical Fill if
                    // post-execution fee accounting is added.
                }
            }
        }

        SubscriptionItem::Notice(notice) => {
            let ts_ms = Utc::now().timestamp_millis();
            let order_id = notice.request_id.and_then(|ibkr_order_id| correlation.get_by_ibkr_order_id(ibkr_order_id));

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

            // Warnings, system messages and informational notices do not
            // mutate canonical order state.
            //
            // Example confirmed from PAPER testing:
            // warning 399 can indicate that a valid market order is being
            // held until the next market open.
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
        OrderStatusKind::ApiPending |
        OrderStatusKind::PendingSubmit => {
            // Engine already owns PendingSubmit.
        }

        OrderStatusKind::PreSubmitted |
        OrderStatusKind::Submitted => {
            // Both are valid working states.
            //
            // PreSubmitted may persist while an order waits for the market
            // to open, and may also appear during an actively filling order.
            let _ = tx_evt.send(ExecEvent::OrderWorking { order_id, ts_ms });
        }

        OrderStatusKind::PendingCancel => {
            // Engine already owns PendingCancel after issuing CancelOrder.
            // Executions may still arrive before cancellation completes.
        }

        OrderStatusKind::ApiCancelled |
        OrderStatusKind::Cancelled => {
            let _ = tx_evt.send(ExecEvent::OrderCanceled { order_id, ts_ms });
        }

        OrderStatusKind::Filled => {
            // Do not emit a Fill here.
            //
            // `filled` is cumulative. Individual executions are emitted
            // exclusively from ExecutionData.
        }

        OrderStatusKind::Inactive => {
            let _ = tx_evt.send(ExecEvent::OrderRejected {
                order_id,
                reason: "IBKR order became inactive".to_string(),
                ts_ms,
            });
        }

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

    let side = match execution.side {
        ExecutionSide::Bought => Side::Buy,
        ExecutionSide::Sold => Side::Sell,
    };

    // TODO:
    // Parse the confirmed IBKR execution.time format into the canonical
    // execution timestamp. Receipt time is used until that is implemented.
    let ts_ms = Utc::now().timestamp_millis();

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