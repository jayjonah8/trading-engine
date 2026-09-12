#![allow(dead_code)]

use hyperliquid_rust_sdk::ExchangeClient;

use tokio::sync::mpsc::UnboundedSender;

use crate::{
    core::execution::{
        ExecEvent,
        Order,
        OrderType,
    },

    exchange::hyperliquid::commands::submit::{
        handle_limit_order,
        handle_market_order,
        handle_stop_limit_order,
        handle_stop_market_order,
    },
};


// ============================================================
// HANDLE SUBMIT ORDER
// ============================================================

/// Routes a canonical engine `Order` to the correct Hyperliquid
/// order-type-specific submit handler.
///
/// This function does not perform venue execution itself. Each order type
/// owns its Hyperliquid-specific translation and immediate response mapping
/// inside `commands/submit/*`.
///
/// Authoritative Order, Trade, Position, FillLedger, and RiskState mutations
/// remain exclusively inside the Engine.
pub async fn handle_submit_order(
    exchange_client: &ExchangeClient,
    tx_evt: &UnboundedSender<ExecEvent>,
    order: Order,
    debug_mode: bool,
) {
    match order.order_type {
        OrderType::Market => {
            handle_market_order(
                exchange_client,
                tx_evt,
                order,
                debug_mode,
            )
            .await;
        }

        OrderType::Limit => {
            handle_limit_order(
                exchange_client,
                tx_evt,
                order,
                debug_mode,
            )
            .await;
        }

        OrderType::StopMarket => {
            handle_stop_market_order(
                exchange_client,
                tx_evt,
                order,
                debug_mode,
            )
            .await;
        }

        OrderType::StopLimit => {
            handle_stop_limit_order(
                exchange_client,
                tx_evt,
                order,
                debug_mode,
            )
            .await;
        }
    }
}