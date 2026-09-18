#![allow(dead_code)]

use std::collections::HashMap;

use tokio::sync::mpsc::{
    UnboundedReceiver,
    UnboundedSender,
};

use crate::core::execution::{
    ExecCommand,
    Venue,
};

use crate::shared::print_log;


// ============================================================
// EXECUTION ROUTES
// ============================================================

/// Registered execution-adapter command channels.
///
/// Each venue owns its own adapter channel. The router receives canonical
/// `ExecCommand`s from the engine and forwards each command to the adapter
/// responsible for the command's `Venue`.
///
/// The router does not mutate engine state and does not perform any
/// venue-specific translation.
pub struct ExecutionRoutes {
    routes: HashMap<Venue, UnboundedSender<ExecCommand>>,
}

impl ExecutionRoutes {
    /// Creates an empty execution route table.
    pub fn new() -> Self {
        Self {
            routes: HashMap::new(),
        }
    }

    /// Registers an execution adapter for a venue.
    pub fn register(
        &mut self,
        venue: Venue,
        tx_cmd: UnboundedSender<ExecCommand>,
    ) {
        self.routes.insert(
            venue,
            tx_cmd,
        );
    }

    /// Returns the adapter command channel registered for a venue.
    pub fn get(
        &self,
        venue: Venue,
    ) -> Option<&UnboundedSender<ExecCommand>> {
        self.routes.get(
            &venue
        )
    }
}


// ============================================================
// EXECUTION ROUTER
// ============================================================

/// Routes canonical execution commands from the engine to the correct
/// venue-specific execution adapter.
///
/// Flow:
///
/// Engine
///     -> ExecCommand
///     -> Execution Router
///     -> Alpaca / IBKR / Hyperliquid adapter
///
/// Routing is performed using the `Venue` carried by the command/order.
///
/// The router intentionally contains no trading state and no venue-specific
/// execution logic. Its only responsibility is command delivery.
pub async fn run_execution_router(
    mut rx_cmd: UnboundedReceiver<ExecCommand>,
    routes: ExecutionRoutes,
    debug_mode: bool,
) {
    print_log(
        debug_mode,
        true,
        "[EXECUTION ROUTER] started",
    );

    while let Some(command) =
        rx_cmd.recv().await
    {
        let venue =
            command.venue();

        // Keep disabled during normal operation.
        // Can be enabled when debugging execution routing.
        print_log(
            debug_mode,
            false,
            format!(
                "[EXECUTION ROUTER] command received | venue={:?} | command={:?}",
                venue,
                command,
            ),
        );

        let Some(tx_adapter) =
            routes.get(
                venue
            )
        else {
            print_log(
                debug_mode,
                true,
                format!(
                    "[EXECUTION ROUTER] no adapter registered | venue={:?}",
                    venue,
                ),
            );

            continue;
        };

        if let Err(err) =
            tx_adapter.send(
                command
            )
        {
            print_log(
                debug_mode,
                true,
                format!(
                    "[EXECUTION ROUTER] adapter channel closed | venue={:?} | error={}",
                    venue,
                    err,
                ),
            );
        }
    }

    print_log(
        debug_mode,
        true,
        "[EXECUTION ROUTER] shutting down",
    );
}