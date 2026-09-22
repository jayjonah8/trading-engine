use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::core::execution::{ExecCommand, ExecEvent};

pub async fn run_interactive_brokers_adapter(
    mut rx_cmd: UnboundedReceiver<ExecCommand>,
    tx_evt: UnboundedSender<ExecEvent>,
) {
    while let Some(command) = rx_cmd.recv().await {
        match command {
            ExecCommand::SubmitOrder { order } => {
                // IBKR placeOrder
            }

            ExecCommand::CancelOrder { venue: _, order_id } => {
                // IBKR cancelOrder
            }

            ExecCommand::ReplaceOrder { venue: _, order_id, changes } => {
                // IBKR modify existing order
            }
        }
    }
}