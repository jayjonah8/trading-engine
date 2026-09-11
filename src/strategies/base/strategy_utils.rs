use crate::notifications::{
    send_remote_message,
    env_helpers::{
        get_slack_general_channel_id,
        get_discord_general_channel_id,
    },
};

use crate::shared::utils::{
    get_bot_name,
    get_local_timestamp,
    get_utc_timestamp,
};

use crate::strategies::{
    Strategy,
    base::state::BaseStrategy,
};

pub fn send_base_strategy_start_message(
    strategy: &BaseStrategy,
) {
    let message = format!(
        "🚀 {} Started\n\
        Bot: {}\n\
        Entry Window: {}s\n\
        Trade Amount: ${:.2}\n\
        Entry: {:.2}%\n\
        Take Profit: {:.2}%\n\
        Stop Loss: {:.2}%\n\
        Emergency Exit: {:.2}%\n\
        Min SL Delay: {}s\n\
        Trade Duration Enabled: {}\n\
        Trade Duration: {:?}s\n\
        New York Time: {}\n\
        UTC Time: {}",
        strategy.name(),
        get_bot_name(),
        strategy.entry_window_secs,
        strategy.trade_amount_usd,
        strategy.entry_percent,
        strategy.exit_profit_percent,
        strategy.exit_loss_percent,
        strategy.emergency_exit_percent,
        strategy.min_sl_delay,
        strategy.enable_trade_duration,
        strategy.trade_duration_secs,
        get_local_timestamp(),
        get_utc_timestamp(),
    );

    {
        let view = strategy.rx_view.borrow();

        send_remote_message(
            &view.cfg,
            message,
            Some(get_slack_general_channel_id()),
            Some(get_discord_general_channel_id()),
        );
    }
}