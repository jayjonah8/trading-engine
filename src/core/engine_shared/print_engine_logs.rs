use crate::core::{
    engine_state::Engine,
    trade_intent::TradeIntent,
};

use crate::shared::print_log;

impl Engine {

    /// Prints a formatted trade intent for debugging.
    ///
    /// This shared formatter can be used by both live and backtest
    /// execution paths to avoid duplicating trade-intent log formatting.
    ///
    /// # Arguments
    ///
    /// * `source` - Label describing where the trade intent originated,
    ///   such as `"LIVE"` or `"BACKTEST"`.
    /// * `intent` - The trade intent to display.
    pub(crate) fn print_trade_intent(
        &self,
        source: &str,
        intent: &TradeIntent,
    ) {
        print_log(
            self.debug_mode,
            true,
            format!(
                "\n\
                 [ENGINE {}] OPEN TRADE INTENT\n\
                 ----------------------------------------\n\
                 Strategy:        {}\n\
                 Symbol:          {}\n\
                 Direction:       {}\n\
                 Price:           ${:.4}\n\
                 Trade Amount:    ${:.2}\n\
                 Entry Move:      {:.3}%\n\
                 Take Profit:     {:.3}%\n\
                 Stop Loss:       {:.3}%\n\
                 Emergency SL:    {:.3}%\n\
                 SL Delay:        {}s\n\
                 Duration:        {:?}\n\
                 Duration On:     {}\n\
                 Timestamp:       {} ms\n\
                 ----------------------------------------",
                source,
                intent.strategy,
                intent.symbol,
                if intent.is_long {
                    "LONG"
                } else {
                    "SHORT"
                },
                intent.price,
                intent.trade_amount_usd,
                intent.entry_percent,
                intent.exit_profit_percent,
                intent.exit_loss_percent,
                intent.emergency_exit_percent,
                intent.min_sl_delay,
                intent.trade_duration_secs,
                intent.enable_trade_duration,
                intent.ts_ms,
            ),
        );
    }

}