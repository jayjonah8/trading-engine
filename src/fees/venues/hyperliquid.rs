pub struct HyperliquidFees {
    pub maker_rate: f64,
    pub taker_rate: f64,
}

impl HyperliquidFees {
    pub fn calculate(
        &self,
        quantity: f64,
        price: f64,
        is_maker: bool,
    ) -> f64 {
        let notional = quantity * price;

        let rate = if is_maker {
            self.maker_rate
        } else {
            self.taker_rate
        };

        notional * rate
    }
}