#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarketRegime {
    MeanReversion,
    Trending,
}