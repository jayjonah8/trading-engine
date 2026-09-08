use std::time::Duration;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
)]
pub struct Timeframe {
    duration: Duration,
}

impl Timeframe {
    pub const ONE_MINUTE: Self =
        Self::from_secs(60);

    pub const FIVE_MINUTES: Self =
        Self::from_secs(5 * 60);

    pub const TEN_MINUTES: Self =
        Self::from_secs(10 * 60);

    pub const FIFTEEN_MINUTES: Self =
        Self::from_secs(15 * 60);

    pub const THIRTY_MINUTES: Self =
        Self::from_secs(30 * 60);

    pub const ONE_HOUR: Self =
        Self::from_secs(60 * 60);

    pub const TWO_HOURS: Self =
        Self::from_secs(2 * 60 * 60);

    pub const FOUR_HOURS: Self =
        Self::from_secs(4 * 60 * 60);

    pub const fn from_secs(
        seconds: u64,
    ) -> Self {
        Self {
            duration:
                Duration::from_secs(seconds),
        }
    }

    pub fn duration(
        self,
    ) -> Duration {
        self.duration
    }

    pub fn as_secs(
        self,
    ) -> u64 {
        self.duration.as_secs()
    }
}