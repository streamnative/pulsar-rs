/// position of the first message that will be consumed
#[derive(Default, Clone, Debug)]
pub enum InitialPosition {
    /// start at the oldest message
    Earliest,
    /// start at the most recent message
    #[default]
    Latest,
}

impl From<InitialPosition> for i32 {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(i: InitialPosition) -> Self {
        match i {
            InitialPosition::Earliest => 1,
            InitialPosition::Latest => 0,
        }
    }
}
