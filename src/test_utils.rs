#[cfg(test)]
use crate::{client::Pulsar, executor::TokioExecutor};

/// Wrapper for the Tokio executor
#[cfg(any(
    feature = "tokio-runtime",
    feature = "tokio-rustls-runtime-aws-lc-rs",
    feature = "tokio-rustls-runtime-ring"
))]
#[cfg(test)]
pub async fn new_pulsar() -> Pulsar<TokioExecutor> {
    use log::LevelFilter;

    use crate::tests::TEST_LOGGER;

    let _result = log::set_logger(&TEST_LOGGER);
    log::set_max_level(LevelFilter::Debug);

    Pulsar::builder("pulsar://127.0.0.1:6650", TokioExecutor)
        .build()
        .await
        .unwrap()
}

#[cfg(test)]
pub(crate) async fn create_partitioned_topic(
    tenant: &str,
    namespace: &str,
    topic_name: &str,
    num_partitions: u32,
) {
    use reqwest::Client;

    let create_partitioned_topic_url = format!(
        "http://127.0.0.1:8080/admin/v2/persistent/{tenant}/{namespace}/{topic_name}/partitions"
    );
    let client = Client::new();
    let response = client
        .put(create_partitioned_topic_url)
        .json(&num_partitions.to_string())
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
}
