use std::sync::Arc;

use murmur3::murmur3_32;

use crate::producer::Message;

#[derive(Clone, Default)]
pub enum RoutingPolicy {
    #[default]
    RoundRobin,
    Single,
    Custom(Arc<dyn CustomRoutingPolicy>),
}

impl RoutingPolicy {
    pub fn compute_partition_index_for_key(key: &str, partition_count: usize) -> usize {
        // Use murmur3 hash for deterministic results across restarts
        // Using a fixed seed (0) ensures consistent hashing
        let hash = murmur3_32(&mut key.as_bytes(), 0).unwrap_or(0);
        (hash % partition_count as u32) as usize
    }
}

pub trait CustomRoutingPolicy: Send + Sync {
    fn route(&self, message: &Message, num_producers: usize) -> usize;
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::routing_policy::RoutingPolicy;

    #[test]
    fn test_compute_partition_index_consistency() {
        let partition_count = 4;
        let key = Uuid::new_v4().to_string();

        let partition_index = RoutingPolicy::compute_partition_index_for_key(&key, partition_count);
        assert!(partition_index < partition_count);

        for _ in 0..10 {
            let other_partition_index =
                RoutingPolicy::compute_partition_index_for_key(&key, partition_count);
            assert!(other_partition_index < partition_count);
            assert_eq!(
                partition_index, other_partition_index,
                "partition index should be deterministic for the same key"
            );
        }
    }

    #[test]
    fn test_compute_partition_index_distribution() {
        let partition_count = 4;
        let mut partition_counts = vec![0; partition_count];

        let total = 1000;
        for _ in 0..total {
            let partition_index = RoutingPolicy::compute_partition_index_for_key(
                &Uuid::new_v4().to_string(),
                partition_count,
            );
            partition_counts[partition_index] += 1;
        }

        for count in partition_counts {
            let ratio = count as f64 / total as f64;
            let expected_ratio = 1.0 / partition_count as f64;

            assert!(
                ratio > (expected_ratio - 0.1) && ratio < (expected_ratio + 0.1),
                "distribution ratio {} is not near expected ratio {}",
                ratio,
                expected_ratio
            );
        }
    }
}
