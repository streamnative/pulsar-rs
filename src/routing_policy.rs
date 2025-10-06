use std::sync::Arc;

use murmur3::murmur3_32;

use crate::producer::Message;

#[derive(Clone, Default)]
pub enum RoutingPolicy {
    #[default]
    RoundRobin,
    Single(usize),
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
