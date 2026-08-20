//! Acks of messages inside batched entries.
//!
//! The broker stores a producer batch as one entry, and an ack without an `ack_set` acks the
//! whole entry. After the first ack of any message in the batch entry, the subscription never
//! delivers the sibling messages again. The tracker keeps a bitset of unacked messages per
//! delivered entry, so an entry is only acked outright once every message in it has been acked.

use std::collections::HashMap;

use crate::message::proto::MessageIdData;

/// How a message that arrived inside a batched entry is acknowledged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BatchAcknowledgment {
    /// Ack the whole entry on the first ack of any of its messages. The subscription moves
    /// past the entry, unacked siblings included, and never delivers them again.
    #[default]
    Entry,
    /// Hold acks back until every message of the entry has been acked, then ack the entry.
    Tracked,
    /// Send the entry's remaining bitset with every ack. A broker with
    /// `acknowledgmentAtBatchIndexLevelEnabled` redelivers only those messages; one without it
    /// ignores the bitset and behaves as under [`Self::Tracked`].
    Indexed,
}

/// Bitset of an entry's unacked message indexes, in the `ack_set` wire layout: bit `i` is bit
/// `i % 64` of word `i / 64`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AckSet {
    words: Vec<u64>,
}

impl AckSet {
    pub(crate) fn full(batch_size: u32) -> Self {
        let mut words = vec![u64::MAX; (batch_size as usize).div_ceil(64)];
        let used_in_last = batch_size % 64;
        if let Some(last) = words.last_mut().filter(|_| used_in_last != 0) {
            *last = (1u64 << used_in_last) - 1;
        }
        AckSet { words }
    }

    pub(crate) fn and(&mut self, other: &AckSet) {
        for (index, word) in self.words.iter_mut().enumerate() {
            *word &= other.words.get(index).copied().unwrap_or(0);
        }
    }

    pub(crate) fn get(&self, index: u32) -> bool {
        self.words
            .get(index as usize / 64)
            .is_some_and(|word| word & (1u64 << (index % 64)) != 0)
    }

    pub(crate) fn clear(&mut self, index: u32) {
        if let Some(word) = self.words.get_mut(index as usize / 64) {
            *word &= !(1u64 << (index % 64));
        }
    }

    pub(crate) fn clear_through(&mut self, index: u32) {
        let last = index as usize / 64;
        let kept_in_last = u64::MAX.checked_shl(index % 64 + 1).unwrap_or(0);
        for (position, word) in self.words.iter_mut().take(last + 1).enumerate() {
            *word &= if position == last { kept_in_last } else { 0 };
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.words.iter().all(|word| *word == 0)
    }

    /// The wire words, trailing zero words dropped.
    pub(crate) fn to_words(&self) -> Vec<i64> {
        let used = self
            .words
            .iter()
            .rposition(|word| *word != 0)
            .map_or(0, |position| position + 1);
        self.words[..used].iter().map(|word| *word as i64).collect()
    }
}

impl From<&[i64]> for AckSet {
    fn from(words: &[i64]) -> Self {
        AckSet {
            words: words.iter().map(|word| *word as u64).collect(),
        }
    }
}

/// The unacked messages of each delivered batched entry.
#[derive(Debug)]
pub(crate) struct BatchAcknowledgmentTracker {
    mode: BatchAcknowledgment,
    entries: HashMap<MessageIdData, AckSet>,
}

impl BatchAcknowledgmentTracker {
    pub(crate) fn new(mode: BatchAcknowledgment) -> Self {
        BatchAcknowledgmentTracker {
            mode,
            entries: HashMap::new(),
        }
    }

    /// Whether entries are held until every message in them is acked.
    pub(crate) fn tracks_entries(&self) -> bool {
        self.mode != BatchAcknowledgment::Entry
    }

    /// Starts tracking a delivered entry, replacing the state of any earlier delivery of it.
    pub(crate) fn track(&mut self, entry: MessageIdData, unacked: AckSet) {
        if !self.tracks_entries() {
            return;
        }
        if unacked.is_empty() {
            self.entries.remove(&entry);
        } else {
            self.entries.insert(entry, unacked);
        }
    }

    /// Records an ack and returns the id to send to the broker, if anything is sent.
    #[must_use]
    pub(crate) fn acknowledge(
        &mut self,
        message_id: MessageIdData,
        cumulative: bool,
    ) -> Option<MessageIdData> {
        if !self.tracks_entries() {
            return Some(message_id);
        }
        let entry = message_id.entry();
        if cumulative {
            let acked = entry.position();
            self.entries.retain(|id, _| id.position() >= acked);
        }
        let Some((batch_index, batch_size)) = message_id.batch() else {
            self.entries.remove(&entry);
            return Some(entry);
        };

        // An entry not delivered on this connection is acked against a full bitset, and nothing
        // is kept for it. Under Indexed the broker intersects that bitset with its own; under
        // Tracked the ack is dropped, since the entry comes back whole and the acks of that
        // delivery complete it.
        let mut untracked = AckSet::full(batch_size);
        let ack_set = self.entries.get_mut(&entry).unwrap_or(&mut untracked);
        if cumulative {
            ack_set.clear_through(batch_index);
        } else {
            ack_set.clear(batch_index);
        }

        if ack_set.is_empty() {
            self.entries.remove(&entry);
            Some(entry)
        } else if self.mode == BatchAcknowledgment::Indexed {
            Some(MessageIdData {
                ack_set: ack_set.to_words(),
                ..entry
            })
        } else if cumulative {
            Some(entry.prev_entry())
        } else {
            None
        }
    }

    /// Forgets an entry the broker is about to redeliver; the redelivery tracks it again.
    pub(crate) fn forget(&mut self, entry: &MessageIdData) {
        self.entries.remove(entry);
    }

    pub(crate) fn clear(&mut self) {
        self.entries.clear();
    }

    #[cfg(test)]
    fn tracked(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(ledger_id: u64, entry_id: u64) -> MessageIdData {
        MessageIdData {
            ledger_id,
            entry_id,
            partition: Some(-1),
            ..Default::default()
        }
    }

    fn batched(ledger_id: u64, entry_id: u64, batch_index: i32, batch_size: i32) -> MessageIdData {
        MessageIdData {
            batch_index: Some(batch_index),
            batch_size: Some(batch_size),
            ..entry(ledger_id, entry_id)
        }
    }

    fn with_ack_set(id: MessageIdData, ack_set: Vec<i64>) -> MessageIdData {
        MessageIdData { ack_set, ..id }
    }

    /// What the broker reports as unacked for an entry of `batch_size` messages.
    fn reported(batch_size: u32, broker_ack_set: &[i64]) -> AckSet {
        let mut ack_set = AckSet::full(batch_size);
        ack_set.and(&AckSet::from(broker_ack_set));
        ack_set
    }

    fn tracked() -> BatchAcknowledgmentTracker {
        BatchAcknowledgmentTracker::new(BatchAcknowledgment::Tracked)
    }

    fn indexed() -> BatchAcknowledgmentTracker {
        BatchAcknowledgmentTracker::new(BatchAcknowledgment::Indexed)
    }

    #[test]
    fn ack_set_wire_layout() {
        assert_eq!(AckSet::full(3).to_words(), vec![0b111]);
        let mut set = AckSet::full(3);
        set.clear(1);
        assert_eq!(set.to_words(), vec![0b101]);
        assert!(set.get(0));
        assert!(!set.get(1));
        assert!(set.get(2));
        assert!(!set.get(3));

        assert_eq!(AckSet::full(64).to_words(), vec![u64::MAX as i64]);
        assert_eq!(AckSet::full(65).to_words(), vec![u64::MAX as i64, 1]);
        assert_eq!(
            AckSet::full(128).to_words(),
            vec![u64::MAX as i64, u64::MAX as i64]
        );
        let mut set = AckSet::full(65);
        set.clear(64);
        assert_eq!(set.to_words(), vec![u64::MAX as i64]);

        assert_eq!(AckSet::full(0).to_words(), Vec::<i64>::new());
        assert!(AckSet::full(0).is_empty());
    }

    #[test]
    fn ack_set_clearing_and_emptiness() {
        let mut set = AckSet::full(4);
        set.clear_through(2);
        assert_eq!(set.to_words(), vec![0b1000]);
        assert!(!set.is_empty());
        set.clear(3);
        assert!(set.is_empty());
        assert_eq!(set.to_words(), Vec::<i64>::new());

        let mut set = AckSet::full(2);
        set.clear(7);
        set.clear(200);
        assert_eq!(set.to_words(), vec![0b11]);
        set.clear_through(100);
        assert!(set.is_empty());
    }

    #[test]
    fn ack_set_clears_through_word_boundaries() {
        let mut set = AckSet::full(130);
        set.clear_through(63);
        assert_eq!(set.to_words(), vec![0, u64::MAX as i64, 0b11]);
        set.clear_through(64);
        assert_eq!(set.to_words(), vec![0, (u64::MAX - 1) as i64, 0b11]);
        set.clear_through(128);
        assert_eq!(set.to_words(), vec![0, 0, 0b10]);

        // An index past the end clears the whole set without walking to it.
        let mut set = AckSet::full(3);
        set.clear_through(u32::MAX);
        assert!(set.is_empty());
    }

    #[test]
    fn ack_set_and_with_broker_set() {
        let mut set = AckSet::full(5);
        set.and(&AckSet::from(&[0b10100i64][..]));
        assert_eq!(set.to_words(), vec![0b10100]);

        let mut set = AckSet::full(70);
        set.and(&AckSet::from(&[0b1i64][..]));
        assert_eq!(set.to_words(), vec![0b1]);
    }

    #[test]
    fn entry_mode_sends_ids_as_they_are_and_tracks_nothing() {
        let mut tracker = BatchAcknowledgmentTracker::new(BatchAcknowledgment::Entry);
        assert!(!tracker.tracks_entries());
        tracker.track(entry(1, 2), AckSet::full(3));
        assert_eq!(tracker.tracked(), 0);
        assert_eq!(
            tracker.acknowledge(batched(1, 2, 1, 3), false),
            Some(batched(1, 2, 1, 3))
        );
        assert_eq!(
            tracker.acknowledge(batched(1, 2, 1, 3), true),
            Some(batched(1, 2, 1, 3))
        );
    }

    #[test]
    fn non_batched_id_is_sent_as_is() {
        let mut tracker = tracked();
        assert_eq!(tracker.acknowledge(entry(1, 2), false), Some(entry(1, 2)));
        assert_eq!(indexed().acknowledge(entry(1, 2), true), Some(entry(1, 2)));
        // A batch index without a batch size does not make a batched id.
        let mut id = entry(1, 2);
        id.batch_index = Some(3);
        assert_eq!(tracker.acknowledge(id, false), Some(entry(1, 2)));
        assert_eq!(tracker.tracked(), 0);
    }

    #[test]
    fn tracked_acks_are_held_until_the_entry_is_fully_acked() {
        let mut tracker = tracked();
        tracker.track(entry(1, 2), AckSet::full(3));
        assert_eq!(tracker.acknowledge(batched(1, 2, 1, 3), false), None);
        assert_eq!(
            tracker.acknowledge(batched(1, 2, 1, 3), false),
            None,
            "acking the same index twice must not complete the entry"
        );
        assert_eq!(tracker.acknowledge(batched(1, 2, 0, 3), false), None);
        assert_eq!(
            tracker.acknowledge(batched(1, 2, 2, 3), false),
            Some(entry(1, 2))
        );
        assert_eq!(tracker.tracked(), 0);
    }

    #[test]
    fn indexed_acks_send_the_remaining_bitset() {
        let mut tracker = indexed();
        tracker.track(entry(1, 2), AckSet::full(3));
        assert_eq!(
            tracker.acknowledge(batched(1, 2, 1, 3), false),
            Some(with_ack_set(entry(1, 2), vec![0b101]))
        );
        assert_eq!(
            tracker.acknowledge(batched(1, 2, 2, 3), false),
            Some(with_ack_set(entry(1, 2), vec![0b001]))
        );
        assert_eq!(
            tracker.acknowledge(batched(1, 2, 0, 3), false),
            Some(entry(1, 2)),
            "the last ack is a plain ack of the entry"
        );
        assert_eq!(tracker.tracked(), 0);
    }

    #[test]
    fn tracked_cumulative_ack_inside_an_entry_acks_the_preceding_entry() {
        let mut tracker = tracked();
        tracker.track(entry(1, 2), AckSet::full(3));
        assert_eq!(
            tracker.acknowledge(batched(1, 2, 1, 3), true),
            Some(entry(1, 1))
        );
        assert_eq!(
            tracker.acknowledge(batched(1, 2, 2, 3), true),
            Some(entry(1, 2))
        );

        // The first entry of a ledger acks up to (ledger, -1).
        tracker.track(entry(1, 0), AckSet::full(2));
        assert_eq!(
            tracker.acknowledge(batched(1, 0, 0, 2), true),
            Some(entry(1, u64::MAX))
        );
    }

    #[test]
    fn indexed_cumulative_ack_clears_through_the_acked_index() {
        let mut tracker = indexed();
        tracker.track(entry(1, 2), AckSet::full(4));
        assert_eq!(
            tracker.acknowledge(batched(1, 2, 1, 4), true),
            Some(with_ack_set(entry(1, 2), vec![0b1100]))
        );
        assert_eq!(
            tracker.acknowledge(batched(1, 2, 3, 4), true),
            Some(entry(1, 2))
        );
    }

    #[test]
    fn broker_ack_set_marks_indexes_as_already_acked() {
        let mut tracker = tracked();
        tracker.track(entry(1, 2), reported(4, &[0b1010]));
        assert_eq!(tracker.acknowledge(batched(1, 2, 1, 4), false), None);
        assert_eq!(
            tracker.acknowledge(batched(1, 2, 3, 4), false),
            Some(entry(1, 2))
        );
    }

    #[test]
    fn acks_of_untracked_entries_leave_no_state() {
        // Tracked: dropped, and acking the rest of the entry cannot complete it either, since
        // nothing was kept.
        let mut tracker = tracked();
        assert_eq!(tracker.acknowledge(batched(1, 2, 0, 2), false), None);
        assert_eq!(tracker.tracked(), 0);
        assert_eq!(tracker.acknowledge(batched(1, 2, 1, 2), false), None);
        assert_eq!(tracker.tracked(), 0);
        // A single-message batch is acked whole right away.
        assert_eq!(
            tracker.acknowledge(batched(1, 4, 0, 1), false),
            Some(entry(1, 4))
        );
        assert_eq!(tracker.tracked(), 0);

        // Indexed: each ack carries a bitset computed from a full one, and the broker is the one
        // that intersects them.
        let mut tracker = indexed();
        assert_eq!(
            tracker.acknowledge(batched(1, 3, 1, 2), false),
            Some(with_ack_set(entry(1, 3), vec![0b01]))
        );
        assert_eq!(
            tracker.acknowledge(batched(1, 3, 0, 2), false),
            Some(with_ack_set(entry(1, 3), vec![0b10]))
        );
        assert_eq!(tracker.tracked(), 0);
    }

    #[test]
    fn cumulative_acks_of_untracked_entries_need_no_state() {
        let mut tracker = tracked();
        assert_eq!(
            tracker.acknowledge(batched(1, 5, 1, 4), true),
            Some(entry(1, 4))
        );
        assert_eq!(
            tracker.acknowledge(batched(1, 5, 3, 4), true),
            Some(entry(1, 5))
        );
        assert_eq!(tracker.tracked(), 0);

        let mut tracker = indexed();
        assert_eq!(
            tracker.acknowledge(batched(1, 5, 1, 4), true),
            Some(with_ack_set(entry(1, 5), vec![0b1100]))
        );
        assert_eq!(tracker.tracked(), 0);
    }

    #[test]
    fn acking_a_completed_entry_again_leaves_no_state() {
        let mut tracker = indexed();
        tracker.track(entry(1, 2), AckSet::full(2));
        assert_eq!(
            tracker.acknowledge(batched(1, 2, 0, 2), false),
            Some(with_ack_set(entry(1, 2), vec![0b10]))
        );
        assert_eq!(
            tracker.acknowledge(batched(1, 2, 1, 2), false),
            Some(entry(1, 2))
        );
        assert_eq!(tracker.tracked(), 0);
        // A repeat ack is resolved against a full bitset and kept nowhere. The broker intersects
        // bitsets, so this cannot reopen an entry it has already acked.
        assert_eq!(
            tracker.acknowledge(batched(1, 2, 1, 2), false),
            Some(with_ack_set(entry(1, 2), vec![0b01]))
        );
        assert_eq!(tracker.tracked(), 0);

        let mut tracker = tracked();
        tracker.track(entry(1, 2), AckSet::full(1));
        assert_eq!(
            tracker.acknowledge(batched(1, 2, 0, 1), false),
            Some(entry(1, 2))
        );
        assert_eq!(tracker.acknowledge(batched(1, 2, 0, 2), false), None);
        assert_eq!(tracker.tracked(), 0);
    }

    #[test]
    fn redelivery_replaces_the_tracked_state() {
        let mut tracker = tracked();
        tracker.track(entry(1, 2), AckSet::full(2));
        assert_eq!(tracker.acknowledge(batched(1, 2, 0, 2), false), None);
        tracker.track(entry(1, 2), AckSet::full(2));
        assert_eq!(
            tracker.acknowledge(batched(1, 2, 1, 2), false),
            None,
            "the index acked before the redelivery must be acked again"
        );
        assert_eq!(
            tracker.acknowledge(batched(1, 2, 0, 2), false),
            Some(entry(1, 2))
        );
    }

    #[test]
    fn cumulative_ack_drops_the_tracking_of_earlier_entries() {
        let mut tracker = tracked();
        for (ledger_id, entry_id) in [(1, 1), (1, 2), (1, 3), (2, 0)] {
            tracker.track(entry(ledger_id, entry_id), AckSet::full(2));
        }
        assert_eq!(
            tracker.acknowledge(batched(1, 2, 0, 2), true),
            Some(entry(1, 1))
        );
        assert_eq!(tracker.tracked(), 3, "entries before (1, 2) are covered");
        assert_eq!(
            tracker.acknowledge(batched(1, 2, 1, 2), true),
            Some(entry(1, 2))
        );
        assert_eq!(tracker.tracked(), 2);
        assert_eq!(
            tracker.acknowledge(entry(1, 3), true),
            Some(entry(1, 3)),
            "a non-batched id acks its entry whole"
        );
        assert_eq!(tracker.tracked(), 1, "only (2, 0) is left");

        // The (ledger, -1) position covers earlier ledgers but not the ledger itself.
        tracker.track(entry(1, 9), AckSet::full(2));
        assert_eq!(
            tracker.acknowledge(batched(2, 0, 0, 2), true),
            Some(entry(2, u64::MAX))
        );
        assert_eq!(tracker.tracked(), 1);
        assert_eq!(
            tracker.acknowledge(batched(2, 0, 1, 2), false),
            Some(entry(2, 0))
        );
        assert_eq!(tracker.tracked(), 0);
    }

    #[test]
    fn whole_entry_ack_by_a_non_batched_id_drops_its_tracking() {
        let mut tracker = tracked();
        tracker.track(entry(1, 1), AckSet::full(2));
        assert_eq!(tracker.acknowledge(entry(1, 1), false), Some(entry(1, 1)));
        assert_eq!(tracker.tracked(), 0);
    }

    #[test]
    fn an_entry_the_broker_reports_fully_acked_is_not_tracked() {
        let mut tracker = tracked();
        tracker.track(entry(1, 1), AckSet::full(2));
        tracker.track(entry(1, 1), reported(2, &[0]));
        assert_eq!(tracker.tracked(), 0);
    }

    #[test]
    fn forget_and_clear_drop_tracked_entries() {
        let mut tracker = tracked();
        tracker.track(entry(1, 2), AckSet::full(2));
        tracker.track(entry(1, 3), AckSet::full(2));
        tracker.forget(&entry(1, 2));
        assert_eq!(tracker.tracked(), 1);
        tracker.clear();
        assert_eq!(tracker.tracked(), 0);
    }
}
