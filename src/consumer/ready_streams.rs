//! Streams keyed by topic, polled only after they are woken.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, MutexGuard, PoisonError, Weak,
    },
    task::{Context, Poll},
};

use futures::{
    task::{waker_ref, ArcWake, AtomicWaker},
    Stream, StreamExt,
};

/// Streams keyed by topic.
///
/// Each stream is polled with its own waker. Waking it puts the stream on a ready queue, and
/// [`ReadyStreams::poll_next`] polls only the streams on that queue. The cost of a poll does not
/// grow with the number of idle streams. This is the technique of
/// [`futures::stream::FuturesUnordered`], kept beside a map so a stream can still be found by
/// its topic.
pub(super) struct ReadyStreams<S> {
    streams: BTreeMap<String, Entry<S>>,
    queue: Arc<ReadyQueue>,
}

struct Entry<S> {
    stream: S,
    waker: Arc<StreamWaker>,
}

#[derive(Default)]
struct ReadyQueue {
    ready: Mutex<VecDeque<Arc<StreamWaker>>>,
    /// The task that polls the [`ReadyStreams`].
    waker: AtomicWaker,
}

impl ReadyQueue {
    fn lock(&self) -> MutexGuard<'_, VecDeque<Arc<StreamWaker>>> {
        // A panic while the lock is held cannot leave the queue half updated.
        self.ready.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The waker of one stream.
struct StreamWaker {
    /// Shared so a delivered item carries its topic without an allocation.
    topic: Arc<str>,
    /// Set while the waker is on the ready queue, so a stream is queued once however often it is
    /// woken.
    queued: AtomicBool,
    /// Weak because the queue holds this waker while it is queued.
    queue: Weak<ReadyQueue>,
}

impl StreamWaker {
    /// Puts the stream on the ready queue unless it is there already. Returns the queue when
    /// this call added it.
    fn enqueue(self: &Arc<Self>) -> Option<Arc<ReadyQueue>> {
        let queue = self.queue.upgrade()?;
        if self.queued.swap(true, Ordering::SeqCst) {
            return None;
        }
        queue.lock().push_back(self.clone());
        Some(queue)
    }
}

impl ArcWake for StreamWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        if let Some(queue) = arc_self.enqueue() {
            queue.waker.wake();
        }
    }
}

impl<S> ReadyStreams<S> {
    pub(super) fn new() -> Self {
        ReadyStreams {
            streams: BTreeMap::new(),
            queue: Arc::new(ReadyQueue::default()),
        }
    }

    /// Adds a stream, replacing and returning the stream already kept for `topic`.
    pub(super) fn insert(&mut self, topic: String, stream: S) -> Option<S> {
        let waker = Arc::new(StreamWaker {
            topic: Arc::from(topic.as_str()),
            queued: AtomicBool::new(false),
            queue: Arc::downgrade(&self.queue),
        });
        // A new stream has not registered a waker yet, so it is polled once to register one.
        ArcWake::wake_by_ref(&waker);
        self.streams
            .insert(topic, Entry { stream, waker })
            .map(|entry| entry.stream)
    }

    /// Removes a stream. A wake of it that is still queued is skipped by
    /// [`ReadyStreams::poll_next`].
    pub(super) fn remove(&mut self, topic: &str) -> Option<S> {
        self.streams.remove(topic).map(|entry| entry.stream)
    }

    pub(super) fn get_mut(&mut self, topic: &str) -> Option<&mut S> {
        self.streams.get_mut(topic).map(|entry| &mut entry.stream)
    }

    pub(super) fn keys(&self) -> impl Iterator<Item = &String> {
        self.streams.keys()
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &S> {
        self.streams.values().map(|entry| &entry.stream)
    }

    pub(super) fn values_mut(&mut self) -> impl Iterator<Item = &mut S> {
        self.streams.values_mut().map(|entry| &mut entry.stream)
    }

    pub(super) fn iter_mut(&mut self) -> impl Iterator<Item = (&String, &mut S)> {
        self.streams
            .iter_mut()
            .map(|(topic, entry)| (topic, &mut entry.stream))
    }
}

impl<S: Stream + Unpin> ReadyStreams<S> {
    /// Polls the woken streams in the order they were woken until one is ready. Returns its
    /// topic and its item, which is `None` when the stream ended. A stream that ended stays in
    /// the map and is not polled again.
    ///
    /// A stream that yields an item goes back on the queue behind the streams already woken, so
    /// a stream that is always ready cannot starve the others.
    pub(super) fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<(Arc<str>, Option<S::Item>)> {
        // Registered before the queue is read, so a stream woken after this point wakes the
        // task, including a stream woken while the loop below runs.
        self.queue.waker.register(cx.waker());

        let queued = self.queue.lock().len();
        for _ in 0..queued {
            let Some(waker) = self.queue.lock().pop_front() else {
                break;
            };
            // Cleared before the poll, so a wake that arrives during the poll queues the stream
            // again instead of being lost.
            waker.queued.swap(false, Ordering::SeqCst);

            let Some(entry) = self.streams.get_mut(&*waker.topic) else {
                continue;
            };
            // A wake queued before the stream was removed or replaced.
            if !Arc::ptr_eq(&entry.waker, &waker) {
                continue;
            }

            let stream_waker = waker_ref(&waker);
            match entry
                .stream
                .poll_next_unpin(&mut Context::from_waker(&stream_waker))
            {
                Poll::Pending => {}
                Poll::Ready(Some(item)) => {
                    // A stream that returns an item does not register its waker, and it may
                    // have more items ready.
                    waker.enqueue();
                    return Poll::Ready((waker.topic.clone(), Some(item)));
                }
                Poll::Ready(None) => return Poll::Ready((waker.topic.clone(), None)),
            }
        }

        Poll::Pending
    }
}

impl<S> FromIterator<(String, S)> for ReadyStreams<S> {
    fn from_iter<I: IntoIterator<Item = (String, S)>>(iter: I) -> Self {
        let mut streams = ReadyStreams::new();
        for (topic, stream) in iter {
            streams.insert(topic, stream);
        }
        streams
    }
}

#[cfg(test)]
mod tests {
    use std::{
        pin::Pin,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use futures::{
        channel::mpsc,
        stream,
        task::{noop_waker_ref, waker},
    };

    use super::*;

    /// Counts the polls of the streams that share `polls`.
    struct Counted<S> {
        stream: S,
        polls: Arc<AtomicUsize>,
    }

    impl<S: Stream + Unpin> Stream for Counted<S> {
        type Item = S::Item;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<S::Item>> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            self.stream.poll_next_unpin(cx)
        }
    }

    type Receiver = Counted<mpsc::UnboundedReceiver<u32>>;

    fn counted(polls: &Arc<AtomicUsize>) -> (mpsc::UnboundedSender<u32>, Receiver) {
        let (tx, rx) = mpsc::unbounded();
        let rx = Counted {
            stream: rx,
            polls: polls.clone(),
        };
        (tx, rx)
    }

    #[derive(Default)]
    struct CountingWaker(AtomicUsize);

    impl ArcWake for CountingWaker {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn poll<S: Stream + Unpin>(streams: &mut ReadyStreams<S>) -> Poll<(Arc<str>, Option<S::Item>)> {
        streams.poll_next(&mut Context::from_waker(noop_waker_ref()))
    }

    #[test]
    fn idle_streams_are_polled_once_however_many_items_the_busy_stream_yields() {
        // The shape of the production case: 606 topics with 6 partitions each, one busy.
        const PARTITIONS: usize = 606 * 6;
        const BUSY: usize = PARTITIONS / 2;
        const IDLE: usize = PARTITIONS - 1;
        const ITEMS: u32 = 1_000;

        let idle_polls = Arc::new(AtomicUsize::new(0));
        let busy_polls = Arc::new(AtomicUsize::new(0));
        let mut streams = ReadyStreams::new();
        let mut senders = Vec::new();
        for i in 0..PARTITIONS {
            let (tx, rx) = counted(if i == BUSY { &busy_polls } else { &idle_polls });
            senders.push(tx);
            streams.insert(format!("partition-{i:04}"), rx);
        }
        let busy_topic: Arc<str> = Arc::from(format!("partition-{BUSY:04}"));

        for item in 0..ITEMS {
            senders[BUSY].unbounded_send(item).unwrap();
            assert_eq!(
                poll(&mut streams),
                Poll::Ready((busy_topic.clone(), Some(item)))
            );
        }

        // Each idle stream is polled once, to register its waker, and never woken after.
        assert_eq!(idle_polls.load(Ordering::SeqCst), IDLE);
        // One poll per item: a stream that returned an item is queued again, so the busy stream
        // never returns Pending here.
        assert_eq!(busy_polls.load(Ordering::SeqCst), ITEMS as usize);
    }

    #[test]
    fn an_item_sent_to_an_idle_stream_wakes_the_task_and_polls_only_that_stream() {
        let polls = Arc::new(AtomicUsize::new(0));
        let mut streams = ReadyStreams::new();
        let mut senders = Vec::new();
        for i in 0..10 {
            let (tx, rx) = counted(&polls);
            senders.push(tx);
            streams.insert(format!("topic-{i}"), rx);
        }
        let task = Arc::new(CountingWaker::default());
        let task_waker = waker(task.clone());
        let mut cx = Context::from_waker(&task_waker);

        assert_eq!(streams.poll_next(&mut cx), Poll::Pending);
        assert_eq!(polls.load(Ordering::SeqCst), 10);
        assert_eq!(task.0.load(Ordering::SeqCst), 0);

        senders[7].unbounded_send(42).unwrap();
        assert_eq!(task.0.load(Ordering::SeqCst), 1);
        assert_eq!(
            streams.poll_next(&mut cx),
            Poll::Ready((Arc::from("topic-7"), Some(42)))
        );
        assert_eq!(streams.poll_next(&mut cx), Poll::Pending);
        // The item poll and the Pending poll of topic-7.
        assert_eq!(polls.load(Ordering::SeqCst), 12);
    }

    #[test]
    fn a_stream_that_is_always_ready_does_not_starve_the_others() {
        let mut streams: ReadyStreams<Pin<Box<dyn Stream<Item = u32> + Send>>> =
            ReadyStreams::new();
        streams.insert("a".to_string(), Box::pin(stream::repeat(1)));
        streams.insert("b".to_string(), Box::pin(stream::repeat(2)));
        let (tx, rx) = mpsc::unbounded();
        streams.insert("c".to_string(), Box::pin(rx));

        let mut topics = Vec::new();
        for _ in 0..6 {
            let Poll::Ready((topic, Some(_))) = poll(&mut streams) else {
                panic!("an always-ready stream returned no item");
            };
            topics.push(topic.to_string());
        }
        assert_eq!(topics, ["a", "b", "a", "b", "a", "b"]);

        // c is queued behind the streams already queued, so it is polled within one round.
        tx.unbounded_send(3).unwrap();
        let mut topics = Vec::new();
        for _ in 0..3 {
            let Poll::Ready((topic, Some(_))) = poll(&mut streams) else {
                panic!("an always-ready stream returned no item");
            };
            topics.push(topic.to_string());
        }
        assert_eq!(topics, ["a", "b", "c"]);
    }

    /// Keeps the waker it is polled with and is never ready.
    struct KeepsWaker {
        waker: Arc<Mutex<Option<std::task::Waker>>>,
        polls: Arc<AtomicUsize>,
    }

    impl Stream for KeepsWaker {
        type Item = u32;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<u32>> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            *self.waker.lock().unwrap() = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    #[test]
    fn a_stream_woken_several_times_before_a_poll_is_polled_once() {
        let polls = Arc::new(AtomicUsize::new(0));
        let stream_waker = Arc::new(Mutex::new(None));
        let mut streams = ReadyStreams::new();
        streams.insert(
            "topic".to_string(),
            KeepsWaker {
                waker: stream_waker.clone(),
                polls: polls.clone(),
            },
        );
        assert_eq!(poll(&mut streams), Poll::Pending);

        let stream_waker = stream_waker.lock().unwrap().take().unwrap();
        stream_waker.wake_by_ref();
        stream_waker.wake_by_ref();
        stream_waker.wake();
        assert_eq!(poll(&mut streams), Poll::Pending);
        assert_eq!(polls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn an_ended_stream_is_returned_with_no_item_and_not_polled_again() {
        let polls = Arc::new(AtomicUsize::new(0));
        let mut streams = ReadyStreams::new();
        let (tx, rx) = counted(&polls);
        streams.insert("topic".to_string(), rx);
        drop(tx);

        assert_eq!(poll(&mut streams), Poll::Ready((Arc::from("topic"), None)));
        assert_eq!(poll(&mut streams), Poll::Pending);
        assert_eq!(polls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_queued_wake_of_a_replaced_stream_does_not_poll_its_replacement_twice() {
        let old_polls = Arc::new(AtomicUsize::new(0));
        let new_polls = Arc::new(AtomicUsize::new(0));
        let mut streams = ReadyStreams::new();
        let (old_tx, old_rx) = counted(&old_polls);
        streams.insert("topic".to_string(), old_rx);
        assert_eq!(poll(&mut streams), Poll::Pending);

        // The old stream's wake is queued when the stream is replaced.
        old_tx.unbounded_send(1).unwrap();
        let (new_tx, new_rx) = counted(&new_polls);
        assert!(streams.insert("topic".to_string(), new_rx).is_some());
        new_tx.unbounded_send(2).unwrap();

        assert_eq!(
            poll(&mut streams),
            Poll::Ready((Arc::from("topic"), Some(2)))
        );
        assert_eq!(poll(&mut streams), Poll::Pending);
        assert_eq!(old_polls.load(Ordering::SeqCst), 1);
        assert_eq!(new_polls.load(Ordering::SeqCst), 2);

        // Kept so the send below wakes the removed stream instead of failing.
        let removed = streams.remove("topic");
        assert!(removed.is_some());
        new_tx.unbounded_send(3).unwrap();
        assert_eq!(poll(&mut streams), Poll::Pending);
        assert_eq!(new_polls.load(Ordering::SeqCst), 2);
    }
}
