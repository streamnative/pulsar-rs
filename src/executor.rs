use futures::future::{ExecuteError, Executor, Future};
use std::sync::Arc;

pub trait PulsarExecutor: Executor<BoxSendFuture> + Send + Sync + 'static {}

impl<T: Executor<BoxSendFuture> + Send + Sync + 'static> PulsarExecutor for T {}

type BoxSendFuture = Box<dyn Future<Item = (), Error = ()> + Send + 'static>;

#[derive(Clone)]
pub(crate) struct TaskExecutor {
    inner: Arc<dyn Executor<BoxSendFuture> + Send + Sync + 'static>,
}

impl TaskExecutor {
    pub(crate) fn new<E>(exec: E) -> Self
    where
        E: PulsarExecutor,
    {
        Self {
            inner: Arc::new(exec),
        }
    }
    pub(crate) fn spawn<F>(&self, f: F)
    where
        F: Future<Item = (), Error = ()> + Send + 'static,
    {
        if self.execute(f).is_err() {
            panic!("no executor available")
        }
    }
}

impl<F> Executor<F> for TaskExecutor
where
    F: Future<Item = (), Error = ()> + Send + 'static,
{
    fn execute(&self, f: F) -> Result<(), ExecuteError<F>> {
        match self.inner.execute(Box::new(f)) {
            Ok(()) => Ok(()),
            Err(_) => panic!("no executor available"),
        }
    }
}
