use futures::future::{Future, FutureExt};
use std::sync::Arc;
use std::pin::Pin;
use std::ops::Deref;
use tokio::runtime::Handle;

pub trait Executor: Send + Sync {
    fn spawn(&self, f: Pin<Box<dyn Future<Output = ()> + Send>>) -> Result<(), ()>;
}

#[derive(Clone)]
pub struct TaskExecutor {
    inner: Arc<dyn Executor + Send + Sync + 'static>,
}

impl TaskExecutor {
    pub fn new<E>(exec: E) -> Self
    where
        E: Executor + 'static,
    {
        Self {
            inner: Arc::new(exec),
        }
    }

    fn execute<F>(&self, f: F) -> Result<(), ()>
        where
            F: Future<Output = Result<(), ()>> + Send + 'static,
    {
        match self.inner.spawn(Box::pin(f.map(|_| ()))) {
            Ok(()) => Ok(()),
            Err(_) => panic!("no executor available"),
        }
    }
}

impl Executor for TaskExecutor
{
    fn spawn(&self, f: Pin<Box<dyn Future<Output = ()> + Send>>) -> Result<(), ()> {
        self.inner.deref().spawn(f)
    }
}

#[derive(Clone,Debug)]
pub struct TokioExecutor(pub Handle);

impl Executor for TokioExecutor {
    fn spawn(&self, f: Pin<Box<dyn Future<Output = ()> + Send>>) -> Result<(), ()> {
        self.0.spawn(f);
        Ok(())
    }
}
