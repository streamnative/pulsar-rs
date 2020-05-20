use futures::future::{Future, FutureExt};
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use tokio::runtime::Handle;

pub trait Executor: Send + Sync {
    fn spawn(&self, f: Pin<Box<dyn Future<Output = ()> + Send>>) -> Result<(), ()>;
}

pub enum ExecutorKind {
    Tokio,
    AsyncStd,
}

pub trait PulsarExecutor: Clone + Send + Sync + 'static {
    fn spawn(f: Pin<Box<dyn Future<Output = ()> + Send>>) -> Result<(), ()>;
    fn spawn_blocking<F, Res>(f: F) -> JoinHandle<Res>
    where
        F: FnOnce() -> Res + Send + 'static,
        Res: Send + 'static;

    // test at runtime and manually choose the implementation
    // because we cannot (yet) have async trait methods,
    // so we cannot move the TCP connection here
    fn kind() -> ExecutorKind;
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

impl Executor for TaskExecutor {
    fn spawn(&self, f: Pin<Box<dyn Future<Output = ()> + Send>>) -> Result<(), ()> {
        self.inner.deref().spawn(f)
    }
}

#[derive(Clone, Debug)]
pub struct TokioExecutor(pub Handle);

impl Executor for TokioExecutor {
    fn spawn(&self, f: Pin<Box<dyn Future<Output = ()> + Send>>) -> Result<(), ()> {
        self.0.spawn(f);
        Ok(())
    }
}

impl PulsarExecutor for TokioExecutor {
    fn spawn(f: Pin<Box<dyn Future<Output = ()> + Send>>) -> Result<(), ()> {
        tokio::task::spawn(f);
        Ok(())
    }

    fn spawn_blocking<F, Res>(f: F) -> JoinHandle<Res>
    where
        F: FnOnce() -> Res + Send + 'static,
        Res: Send + 'static,
    {
        JoinHandle::Tokio(tokio::task::spawn_blocking(f))
    }

    fn kind() -> ExecutorKind {
        ExecutorKind::Tokio
    }
}

#[derive(Clone, Debug)]
pub struct AsyncStdExecutor;

impl Executor for AsyncStdExecutor {
    fn spawn(&self, f: Pin<Box<dyn Future<Output = ()> + Send>>) -> Result<(), ()> {
        async_std::task::spawn(f);
        Ok(())
    }
}

impl PulsarExecutor for AsyncStdExecutor {
    fn spawn(f: Pin<Box<dyn Future<Output = ()> + Send>>) -> Result<(), ()> {
        async_std::task::spawn(f);
        Ok(())
    }

    fn spawn_blocking<F, Res>(f: F) -> JoinHandle<Res>
    where
        F: FnOnce() -> Res + Send + 'static,
        Res: Send + 'static,
    {
        JoinHandle::AsyncStd(async_std::task::spawn_blocking(f))
    }

    fn kind() -> ExecutorKind {
        ExecutorKind::AsyncStd
    }
}

pub enum JoinHandle<T> {
    Tokio(tokio::task::JoinHandle<T>),
    AsyncStd(async_std::task::JoinHandle<T>),
}

use std::task::Poll;
impl<T> Future for JoinHandle<T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context) -> std::task::Poll<Self::Output> {
        unsafe {
            match Pin::get_unchecked_mut(self) {
                JoinHandle::Tokio(j) => match Pin::new_unchecked(j).poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(v) => Poll::Ready(v.ok()),
                },
                JoinHandle::AsyncStd(j) => match Pin::new_unchecked(j).poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(v) => Poll::Ready(Some(v)),
                },
            }
        }
    }
}
