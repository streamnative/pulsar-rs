use futures::{Future, Stream};
use std::pin::Pin;
use tokio::runtime::Handle;

pub enum ExecutorKind {
    Tokio,
    AsyncStd,
}

pub trait Executor: Clone + Send + Sync + 'static {
    fn spawn(f: Pin<Box<dyn Future<Output = ()> + Send>>) -> Result<(), ()>;
    fn spawn_blocking<F, Res>(f: F) -> JoinHandle<Res>
    where
        F: FnOnce() -> Res + Send + 'static,
        Res: Send + 'static;

    fn interval(duration: std::time::Duration) -> Interval;

    // test at runtime and manually choose the implementation
    // because we cannot (yet) have async trait methods,
    // so we cannot move the TCP connection here
    fn kind() -> ExecutorKind;
}

#[derive(Clone, Debug)]
pub struct TokioExecutor(pub Handle);

impl Executor for TokioExecutor {
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

    fn interval(duration: std::time::Duration) -> Interval {
        Interval::Tokio(tokio::time::interval(duration))
    }

    fn kind() -> ExecutorKind {
        ExecutorKind::Tokio
    }
}

#[derive(Clone, Debug)]
pub struct AsyncStdExecutor;

impl Executor for AsyncStdExecutor {
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

    fn interval(duration: std::time::Duration) -> Interval {
        Interval::AsyncStd(async_std::stream::interval(duration))
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

pub enum Interval {
  Tokio(tokio::time::Interval),
  AsyncStd(async_std::stream::Interval),
}

impl Stream for Interval {
    type Item = ();

    fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context) -> std::task::Poll<Option<Self::Item>> {
        unsafe {
            match Pin::get_unchecked_mut(self) {
                Interval::Tokio(j) => match Pin::new_unchecked(j).poll_next(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(v) => Poll::Ready(v.map(|_| ())),
                },
                Interval::AsyncStd(j) => match Pin::new_unchecked(j).poll_next(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(v) => Poll::Ready(v),
                },
            }
        }
    }
}
