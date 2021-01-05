use futures::{Future, Stream};
use std::{ops::Deref, pin::Pin, sync::Arc};

pub enum ExecutorKind {
    Tokio,
    AsyncStd,
}

/// Wrapper trait abstracting the Tokio and async-std executors
pub trait Executor: Clone + Send + Sync + 'static {
    #[allow(clippy::clippy::result_unit_err)]
    fn spawn(&self, f: Pin<Box<dyn Future<Output = ()> + Send>>) -> Result<(), ()>;
    fn spawn_blocking<F, Res>(&self, f: F) -> JoinHandle<Res>
    where
        F: FnOnce() -> Res + Send + 'static,
        Res: Send + 'static;

    fn interval(&self, duration: std::time::Duration) -> Interval;
    fn delay(&self, duration: std::time::Duration) -> Delay;

    // test at runtime and manually choose the implementation
    // because we cannot (yet) have async trait methods,
    // so we cannot move the TCP connection here
    fn kind(&self) -> ExecutorKind;
}

/// Wrapper for the Tokio executor
#[cfg(feature = "tokio-runtime")]
#[derive(Clone, Debug)]
pub struct TokioExecutor;

#[cfg(feature = "tokio-runtime")]
impl Executor for TokioExecutor {
    fn spawn(&self, f: Pin<Box<dyn Future<Output = ()> + Send>>) -> Result<(), ()> {
        tokio::task::spawn(f);
        Ok(())
    }

    fn spawn_blocking<F, Res>(&self, f: F) -> JoinHandle<Res>
    where
        F: FnOnce() -> Res + Send + 'static,
        Res: Send + 'static,
    {
        JoinHandle::Tokio(tokio::task::spawn_blocking(f))
    }

    fn interval(&self, duration: std::time::Duration) -> Interval {
        Interval::Tokio(tokio::time::interval(duration))
    }

    fn delay(&self, duration: std::time::Duration) -> Delay {
        Delay::Tokio(tokio::time::sleep(duration))
    }

    fn kind(&self) -> ExecutorKind {
        ExecutorKind::Tokio
    }
}

/// Wrapper for the async-std executor
#[cfg(feature = "async-std-runtime")]
#[derive(Clone, Debug)]
pub struct AsyncStdExecutor;

#[cfg(feature = "async-std-runtime")]
impl Executor for AsyncStdExecutor {
    fn spawn(&self, f: Pin<Box<dyn Future<Output = ()> + Send>>) -> Result<(), ()> {
        async_std::task::spawn(f);
        Ok(())
    }

    fn spawn_blocking<F, Res>(&self, f: F) -> JoinHandle<Res>
    where
        F: FnOnce() -> Res + Send + 'static,
        Res: Send + 'static,
    {
        JoinHandle::AsyncStd(async_std::task::spawn_blocking(f))
    }

    fn interval(&self, duration: std::time::Duration) -> Interval {
        Interval::AsyncStd(async_std::stream::interval(duration))
    }

    fn delay(&self, duration: std::time::Duration) -> Delay {
        use async_std::prelude::FutureExt;
        Delay::AsyncStd(Box::pin(async_std::future::ready(()).delay(duration)))
    }

    fn kind(&self) -> ExecutorKind {
        ExecutorKind::AsyncStd
    }
}

impl<Exe: Executor> Executor for Arc<Exe> {
    fn spawn(&self, f: Pin<Box<dyn Future<Output = ()> + Send>>) -> Result<(), ()> {
        self.deref().spawn(f)
    }

    fn spawn_blocking<F, Res>(&self, f: F) -> JoinHandle<Res>
    where
        F: FnOnce() -> Res + Send + 'static,
        Res: Send + 'static,
    {
        self.deref().spawn_blocking(f)
    }

    fn interval(&self, duration: std::time::Duration) -> Interval {
        self.deref().interval(duration)
    }

    fn delay(&self, duration: std::time::Duration) -> Delay {
        self.deref().delay(duration)
    }

    fn kind(&self) -> ExecutorKind {
        self.deref().kind()
    }
}

pub enum JoinHandle<T> {
    #[cfg(feature = "tokio-runtime")]
    Tokio(tokio::task::JoinHandle<T>),
    #[cfg(feature = "async-std-runtime")]
    AsyncStd(async_std::task::JoinHandle<T>),
    // here to avoid a compilation error since T is not used
    #[cfg(all(not(feature = "tokio-runtime"), not(feature = "async-std-runtime")))]
    PlaceHolder(T),
}

use std::task::Poll;
impl<T> Future for JoinHandle<T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context) -> std::task::Poll<Self::Output> {
        unsafe {
            match Pin::get_unchecked_mut(self) {
                #[cfg(feature = "tokio-runtime")]
                JoinHandle::Tokio(j) => match Pin::new_unchecked(j).poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(v) => Poll::Ready(v.ok()),
                },
                #[cfg(feature = "async-std-runtime")]
                JoinHandle::AsyncStd(j) => match Pin::new_unchecked(j).poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(v) => Poll::Ready(Some(v)),
                },
                #[cfg(all(not(feature = "tokio-runtime"), not(feature = "async-std-runtime")))]
                JoinHandle::PlaceHolder(t) => {
                    unimplemented!("please activate one of the following cargo features: tokio-runtime, async-std-runtime")
                }
            }
        }
    }
}

pub enum Interval {
    #[cfg(feature = "tokio-runtime")]
    Tokio(tokio::time::Interval),
    #[cfg(feature = "async-std-runtime")]
    AsyncStd(async_std::stream::Interval),
    #[cfg(all(not(feature = "tokio-runtime"), not(feature = "async-std-runtime")))]
    PlaceHolder,
}

impl Stream for Interval {
    type Item = ();

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context,
    ) -> std::task::Poll<Option<Self::Item>> {
        unsafe {
            match Pin::get_unchecked_mut(self) {
                #[cfg(feature = "tokio-runtime")]
                Interval::Tokio(j) => match Pin::new_unchecked(j).poll_tick(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(_) => Poll::Ready(Some(())),
                },
                #[cfg(feature = "async-std-runtime")]
                Interval::AsyncStd(j) => match Pin::new_unchecked(j).poll_next(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(v) => Poll::Ready(v),
                },
                #[cfg(all(not(feature = "tokio-runtime"), not(feature = "async-std-runtime")))]
                Interval::PlaceHolder => {
                    unimplemented!("please activate one of the following cargo features: tokio-runtime, async-std-runtime")
                }
            }
        }
    }
}

pub enum Delay {
    #[cfg(feature = "tokio-runtime")]
    Tokio(tokio::time::Sleep),
    #[cfg(feature = "async-std-runtime")]
    AsyncStd(Pin<Box<dyn Future<Output = ()> + Send>>),
}

impl Future for Delay {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context) -> std::task::Poll<Self::Output> {
        unsafe {
            match Pin::get_unchecked_mut(self) {
                #[cfg(feature = "tokio-runtime")]
                Delay::Tokio(d) => match Pin::new_unchecked(d).poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(_) => Poll::Ready(()),
                },
                #[cfg(feature = "async-std-runtime")]
                Delay::AsyncStd(j) => match Pin::new_unchecked(j).poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(_) => Poll::Ready(()),
                },
            }
        }
    }
}
