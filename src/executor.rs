//! executor abstraction
//!
//! this crate is compatible with Tokio and async-std, by assembling  them
//! under the [Executor] trait
use std::{ops::Deref, pin::Pin, sync::Arc, task::Poll};

use futures::{Future, Stream};

/// indicates which executor is used
pub enum ExecutorKind {
    /// Tokio executor
    Tokio,
    /// async-std executor
    AsyncStd,
}

/// Wrapper trait abstracting the Tokio and async-std executors
pub trait Executor: Default + Clone + Send + Sync + 'static {
    /// spawns a new task
    #[allow(clippy::result_unit_err)]
    fn spawn(&self, f: Pin<Box<dyn Future<Output = ()> + Send>>) -> Result<(), ()>;

    /// spawns a new blocking task
    fn spawn_blocking<F, Res>(&self, f: F) -> JoinHandle<Res>
    where
        F: FnOnce() -> Res + Send + 'static,
        Res: Send + 'static;

    /// returns a Stream that will produce at regular intervals
    fn interval(&self, duration: std::time::Duration) -> Interval;

    /// waits for a configurable time
    fn delay(&self, duration: std::time::Duration) -> Delay;

    /// returns which executor is currently used
    // test at runtime and manually choose the implementation
    // because we cannot (yet) have async trait methods,
    // so we cannot move the TCP connection here
    fn kind(&self) -> ExecutorKind;
}

/// Wrapper for the Tokio executor
#[cfg(feature = "tokio-runtime")]
#[derive(Clone, Debug, Default)]
pub struct TokioExecutor;

#[cfg(feature = "tokio-runtime")]
impl Executor for TokioExecutor {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn spawn(&self, f: Pin<Box<dyn Future<Output = ()> + Send>>) -> Result<(), ()> {
        tokio::task::spawn(f);
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn spawn_blocking<F, Res>(&self, f: F) -> JoinHandle<Res>
    where
        F: FnOnce() -> Res + Send + 'static,
        Res: Send + 'static,
    {
        JoinHandle::Tokio(tokio::task::spawn_blocking(f))
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn interval(&self, duration: std::time::Duration) -> Interval {
        Interval::Tokio(tokio::time::interval(duration))
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn delay(&self, duration: std::time::Duration) -> Delay {
        Delay::Tokio(tokio::time::sleep(duration))
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn kind(&self) -> ExecutorKind {
        ExecutorKind::Tokio
    }
}

/// Wrapper for the async-std executor
#[cfg(feature = "async-std-runtime")]
#[derive(Clone, Debug, Default)]
pub struct AsyncStdExecutor;

#[cfg(feature = "async-std-runtime")]
impl Executor for AsyncStdExecutor {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn spawn(&self, f: Pin<Box<dyn Future<Output = ()> + Send>>) -> Result<(), ()> {
        async_std::task::spawn(f);
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn spawn_blocking<F, Res>(&self, f: F) -> JoinHandle<Res>
    where
        F: FnOnce() -> Res + Send + 'static,
        Res: Send + 'static,
    {
        JoinHandle::AsyncStd(async_std::task::spawn_blocking(f))
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn interval(&self, duration: std::time::Duration) -> Interval {
        Interval::AsyncStd(async_std::stream::interval(duration))
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn delay(&self, duration: std::time::Duration) -> Delay {
        use async_std::prelude::FutureExt;
        Delay::AsyncStd(Box::pin(async_std::future::ready(()).delay(duration)))
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn kind(&self) -> ExecutorKind {
        ExecutorKind::AsyncStd
    }
}

impl<Exe: Executor> Executor for Arc<Exe> {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn spawn(&self, f: Pin<Box<dyn Future<Output = ()> + Send>>) -> Result<(), ()> {
        self.deref().spawn(f)
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn spawn_blocking<F, Res>(&self, f: F) -> JoinHandle<Res>
    where
        F: FnOnce() -> Res + Send + 'static,
        Res: Send + 'static,
    {
        self.deref().spawn_blocking(f)
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn interval(&self, duration: std::time::Duration) -> Interval {
        self.deref().interval(duration)
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn delay(&self, duration: std::time::Duration) -> Delay {
        self.deref().delay(duration)
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn kind(&self) -> ExecutorKind {
        self.deref().kind()
    }
}

/// future returned by [Executor::spawn_blocking] to await on the task's result
pub enum JoinHandle<T> {
    /// wrapper for tokio's `JoinHandle`
    #[cfg(feature = "tokio-runtime")]
    Tokio(tokio::task::JoinHandle<T>),
    /// wrapper for async-std's `JoinHandle`
    #[cfg(feature = "async-std-runtime")]
    AsyncStd(async_std::task::JoinHandle<T>),
    // here to avoid a compilation error since T is not used
    #[cfg(all(not(feature = "tokio-runtime"), not(feature = "async-std-runtime")))]
    PlaceHolder(T),
}

impl<T> Future for JoinHandle<T> {
    type Output = Option<T>;

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context) -> std::task::Poll<Self::Output> {
        match self.get_mut() {
            #[cfg(feature = "tokio-runtime")]
            JoinHandle::Tokio(j) => match Pin::new(j).poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(v) => Poll::Ready(v.ok()),
            },
            #[cfg(feature = "async-std-runtime")]
            JoinHandle::AsyncStd(j) => match Pin::new(j).poll(cx) {
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

/// a `Stream` producing a `()` at regular time intervals
pub enum Interval {
    /// wrapper for tokio's interval
    #[cfg(feature = "tokio-runtime")]
    Tokio(tokio::time::Interval),
    /// wrapper for async-std's interval
    #[cfg(feature = "async-std-runtime")]
    AsyncStd(async_std::stream::Interval),
    #[cfg(all(not(feature = "tokio-runtime"), not(feature = "async-std-runtime")))]
    PlaceHolder,
}

impl Stream for Interval {
    type Item = ();

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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

/// a future producing a `()` after some time
pub enum Delay {
    /// wrapper around tokio's `Sleep`
    #[cfg(feature = "tokio-runtime")]
    Tokio(tokio::time::Sleep),
    /// wrapper around async-std's `Delay`
    #[cfg(feature = "async-std-runtime")]
    AsyncStd(Pin<Box<dyn Future<Output = ()> + Send>>),
}

impl Future for Delay {
    type Output = ();

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
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
