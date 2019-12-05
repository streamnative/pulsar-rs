use tokio::prelude::*;
use std::time::{Duration, Instant};
use tokio_retry::strategy::*;
use tokio::timer::Delay;
use std::sync::Arc;

pub enum Error<E> {
    Reconnect(E),
    Abort(E),
}

pub struct ReconnectingStream<T, E> {
    stream: Box<dyn Stream<Item=T, Error=E> + Send>,
    reconnecting: Option<Box<dyn Future<Item=Box<dyn Stream<Item=T, Error=E> + Send>, Error=E>>>,
    reconnect: Arc<Fn() -> Box<dyn Future<Item=Box<dyn Stream<Item=T, Error=E> + Send>, Error=E>>>,
    max_retries: u32,
    max_backoff: Duration,
    current_retries: u32,
    current_backoff: ExponentialBackoff,
}

impl<T, E> ReconnectingStream<T, E> {
    pub fn new<F>(connect: F, max_retries: u32, max_backoff: Duration) -> impl Future<Item=ReconnectingStream<T, E>, Error=E>
        where F: Fn() -> Box<dyn Future<Item=Box<dyn Stream<Item=T, Error=E> + Send>, Error=E>> + Send + 'static
    {
        connect().map(move |stream| {
            ReconnectingStream {
                stream,
                reconnecting: None,
                reconnect: Arc::new(connect),
                max_retries,
                max_backoff,
                current_retries: 0,
                current_backoff: backoff(max_backoff),
            }
        })
    }

    fn reset_backoff(&mut self) {
        self.current_retries = 0;
        self.reconnecting = None;
        self.current_backoff = backoff(self.max_backoff);
    }
}

fn backoff(max_backoff: Duration) -> ExponentialBackoff {
    ExponentialBackoff::from_millis(10).max_delay(max_backoff)
}

impl<T: 'static, E: 'static> Stream for ReconnectingStream<T, E>
    where Error<E>: From<E>
{
    type Item = T;
    type Error = E;

    fn poll(&mut self) -> Result<Async<Option<T>>, E> {
        if let Some(reconnecting) = self.reconnecting.as_mut() {
            match reconnecting.poll().map_err(Error::from) {
                Ok(Async::NotReady) => {
                    return Ok(Async::NotReady);
                }
                Ok(Async::Ready(reconnected_stream)) => {
                    self.stream = reconnected_stream;
                    self.reset_backoff()
                }

                Err(Error::Abort(e)) => return Err(e),

                Err(Error::Reconnect(e)) => {
                    if self.current_retries >= self.max_retries {
                        return Err(e)
                    }
                    self.current_retries += 1;
                    let reconnect = self.reconnect.clone();
                    self.reconnecting = Some(Box::new(Delay::new(Instant::now() + self.current_backoff.next().unwrap())
                        .map_err(|_| unreachable!())
                        .and_then(move |_| reconnect())));
                    return self.poll();
                }
            }
        }

        match self.stream.poll().map_err(|e| e.into()) {
            Ok(Async::NotReady) => Ok(Async::NotReady),

            Ok(Async::Ready(Some(data))) => Ok(Async::Ready(Some(data))),

            Ok(Async::Ready(None)) if self.current_retries >= self.max_retries => Ok(Async::Ready(None)),

            Ok(Async::Ready(None)) => {
                self.current_retries += 1;
                let reconnect = self.reconnect.clone();
                self.reconnecting = Some(Box::new(Delay::new(Instant::now() + self.current_backoff.next().unwrap())
                    .map_err(|_| unreachable!())
                    .and_then(move |_| reconnect())));
                self.poll()
            }

            Err(Error::Abort(e)) => Err(e),

            Err(Error::Reconnect(e)) => {
                if self.current_retries >= self.max_retries {
                    return Err(e);
                }
                self.current_retries += 1;
                self.reconnecting = Some((&self.reconnect)());
                self.poll()
            }
        }
    }
}
