
pub struct ReconnectingStream<T, E, S: Stream<Item = T, Error = E>> {
    stream: S,
    reconnecting: Option<Box<dyn Future<Item = S, Error = E>>>,
    reconnect: Box<Fn() -> Box<dyn Future<Item = S, Error = E>>>,
    max_retries: usize,
    current_retries: usize,
}

impl<T, E, S> ReconnectingStream<T, E, S>
    where S: Stream<Item = T, Error = E>
{
    pub fn new<F>(connect: F, max_retries: usize) -> impl Future<Item = ReconnectingStream<T, E, S>, Error = E>
        where F: Fn() -> Box<dyn Future<Item = S, Error = E>> + 'static
    {
        connect().map(move |stream| {
            ReconnectingStream {
                stream,
                reconnecting: None,
                reconnect: Box::new(connect),
                max_retries,
                current_retries: 0,
            }
        })
    }
}

impl<T, E, S> Stream for ReconnectingStream<T, E, S>
    where S: Stream<Item = T, Error = E>
{
    type Item = T;
    type Error = E;

    fn poll(&mut self) -> Result<Async<Option<T>>, E> {
        match self.reconnecting.as_mut().map(|r| r.poll()) {
            Some(Ok(Async::Ready(reconnected_stream))) => {
                self.stream = reconnected_stream;
                self.reconnecting = None;
            },
            Some(Ok(Async::NotReady)) => {
                return Ok(Async::NotReady);
            },
            Some(Err(e)) => {
                self.reconnecting = None;
                return Err(e);
            },
            None => {},
        };

        match self.stream.poll() {
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => Err(e),

            Ok(Async::Ready(Some(data))) => {
                //retries reset on successful data in order to prevent a continuously
                //reconnecting but empty stream from causing a stack overflow
                self.current_retries = 0;
                Ok(Async::Ready(Some(data)))
            },

            //needs reconnect
            Ok(Async::Ready(None)) => {
                self.current_retries += 1;
                if self.current_retries >= self.max_retries {
                    Ok(Async::Ready(None))
                } else {
                    self.reconnecting = Some((&self.reconnect)());
                    self.poll()
                }
            }
        }
    }
}
