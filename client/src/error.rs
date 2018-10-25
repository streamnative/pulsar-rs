use std::io;
use std::sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}};

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "{}", _0)]
    Io(io::Error),
    #[fail(display = "Disconnected")]
    Disconnected,
    #[fail(display = "{}", _0)]
    PulsarError(String),
    #[fail(display = "{}", _0)]
    Unexpected(String),
    #[fail(display = "Error decoding message: {}", _0)]
    Decoding(String),
    #[fail(display = "Error encoding message: {}", _0)]
    Encoding(String),
    #[fail(display = "Error obtaining socket address: {}", _0)]
    SocketAddr(String),
    #[fail(display = "Unexpected response from pulsar: {}", _0)]
    UnexpectedResponse(String)
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
    }
}

#[derive(Clone)]
pub struct SharedError {
    error_set: Arc<AtomicBool>,
    error: Arc<Mutex<Option<Error>>>,
}

impl SharedError {
    pub fn new() -> SharedError {
        SharedError {
            error_set: Arc::new(AtomicBool::new(false)),
            error: Arc::new(Mutex::new(None)),
        }
    }

    pub fn is_set(&self) -> bool {
        self.error_set.load(Ordering::Relaxed)
    }

    pub fn remove(&self) -> Option<Error> {
        let mut lock = self.error.lock().unwrap();
        let error = lock.take();
        self.error_set.store(false, Ordering::Release);
        error
    }

    pub fn set(&self, error: Error) {
        let mut lock = self.error.lock().unwrap();
        *lock = Some(error);
        self.error_set.store(true, Ordering::Release);
    }
}
