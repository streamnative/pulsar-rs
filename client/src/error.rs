use std::{io, fmt};
use std::sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}};
use serde_json;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Disconnected,
    PulsarError(String),
    Unexpected(String),
    Decoding(String),
    Encoding(String),
    SocketAddr(String),
    UnexpectedResponse(String),
    Shutdown,
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
    }
}

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match self {
      Error::Io(e) => write!(f, "{}", e),
      Error::Disconnected => write!(f, "Disconnected"),
      Error::PulsarError(e) => write!(f, "{}", e),
      Error::Unexpected(e) => write!(f, "{}", e),
      Error::Decoding(e) => write!(f, "Error decoding message: {}", e),
      Error::Encoding(e) => write!(f, "Error encoding message: {}", e),
      Error::SocketAddr(e) => write!(f, "Error obtaning socket address: {}", e),
      Error::UnexpectedResponse(e) => write!(f, "Unexpected response from pulsar: {}", e),
      Error::Shutdown => write!(f, "The connection was shut down"),
    }
  }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub enum ConsumerError {
    Connection(Error),
    MissingPayload(String),
    Serde(serde_json::Error),
}

impl From<Error> for ConsumerError {
    fn from(err: Error) -> Self {
        ConsumerError::Connection(err)
    }
}

impl From<serde_json::Error> for ConsumerError {
    fn from(err: serde_json::Error) -> Self {
        ConsumerError::Serde(err)
    }
}

impl fmt::Display for ConsumerError {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match self {
      ConsumerError::Connection(e) => write!(f, "Connection error: {}", e),
      ConsumerError::MissingPayload(s) => write!(f, "Missing payload: {}", s),
      ConsumerError::Serde(e) => write!(f, "Deserialization error: {}", e),
    }
  }
}

impl std::error::Error for ConsumerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConsumerError::Connection(e) => Some(e),
            ConsumerError::Serde(e) => Some(e),
            _ => None,
        }
    }
}


#[derive(Debug)]
pub enum ProducerError {
    Connection(Error),
    Serde(serde_json::Error),
}

impl From<Error> for ProducerError {
    fn from(err: Error) -> Self {
        ProducerError::Connection(err)
    }
}

impl From<serde_json::Error> for ProducerError {
    fn from(err: serde_json::Error) -> Self {
        ProducerError::Serde(err)
    }
}

impl fmt::Display for ProducerError {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match self {
      ProducerError::Connection(e) => write!(f, "Connection error: {}", e),
      ProducerError::Serde(e) => write!(f, "Serialization error: {}", e),
    }
  }
}

impl std::error::Error for ProducerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ProducerError::Connection(e) => Some(e),
            ProducerError::Serde(e) => Some(e),
        }
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
