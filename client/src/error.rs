use std::{io, fmt};
use std::sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}};
use serde_json;

#[derive(Debug)]
pub enum Error {
  Connection(ConnectionError),
  Consumer(ConsumerError),
  Producer(ProducerError),
}

impl From<ConnectionError> for Error {
    fn from(err: ConnectionError) -> Self {
        Error::Connection(err)
    }
}

impl From<ConsumerError> for Error {
    fn from(err: ConsumerError) -> Self {
        Error::Consumer(err)
    }
}

impl From<ProducerError> for Error {
    fn from(err: ProducerError) -> Self {
        Error::Producer(err)
    }
}

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match self {
      Error::Connection(e) => write!(f, "Connection error: {}", e),
      Error::Consumer(e) => write!(f, "consumer error: {}", e),
      Error::Producer(e) => write!(f, "producer error: {}", e),
    }
  }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Connection(e) => e.source(),
            Error::Consumer(e) => e.source(),
            Error::Producer(e) => e.source(),
        }
    }
}

#[derive(Debug)]
pub enum ConnectionError {
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

impl From<io::Error> for ConnectionError {
    fn from(err: io::Error) -> Self {
        ConnectionError::Io(err)
    }
}

impl fmt::Display for ConnectionError {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match self {
      ConnectionError::Io(e) => write!(f, "{}", e),
      ConnectionError::Disconnected => write!(f, "Disconnected"),
      ConnectionError::PulsarError(e) => write!(f, "{}", e),
      ConnectionError::Unexpected(e) => write!(f, "{}", e),
      ConnectionError::Decoding(e) => write!(f, "Error decoding message: {}", e),
      ConnectionError::Encoding(e) => write!(f, "Error encoding message: {}", e),
      ConnectionError::SocketAddr(e) => write!(f, "Error obtaning socket address: {}", e),
      ConnectionError::UnexpectedResponse(e) => write!(f, "Unexpected response from pulsar: {}", e),
      ConnectionError::Shutdown => write!(f, "The connection was shut down"),
    }
  }
}

impl std::error::Error for ConnectionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConnectionError::Io(e) => Some(e),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub enum ConsumerError {
    Connection(ConnectionError),
    MissingPayload(String),
    Serde(serde_json::Error),
}

impl From<ConnectionError> for ConsumerError {
    fn from(err: ConnectionError) -> Self {
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
    Connection(ConnectionError),
    Serde(serde_json::Error),
}

impl From<ConnectionError> for ProducerError {
    fn from(err: ConnectionError) -> Self {
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
    error: Arc<Mutex<Option<ConnectionError>>>,
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

    pub fn remove(&self) -> Option<ConnectionError> {
        let mut lock = self.error.lock().unwrap();
        let error = lock.take();
        self.error_set.store(false, Ordering::Release);
        error
    }

    pub fn set(&self, error: ConnectionError) {
        let mut lock = self.error.lock().unwrap();
        *lock = Some(error);
        self.error_set.store(true, Ordering::Release);
    }
}
