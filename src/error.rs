//! Error types
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::{fmt, io};

#[derive(Debug)]
pub enum Error {
    Connection(ConnectionError),
    Consumer(ConsumerError),
    Producer(ProducerError),
    ServiceDiscovery(ServiceDiscoveryError),
    Custom(String),
    Executor,
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

impl From<ServiceDiscoveryError> for Error {
    fn from(err: ServiceDiscoveryError) -> Self {
        Error::ServiceDiscovery(err)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Connection(e) => write!(f, "Connection error: {}", e),
            Error::Consumer(e) => write!(f, "consumer error: {}", e),
            Error::Producer(e) => write!(f, "producer error: {}", e),
            Error::ServiceDiscovery(e) => write!(f, "service discovery error: {}", e),
            Error::Custom(e) => write!(f, "error: {}", e),
            Error::Executor => write!(f, "could not spawn task"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Connection(e) => e.source(),
            Error::Consumer(e) => e.source(),
            Error::Producer(e) => e.source(),
            Error::ServiceDiscovery(e) => e.source(),
            Error::Custom(_) => None,
            Error::Executor => None,
        }
    }
}

#[derive(Debug)]
pub enum ConnectionError {
    Io(io::Error),
    Disconnected,
    PulsarError(Option<crate::message::proto::ServerError>, Option<String>),
    Unexpected(String),
    Decoding(String),
    Encoding(String),
    SocketAddr(String),
    UnexpectedResponse(String),
    Tls(native_tls::Error),
    NotFound,
    Canceled,
    Shutdown,
}

impl From<io::Error> for ConnectionError {
    fn from(err: io::Error) -> Self {
        ConnectionError::Io(err)
    }
}

impl From<native_tls::Error> for ConnectionError {
    fn from(err: native_tls::Error) -> Self {
        ConnectionError::Tls(err)
    }
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ConnectionError::Io(e) => write!(f, "{}", e),
            ConnectionError::Disconnected => write!(f, "Disconnected"),
            ConnectionError::PulsarError(e, s) => {
                write!(f, "Server error ({:?}): {}", e, s.as_deref().unwrap_or(""))
            }
            ConnectionError::Unexpected(e) => write!(f, "{}", e),
            ConnectionError::Decoding(e) => write!(f, "Error decoding message: {}", e),
            ConnectionError::Encoding(e) => write!(f, "Error encoding message: {}", e),
            ConnectionError::SocketAddr(e) => write!(f, "Error obtaining socket address: {}", e),
            ConnectionError::Tls(e) => write!(f, "Error connecting TLS stream: {}", e),
            ConnectionError::UnexpectedResponse(e) => {
                write!(f, "Unexpected response from pulsar: {}", e)
            }
            ConnectionError::NotFound => write!(f, "error looking up URL"),
            ConnectionError::Canceled => write!(f, "canceled request"),
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
    Io(io::Error),
    ChannelFull,
    Closed,
}

impl From<ConnectionError> for ConsumerError {
    fn from(err: ConnectionError) -> Self {
        ConsumerError::Connection(err)
    }
}

impl From<io::Error> for ConsumerError {
    fn from(err: io::Error) -> Self {
        ConsumerError::Io(err)
    }
}

impl From<futures::channel::mpsc::SendError> for ConsumerError {
    fn from(err: futures::channel::mpsc::SendError) -> Self {
        if err.is_full() {
            ConsumerError::ChannelFull
        } else {
            ConsumerError::Closed
        }
    }
}

impl fmt::Display for ConsumerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ConsumerError::Connection(e) => write!(f, "Connection error: {}", e),
            ConsumerError::MissingPayload(s) => write!(f, "Missing payload: {}", s),
            ConsumerError::Io(s) => write!(f, "Decompression error: {}", s),
            ConsumerError::ChannelFull => write!(
                f,
                "cannot send message to the consumer engine: the channel is full"
            ),
            ConsumerError::Closed => write!(
                f,
                "cannot send message to the consumer engine: the channel is closed"
            ),
        }
    }
}

impl std::error::Error for ConsumerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConsumerError::Connection(e) => Some(e),
            _ => None,
        }
    }
}

pub enum ProducerError {
    Connection(ConnectionError),
    Custom(String),
    Io(io::Error),
    PartialSend(Vec<Result<SendFuture, Error>>),
    /// Indiciates the error was part of sending a batch, and thus shared across the batch
    Batch(Arc<Error>)
}

impl From<ConnectionError> for ProducerError {
    fn from(err: ConnectionError) -> Self {
        ProducerError::Connection(err)
    }
}

impl From<io::Error> for ProducerError {
    fn from(err: io::Error) -> Self {
        ProducerError::Io(err)
    }
}

impl fmt::Display for ProducerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ProducerError::Connection(e) => write!(f, "Connection error: {}", e),
            ProducerError::Io(e) => write!(f, "Compression error: {}", e),
            ProducerError::Custom(s) => write!(f, "Custom error: {}", s),
            ProducerError::Batch(e) => write!(f, "Batch error: {}", e),
            ProducerError::PartialSend(e) => {
                let (successes, failures) = e.iter().fold((0, 0), |(s, f), r| match r {
                    Ok(_) => (s + 1, f),
                    Err(_) => (s, f + 1)
                });
                write!(f, "Partial send error - {} successful, {} failed", successes, failures)?;

                if failures > 0 {
                    let first_error = e.iter()
                        .find(|r| r.is_err())
                        .unwrap()
                        .as_ref()
                        .map(drop)
                        .unwrap_err();
                    write!(f, "first error: {}", first_error)?;
                }
                Ok(())
            }
        }
    }
}

impl fmt::Debug for ProducerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProducerError::Connection(e) => write!(f, "Connection({:?})", e),
            ProducerError::Custom(msg) => write!(f, "Custom({:?})", msg),
            ProducerError::Io(e) => write!(f, "Connection({:?})", e),
            ProducerError::Batch(e) => write!(f, "Connection({:?})", e),
            ProducerError::PartialSend(parts) => {
                write!(f, "PartialSend(")?;
                for (i, part) in parts.iter().enumerate() {
                    match part {
                        Ok(_) => write!(f, "Ok(SendFuture)")?,
                        Err(e) => write!(f, "Err({:?})", e)?,
                    }
                    if i < (parts.len() - 1) {
                        write!(f, ", ")?;
                    }
                }
                write!(f, ")")
            }
        }
    }
}

impl std::error::Error for ProducerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ProducerError::Connection(e) => Some(e),
            ProducerError::Io(e) => Some(e),
            ProducerError::Batch(e) => Some(e.as_ref()),
            ProducerError::PartialSend(parts) => {
                parts.iter()
                    .find(|r| r.is_err())
                    .map(|r| r.as_ref().map(drop).unwrap_err() as _)
            },
            ProducerError::Custom(_) => None,


        }
    }
}

#[derive(Debug)]
pub enum ServiceDiscoveryError {
    Connection(ConnectionError),
    Query(Option<crate::message::proto::ServerError>, Option<String>),
    NotFound,
    DnsLookupError,
    Canceled,
    Shutdown,
    Dummy,
}

impl From<ConnectionError> for ServiceDiscoveryError {
    fn from(err: ConnectionError) -> Self {
        ServiceDiscoveryError::Connection(err)
    }
}

impl fmt::Display for ServiceDiscoveryError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ServiceDiscoveryError::Connection(e) => write!(f, "Connection error: {}", e),
            ServiceDiscoveryError::Query(e, s) => {
                write!(f, "Query error ({:?}): {}", e, s.as_deref().unwrap_or(""))
            }
            ServiceDiscoveryError::NotFound => write!(f, "cannot find topic"),
            ServiceDiscoveryError::DnsLookupError => write!(f, "cannot lookup broker address"),
            ServiceDiscoveryError::Canceled => write!(f, "canceled request"),
            ServiceDiscoveryError::Shutdown => write!(f, "service discovery engine not responding"),
            ServiceDiscoveryError::Dummy => write!(f, "placeholder error"),
        }
    }
}

impl std::error::Error for ServiceDiscoveryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ServiceDiscoveryError::Connection(e) => Some(e),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub(crate) struct SharedError {
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

use crate::message::proto::ServerError;
use crate::producer::SendFuture;

pub(crate) fn server_error(i: i32) -> Option<ServerError> {
    match i {
        0 => Some(ServerError::UnknownError),
        1 => Some(ServerError::MetadataError),
        2 => Some(ServerError::PersistenceError),
        3 => Some(ServerError::AuthenticationError),
        4 => Some(ServerError::AuthorizationError),
        5 => Some(ServerError::ConsumerBusy),
        6 => Some(ServerError::ServiceNotReady),
        7 => Some(ServerError::ProducerBlockedQuotaExceededError),
        8 => Some(ServerError::ProducerBlockedQuotaExceededException),
        9 => Some(ServerError::ChecksumError),
        10 => Some(ServerError::UnsupportedVersionError),
        11 => Some(ServerError::TopicNotFound),
        12 => Some(ServerError::SubscriptionNotFound),
        13 => Some(ServerError::ConsumerNotFound),
        14 => Some(ServerError::TooManyRequests),
        15 => Some(ServerError::TopicTerminatedError),
        16 => Some(ServerError::ProducerBusy),
        17 => Some(ServerError::InvalidTopicName),
        /* FIXME: why aren't they found by the compiler? Max enum size?
        18 => Some(ServerError::IncompatibleSchema),
        19 => Some(ServerError::ConsumerAssignError),
        20 => Some(ServerError::TransactionCoordinatorNotFound),
        21 => Some(ServerError::InvalidTxnStatus),
        */
        _ => None,
    }
}
