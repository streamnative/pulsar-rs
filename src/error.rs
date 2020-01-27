use std::{fmt, io};
use crate::proto::proto::CommandError;
use crate::proto::ServerError;

#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
}

impl Error {
    pub(crate) fn decoding<S: Into<String>>(msg: S) -> Error {
        Error { kind: ErrorKind::Decoding(msg.into()) }
    }

    pub(crate) fn encoding<S: Into<String>>(msg: S) -> Error {
        Error { kind: ErrorKind::Encoding(msg.into()) }
    }

    pub(crate) fn unexpected_response<S: Into<String>>(msg: S) -> Error {
        Error { kind: ErrorKind::UnexpectedResponse(msg.into()) }
    }

    pub(crate) fn disconnected<S: Into<String>>(msg: S) -> Error {
        Error { kind: ErrorKind::Disconnected(msg.into()) }
    }

    pub(crate) fn unexpected<S: Into<String>>(msg: S) -> Error {
        Error { kind: ErrorKind::Unexpected(msg.into()) }
    }

    pub(crate) fn pulsar(kind: ServerError, message: String) -> Error {
        Error { kind: ErrorKind::Pulsar {
            kind,
            message
        } }
    }
}

#[derive(Debug, Clone)]
enum ErrorKind {
    Io(String),
    Disconnected(String),
    Pulsar {
        kind: ServerError,
        message: String,
    },
    Unexpected(String),
    Decoding(String),
    Encoding(String),
    SocketAddr(String),
    UnexpectedResponse(String),
    Canceled,
    Shutdown,
    Custom(String),
}
//
//impl From<ConnectionError> for Error {
//    fn from(err: ConnectionError) -> Self {
//        Error::Connection(err)
//    }
//}
//
//impl From<ConsumerError> for Error {
//    fn from(err: ConsumerError) -> Self {
//        Error::Consumer(err)
//    }
//}
//
//impl From<ProducerError> for Error {
//    fn from(err: ProducerError) -> Self {
//        Error::Producer(err)
//    }
//}
//
//impl From<ServiceDiscoveryError> for Error {
//    fn from(err: ServiceDiscoveryError) -> Self {
//        Error::ServiceDiscovery(err)
//    }
//}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.kind {
            ErrorKind::Io(e) => write!(f, "pulsar error - Io: {}", e),
            ErrorKind::Disconnected(msg) => write!(f, "pulsar error - Disconnected: {}", msg),
            ErrorKind::Pulsar { kind, message } => write!(f, "pulsar error - {:?}: {}", kind, message),
            ErrorKind::Unexpected(e) => write!(f, "pulsar error - Unexpected: {}", e),
            ErrorKind::Decoding(e) => write!(f, "pulsar error - Decoding: {}", e),
            ErrorKind::Encoding(e) => write!(f, "pulsar error - Encoding: {}", e),
            ErrorKind::SocketAddr(e) => write!(f, "pulsar error - SocketAddr: {}", e),
            ErrorKind::UnexpectedResponse(e) => write!(f, "pulsar error - UnexpectedResponse: {}", e),
            ErrorKind::Canceled => write!(f, "pulsar error - Canceled"),
            ErrorKind::Shutdown => write!(f, "pulsar error - Shutdown"),
            ErrorKind::Custom(e) => write!(f, "pulsar error - Custom: {}", e),
        }
    }
}

impl std::error::Error for Error {
//    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
//        match self {
//            Error::Connection(e) => e.source(),
//            Error::Consumer(e) => e.source(),
//            Error::Producer(e) => e.source(),
//            Error::ServiceDiscovery(e) => e.source(),
//            Error::Custom(_) => None,
//        }
//    }
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
    Canceled,
    Shutdown,
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error { kind: ErrorKind::Io(err.to_string()) }
    }
}

impl From<CommandError> for Error {
    fn from(err: CommandError) -> Self {
        Error { kind: ErrorKind::Pulsar {
            kind: err.error as ServerError,
            message: err.message,
        }}
    }
}
//
//impl fmt::Display for ConnectionError {
//    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//        match self {
//            ConnectionError::Io(e) => write!(f, "{}", e),
//            ConnectionError::Disconnected => write!(f, "Disconnected"),
//            ConnectionError::PulsarError(e) => write!(f, "{}", e),
//            ConnectionError::Unexpected(e) => write!(f, "{}", e),
//            ConnectionError::Decoding(e) => write!(f, "Error decoding message: {}", e),
//            ConnectionError::Encoding(e) => write!(f, "Error encoding message: {}", e),
//            ConnectionError::SocketAddr(e) => write!(f, "Error obtaning socket address: {}", e),
//            ConnectionError::UnexpectedResponse(e) => {
//                write!(f, "Unexpected response from pulsar: {}", e)
//            }
//            ConnectionError::Canceled => write!(f, "canceled request"),
//            ConnectionError::Shutdown => write!(f, "The connection was shut down"),
//        }
//    }
//}

//impl std::error::Error for ConnectionError {
//    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
//        match self {
//            ConnectionError::Io(e) => Some(e),
//            _ => None,
//        }
//    }
//}
//
//impl fmt::Display for ConsumerError {
//    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//        match self {
//            ConsumerError::Connection(e) => write!(f, "Connection error: {}", e),
//            ConsumerError::MissingPayload(s) => write!(f, "Missing payload: {}", s),
//        }
//    }
//}
//
//impl std::error::Error for ConsumerError {
//    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
//        match self {
//            ConsumerError::Connection(e) => Some(e),
//            _ => None,
//        }
//    }
//}
//
//#[derive(Debug)]
//pub(crate) enum ProducerError {
//    Connection(ConnectionError),
//    Custom(String),
//}
//
//impl From<ConnectionError> for ProducerError {
//    fn from(err: ConnectionError) -> Self {
//        ProducerError::Connection(err)
//    }
//}
//
//impl fmt::Display for ProducerError {
//    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//        match self {
//            ProducerError::Connection(e) => write!(f, "Connection error: {}", e),
//            ProducerError::Custom(s) => write!(f, "Custom error: {}", s),
//        }
//    }
//}
//
//impl std::error::Error for ProducerError {
//    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
//        match self {
//            ProducerError::Connection(e) => Some(e),
//            ProducerError::Custom(_) => None,
//        }
//    }
//}
//
//#[derive(Debug)]
//pub(crate) enum ServiceDiscoveryError {
//    Connection(ConnectionError),
//    Query(String),
//    NotFound,
//    DnsLookupError,
//    Canceled,
//    Shutdown,
//    Dummy,
//}
//
//impl fmt::Display for ServiceDiscoveryError {
//    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//        match self {
//            ServiceDiscoveryError::Connection(e) => write!(f, "Connection error: {}", e),
//            ServiceDiscoveryError::Query(s) => write!(f, "Query error: {}", s),
//            ServiceDiscoveryError::NotFound => write!(f, "cannot find topic"),
//            ServiceDiscoveryError::DnsLookupError => write!(f, "cannot lookup broker address"),
//            ServiceDiscoveryError::Canceled => write!(f, "canceled request"),
//            ServiceDiscoveryError::Shutdown => write!(f, "service discovery engine not responding"),
//            ServiceDiscoveryError::Dummy => write!(f, "placeholder error"),
//        }
//    }
//}
//
//#[derive(Clone)]
//pub struct SharedError {
//    error_set: Arc<AtomicBool>,
//    error: Arc<Mutex<Option<ConnectionError>>>,
//}
//
//impl SharedError {
//    pub fn new() -> SharedError {
//        SharedError {
//            error_set: Arc::new(AtomicBool::new(false)),
//            error: Arc::new(Mutex::new(None)),
//        }
//    }
//
//    pub fn is_set(&self) -> bool {
//        self.error_set.load(Ordering::Relaxed)
//    }
//
//    pub fn remove(&self) -> Option<ConnectionError> {
//        let mut lock = self.error.lock().unwrap();
//        let error = lock.take();
//        self.error_set.store(false, Ordering::Release);
//        error
//    }
//
//    pub fn set(&self, error: ConnectionError) {
//        let mut lock = self.error.lock().unwrap();
//        *lock = Some(error);
//        self.error_set.store(true, Ordering::Release);
//    }
//}
