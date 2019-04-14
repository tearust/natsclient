//! # Error wrappers and boilerplate
//!
//! This module provides some basic boilerplate for errors. As a consumer of this
//! library, you should expect that all public functions return a `Result` type
//! using this local `Error`, which implements the standard Error trait.
//! As a general rule, errors that come from dependent crates are wrapped by
//! this crate's error type.
#![allow(unused_macros)]

use core::fmt;
use nats_types::DeliveredMessage;
use nats_types::ProtocolMessage;

use std::{
    error::Error as StdError,
    string::{String, ToString},
};

/// Provides an error type specific to the natsclient library
#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,

    description: Option<String>,
}

/// Provides context as to how a particular natsclient error might have occurred
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ErrorKind {
    /// Indicates a bad URL scheme was supplied for server address(es)
    InvalidUriScheme,
    /// URI Parse failure
    UriParseFailure,
    /// Invalid client configuration
    InvalidClientConfig,
    /// I/O failure
    IOError,
    /// Concurrency Failure
    ConcurrencyFailure,
    /// Subscription Failure
    SubscriptionFailure,
    // Timeout expired
    Timeout,
}

/// A handy macro borrowed from the `signatory` crate that lets library-internal code generate
/// more readable exception handling flows
#[macro_export]
macro_rules! err {
    ($variant:ident, $msg:expr) => {
        $crate::error::Error::new(
            $crate::error::ErrorKind::$variant,
            Some($msg)
        )
    };
    ($variant:ident, $fmt:expr, $($arg:tt)+) => {
        err!($variant, &format!($fmt, $($arg)+))
    };
}

impl ErrorKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorKind::InvalidUriScheme => "Invalid URI scheme",
            ErrorKind::UriParseFailure => "URI parse failure",
            ErrorKind::InvalidClientConfig => "Invalid client configuration",
            ErrorKind::IOError => "I/O failure",
            ErrorKind::ConcurrencyFailure => "Concurrency Failure",
            ErrorKind::SubscriptionFailure => "Subscription Failure",
            ErrorKind::Timeout => "Timeout expired",
        }
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Error {
    /// Creates a new natsclient error wrapper
    pub fn new(kind: ErrorKind, description: Option<&str>) -> Self {
        Error {
            kind,
            description: description.map(|desc| desc.to_string()),
        }
    }

    /// An accessor exposing the error kind enum. Crate consumers should have little to no
    /// need to access this directly and it's mostly used to assert that internal functions
    /// are creating appropriate error wrappers.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }
}

impl From<url::ParseError> for Error {
    fn from(source: url::ParseError) -> Error {
        err!(UriParseFailure, "URI parse failure: {}", source)
    }
}

impl From<std::io::Error> for Error {
    fn from(source: std::io::Error) -> Error {
        err!(IOError, "I/O error: {}", source)
    }
}

impl From<(ErrorKind, &'static str)> for Error {
    fn from((kind, description): (ErrorKind, &'static str)) -> Error {
        Error {
            kind,
            description: Some(description.to_string()),
        }
    }
}

impl From<crossbeam_channel::SendError<ProtocolMessage>> for Error {
    fn from(source: crossbeam_channel::SendError<ProtocolMessage>) -> Error {
        err!(ConcurrencyFailure, "Concurrency error: {}", source)
    }
}

impl From<crossbeam_channel::SendError<DeliveredMessage>> for Error {
    fn from(source: crossbeam_channel::SendError<DeliveredMessage>) -> Error {
        err!(ConcurrencyFailure, "Concurrency error: {}", source)
    }
}

impl From<crossbeam_channel::RecvTimeoutError> for Error {
    fn from(source: crossbeam_channel::RecvTimeoutError) -> Error {
        err!(Timeout, "Timeout expired: {}", source)
    }
}

impl StdError for Error {
    fn description(&self) -> &str {
        if let Some(ref desc) = self.description {
            desc
        } else {
            self.kind.as_str()
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.description {
            Some(ref desc) => write!(f, "{}: {}", self.description(), desc),
            None => write!(f, "{}", self.description()),
        }
    }
}
