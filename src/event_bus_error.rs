/*******************************************************************************
 *
 *    Copyright (c) 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! Error type returned by event bus operations.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

/// Error returned by event bus configuration, publishing, or subscription work.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum EventBusError {
    /// Operation requires a started event bus.
    NotStarted,
    /// An argument value is invalid.
    InvalidArgument {
        /// Argument name.
        field: &'static str,
        /// Human-readable validation message.
        message: String,
    },
    /// A required builder field is missing.
    MissingField {
        /// Missing field name.
        field: &'static str,
    },
    /// Subscriber handler failed.
    HandlerFailed {
        /// Human-readable failure message.
        message: String,
    },
    /// Shared state lock was poisoned.
    LockPoisoned {
        /// Shared resource name.
        resource: &'static str,
    },
    /// A type-erased event or handler had an unexpected payload type.
    TypeMismatch {
        /// Expected Rust type name.
        expected: &'static str,
        /// Actual Rust type name.
        actual: &'static str,
    },
    /// A background thread panicked before returning a result.
    ThreadJoinFailed,
    /// Operation is not supported by this backend.
    UnsupportedOperation {
        /// Operation name or feature category.
        operation: &'static str,
    },
}

impl EventBusError {
    /// Creates [`EventBusError::NotStarted`].
    ///
    /// # Returns
    /// Error indicating that the bus must be started first.
    pub const fn not_started() -> Self {
        Self::NotStarted
    }

    /// Creates [`EventBusError::InvalidArgument`].
    ///
    /// # Parameters
    /// - `field`: Argument name.
    /// - `message`: Validation message.
    ///
    /// # Returns
    /// Validation error with field context.
    pub fn invalid_argument(field: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidArgument {
            field,
            message: message.into(),
        }
    }

    /// Creates [`EventBusError::MissingField`].
    ///
    /// # Parameters
    /// - `field`: Missing builder field.
    ///
    /// # Returns
    /// Builder validation error.
    pub const fn missing_field(field: &'static str) -> Self {
        Self::MissingField { field }
    }

    /// Creates [`EventBusError::HandlerFailed`].
    ///
    /// # Parameters
    /// - `message`: Handler failure description.
    ///
    /// # Returns
    /// Handler failure error.
    pub fn handler_failed(message: impl Into<String>) -> Self {
        Self::HandlerFailed {
            message: message.into(),
        }
    }

    /// Creates [`EventBusError::LockPoisoned`].
    ///
    /// # Parameters
    /// - `resource`: Name of the poisoned shared state.
    ///
    /// # Returns
    /// Lock-poisoning error.
    pub const fn lock_poisoned(resource: &'static str) -> Self {
        Self::LockPoisoned { resource }
    }

    /// Creates [`EventBusError::TypeMismatch`].
    ///
    /// # Parameters
    /// - `expected`: Expected type name.
    /// - `actual`: Actual type name.
    ///
    /// # Returns
    /// Type-erasure mismatch error.
    pub const fn type_mismatch(expected: &'static str, actual: &'static str) -> Self {
        Self::TypeMismatch { expected, actual }
    }

    /// Creates [`EventBusError::UnsupportedOperation`].
    ///
    /// # Parameters
    /// - `operation`: Operation name or feature category.
    ///
    /// # Returns
    /// Error indicating that the current backend does not support the operation.
    pub const fn unsupported_operation(operation: &'static str) -> Self {
        Self::UnsupportedOperation { operation }
    }
}

impl Display for EventBusError {
    /// Formats the error for logs and assertions.
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotStarted => write!(formatter, "the EventBus has not been started"),
            Self::InvalidArgument { field, message } => {
                write!(formatter, "invalid argument `{field}`: {message}")
            }
            Self::MissingField { field } => write!(formatter, "missing required field `{field}`"),
            Self::HandlerFailed { message } => write!(formatter, "event handler failed: {message}"),
            Self::LockPoisoned { resource } => {
                write!(formatter, "shared state lock was poisoned: {resource}")
            }
            Self::TypeMismatch { expected, actual } => {
                write!(
                    formatter,
                    "event payload type mismatch: expected {expected}, got {actual}"
                )
            }
            Self::ThreadJoinFailed => write!(formatter, "background thread panicked"),
            Self::UnsupportedOperation { operation } => {
                write!(formatter, "unsupported event bus operation: {operation}")
            }
        }
    }
}

impl Error for EventBusError {}
