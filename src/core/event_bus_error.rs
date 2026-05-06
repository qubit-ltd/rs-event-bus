/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Error type returned by event bus operations.

use std::error::Error;
use std::fmt::{
    self,
    Display,
    Formatter,
};

/// Result type used by event bus operations.
pub type EventBusResult<T> = Result<T, EventBusError>;

/// Error returned by event bus configuration, publishing, or subscription work.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum EventBusError {
    /// Operation requires a started event bus.
    NotStarted,
    /// The event bus could not be started.
    StartFailed {
        /// Human-readable startup failure reason.
        message: String,
    },
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
    /// Subscriber handler or interceptor panicked.
    HandlerPanicked,
    /// A configured interceptor failed while processing an event.
    InterceptorFailed {
        /// Interceptor phase.
        phase: &'static str,
        /// Human-readable failure message.
        message: String,
    },
    /// A configured error handler failed while processing another error.
    ErrorHandlerFailed {
        /// Error handling phase.
        phase: &'static str,
        /// Human-readable failure message.
        message: String,
    },
    /// Dead-letter routing failed after a terminal subscriber failure.
    DeadLetterFailed {
        /// Human-readable routing failure message.
        message: String,
    },
    /// Managed executor rejected event processing work.
    ExecutionRejected {
        /// Human-readable rejection reason.
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

    /// Creates [`EventBusError::StartFailed`].
    ///
    /// # Parameters
    /// - `message`: Startup failure details.
    ///
    /// # Returns
    /// Startup error with context.
    pub fn start_failed(message: impl Into<String>) -> Self {
        Self::StartFailed {
            message: message.into(),
        }
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

    /// Creates [`EventBusError::HandlerPanicked`].
    ///
    /// # Returns
    /// Handler panic error used by subscriber failure handling.
    pub const fn handler_panicked() -> Self {
        Self::HandlerPanicked
    }

    /// Creates [`EventBusError::InterceptorFailed`].
    ///
    /// # Parameters
    /// - `phase`: Interceptor phase.
    /// - `message`: Interceptor failure details.
    ///
    /// # Returns
    /// Interceptor failure with context.
    pub fn interceptor_failed(phase: &'static str, message: impl Into<String>) -> Self {
        Self::InterceptorFailed {
            phase,
            message: message.into(),
        }
    }

    /// Creates [`EventBusError::ErrorHandlerFailed`].
    ///
    /// # Parameters
    /// - `phase`: Error handling phase.
    /// - `message`: Handler failure details.
    ///
    /// # Returns
    /// Error handler failure with context.
    pub fn error_handler_failed(phase: &'static str, message: impl Into<String>) -> Self {
        Self::ErrorHandlerFailed {
            phase,
            message: message.into(),
        }
    }

    /// Creates [`EventBusError::DeadLetterFailed`].
    ///
    /// # Parameters
    /// - `message`: Dead-letter routing failure details.
    ///
    /// # Returns
    /// Dead-letter failure with context.
    pub fn dead_letter_failed(message: impl Into<String>) -> Self {
        Self::DeadLetterFailed {
            message: message.into(),
        }
    }

    /// Creates [`EventBusError::ExecutionRejected`].
    ///
    /// # Parameters
    /// - `message`: Executor rejection details.
    ///
    /// # Returns
    /// Rejection error with executor context.
    pub fn execution_rejected(message: impl Into<String>) -> Self {
        Self::ExecutionRejected {
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

    /// Returns a stable symbolic error kind.
    ///
    /// # Returns
    /// Static string useful for diagnostics and dead-letter metadata.
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::NotStarted => "not_started",
            Self::StartFailed { .. } => "start_failed",
            Self::InvalidArgument { .. } => "invalid_argument",
            Self::MissingField { .. } => "missing_field",
            Self::HandlerFailed { .. } => "handler_failed",
            Self::HandlerPanicked => "handler_panicked",
            Self::InterceptorFailed { .. } => "interceptor_failed",
            Self::ErrorHandlerFailed { .. } => "error_handler_failed",
            Self::DeadLetterFailed { .. } => "dead_letter_failed",
            Self::ExecutionRejected { .. } => "execution_rejected",
            Self::LockPoisoned { .. } => "lock_poisoned",
            Self::TypeMismatch { .. } => "type_mismatch",
            Self::UnsupportedOperation { .. } => "unsupported_operation",
        }
    }
}

impl Display for EventBusError {
    /// Formats the error for logs and assertions.
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotStarted => write!(formatter, "the EventBus has not been started"),
            Self::StartFailed { message } => {
                write!(formatter, "failed to start EventBus: {message}")
            }
            Self::InvalidArgument { field, message } => {
                write!(formatter, "invalid argument `{field}`: {message}")
            }
            Self::MissingField { field } => write!(formatter, "missing required field `{field}`"),
            Self::HandlerFailed { message } => write!(formatter, "event handler failed: {message}"),
            Self::HandlerPanicked => write!(formatter, "event handler panicked"),
            Self::InterceptorFailed { phase, message } => {
                write!(formatter, "{phase} interceptor failed: {message}")
            }
            Self::ErrorHandlerFailed { phase, message } => {
                write!(formatter, "{phase} error handler failed: {message}")
            }
            Self::DeadLetterFailed { message } => {
                write!(formatter, "dead-letter routing failed: {message}")
            }
            Self::ExecutionRejected { message } => {
                write!(formatter, "event processing task was rejected: {message}")
            }
            Self::LockPoisoned { resource } => {
                write!(formatter, "shared state lock was poisoned: {resource}")
            }
            Self::TypeMismatch { expected, actual } => {
                write!(
                    formatter,
                    "event payload type mismatch: expected {expected}, got {actual}"
                )
            }
            Self::UnsupportedOperation { operation } => {
                write!(formatter, "unsupported event bus operation: {operation}")
            }
        }
    }
}

impl Error for EventBusError {}
