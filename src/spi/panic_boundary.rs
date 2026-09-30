// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Panic isolation for calls crossing a provider SPI boundary.

use std::any::Any;
use std::io::Error;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;

use crate::error::SpiError;

/// Runs one synchronous portion of a provider call and classifies an unwind.
///
/// # Type Parameters
/// - `T`: Result type returned by the provider operation when it succeeds.
///
/// # Parameters
/// - `provider_id`: identity attached to failures at the SPI boundary.
/// - `operation`: stable operation name included in a structured failure.
/// - `resource`: optional topic or resource associated with the operation.
/// - `call`: synchronous provider operation to invoke.
///
/// # Returns
/// The operation result, or a non-retryable provider panic error.
///
/// # Errors
/// Returns [`SpiError::Operation`] with kind `provider_panicked` if `call`
/// unwinds. Errors contained in `T` are returned as part of `Ok(T)`.
pub(crate) fn catch_spi_call<T>(
    provider_id: &str,
    operation: &'static str,
    resource: Option<&str>,
    call: impl FnOnce() -> T,
) -> Result<T, SpiError> {
    catch_unwind(AssertUnwindSafe(call)).map_err(|payload| provider_panic(provider_id, operation, resource, payload))
}

/// Converts a Rust panic payload into the stable provider failure shape.
///
/// # Parameters
/// - `provider_id`: provider identity reported in the resulting error.
/// - `operation`: stable operation name where the panic occurred.
/// - `resource`: optional resource associated with that operation.
/// - `payload`: panic value captured from the provider call.
///
/// # Returns
/// A non-retryable SPI operation error containing stable panic context.
pub(crate) fn provider_panic(
    provider_id: &str,
    operation: &'static str,
    resource: Option<&str>,
    payload: Box<dyn Any + Send>,
) -> SpiError {
    let message = payload
        .downcast_ref::<&'static str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("provider panicked");
    SpiError::Operation {
        provider_id: provider_id.into(),
        operation,
        resource: resource.map(Into::into),
        kind: "provider_panicked",
        retryable: Some(false),
        source: Box::new(Error::other(message.to_owned())),
    }
}

#[cfg(test)]
mod tests {
    use std::panic::panic_any;

    use super::catch_spi_call;
    use crate::error::SpiError;

    #[test]
    fn test_catches_provider_panic_payloads_and_preserves_call_results() {
        assert_eq!(
            7,
            catch_spi_call("test", "operation", None, || 7).expect("call succeeds")
        );
        let panic_calls: [Box<dyn Fn() -> usize>; 3] = [
            Box::new(|| panic_any("static panic")),
            Box::new(|| panic_any(String::from("owned panic"))),
            Box::new(|| panic_any(17_u8)),
        ];
        for panic_call in panic_calls {
            let result = catch_spi_call("test", "operation", Some("topic"), panic_call);
            assert!(matches!(
                result,
                Err(SpiError::Operation {
                    provider_id,
                    operation: "operation",
                    resource: Some(resource),
                    kind: "provider_panicked",
                    retryable: Some(false),
                    ..
                }) if provider_id.as_ref() == "test" && resource.as_ref() == "topic"
            ));
        }
    }
}
