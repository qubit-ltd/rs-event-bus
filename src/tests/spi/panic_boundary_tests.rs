// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Crate-internal source-chain contracts for provider panic classification.

use std::error::Error;
use std::panic::panic_any;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use crate::SpiError;
use crate::model::ProviderId;
use crate::spi::panic_boundary::catch_spi_call;
use crate::spi::panic_boundary::provider_panic;

struct PanicPayload;

impl Drop for PanicPayload {
    fn drop(&mut self) {
        panic!("panic payload destructor");
    }
}

static NORMAL_DROPS: AtomicUsize = AtomicUsize::new(0);
struct CountedPayload;
impl Drop for CountedPayload {
    fn drop(&mut self) {
        NORMAL_DROPS.fetch_add(1, Ordering::SeqCst);
    }
}

static SECONDARY_DROPS: AtomicUsize = AtomicUsize::new(0);
struct Secondary;
impl Drop for Secondary {
    fn drop(&mut self) {
        SECONDARY_DROPS.fetch_add(1, Ordering::SeqCst);
        panic!("secondary destructor");
    }
}
struct Primary;
impl Drop for Primary {
    fn drop(&mut self) {
        panic_any(Secondary);
    }
}

#[test]
fn test_spi_boundary_contains_panicking_panic_payload_destructor() {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        catch_spi_call::<()>("test", "publish", Some("topic"), || panic_any(PanicPayload))
    }));
    let error = result
        .expect("payload destructor panic must remain contained")
        .unwrap_err();
    assert!(matches!(error, SpiError::Operation {
        provider_id, operation: "publish", resource: Some(resource),
        kind: "provider_panicked", retryable: Some(false), ..
    } if provider_id.as_ref() == "test" && resource.as_ref() == "topic"));
}

#[test]
fn test_spi_boundary_disposes_panic_payload_once_and_forgets_secondary() {
    NORMAL_DROPS.store(0, Ordering::SeqCst);
    SECONDARY_DROPS.store(0, Ordering::SeqCst);
    let _ = catch_spi_call::<()>("test", "publish", None, || panic_any(CountedPayload));
    assert_eq!(NORMAL_DROPS.load(Ordering::SeqCst), 1);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        catch_spi_call::<()>("test", "publish", None, || panic_any(Primary))
    }));
    assert!(result.is_ok());
    assert_eq!(SECONDARY_DROPS.load(Ordering::SeqCst), 0);
}

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

#[test]
fn test_provider_panic_error_has_stable_operation_context() {
    let provider_id = ProviderId::new("panic-test").expect("valid provider ID");
    let error = provider_panic(provider_id.as_str(), "publish", None, Box::new("provider SPI panicked"));
    assert!(
        matches!(
            &error,
            SpiError::Operation {
                operation: "publish",
                kind: "provider_panicked",
                retryable: Some(false),
                ..
            }
        ),
        "provider panic should retain its stable operation classification"
    );
    assert_eq!(
        Error::source(&error)
            .expect("provider panic source remains chained")
            .to_string(),
        "provider SPI panicked"
    );
}
