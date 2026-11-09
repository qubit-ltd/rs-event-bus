// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::num::NonZeroUsize;

use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::HeaderLimits;

#[test]
fn test_header_limits_are_configurable_and_defaults_are_finite() {
    let defaults = EventBusFacadeConfig::new().header_limits();
    assert_eq!(defaults.max_count().get(), 1_024);
    assert_eq!(defaults.max_bytes().get(), 65_536);

    let limits = HeaderLimits::new(NonZeroUsize::new(2).unwrap(), NonZeroUsize::new(7).unwrap());
    let config = EventBusFacadeConfig::new().with_header_limits(limits);
    assert_eq!(config.header_limits(), limits);
}

#[test]
fn test_publish_header_overflow_keeps_original_event_id_and_not_accepted_effect() {
    use qubit_event_bus::EventBus;
    use qubit_event_bus::PublishError;
    use qubit_event_bus::local::LocalEventBusConfig;
    use qubit_event_bus::model::EventId;
    use qubit_event_bus::model::PublishEffect;
    use qubit_event_bus::model::PublishOptions;
    use qubit_event_bus::model::PublishRequest;
    use qubit_event_bus::model::Topic;

    let limits = HeaderLimits::new(NonZeroUsize::new(1).unwrap(), NonZeroUsize::new(1).unwrap());
    let (bus, _monitor) = EventBus::local_with_config_and_monitor(
        LocalEventBusConfig::new(),
        EventBusFacadeConfig::new().with_header_limits(limits),
    )
    .unwrap();
    let options = PublishOptions::builder()
        .interceptor(|mut envelope: qubit_event_bus::model::EventEnvelope<String>| {
            envelope.set_header("k", "v")?;
            Ok(Some(envelope))
        })
        .build();
    let request = PublishRequest::builder()
        .topic(Topic::new("headers").unwrap())
        .payload("value".to_owned())
        .event_id(EventId::new("original-id").unwrap())
        .options(options)
        .build()
        .unwrap();
    let failure = bus.publish(request).unwrap_err();
    assert_eq!(failure.event_id().as_str(), "original-id");
    assert_eq!(failure.effect(), PublishEffect::NotAccepted);
    assert!(matches!(failure.cause(), PublishError::Headers(_)));
    let _ = bus.shutdown(qubit_event_bus::spi::ShutdownMode::Immediate).unwrap();
}
