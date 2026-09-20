// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use qubit_event_bus::DeliveryLimits;

#[test]
fn test_delivery_limits_expose_and_validate_bounds() {
    let bounded = DeliveryLimits::bounded(8, Some(2));

    assert_eq!(bounded.max_in_flight(), 8);
    assert_eq!(bounded.handler_queue_capacity(), Some(2));
    assert!(DeliveryLimits::new(0, None).validate().is_err());
    assert!(DeliveryLimits::new(1, Some(0)).validate().is_err());
    assert_eq!(
        DeliveryLimits::unbounded(4)
            .validate()
            .expect("positive unbounded limits should validate")
            .handler_queue_capacity(),
        None
    );
}
