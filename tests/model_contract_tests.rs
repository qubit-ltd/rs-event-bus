// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_event_bus::SubscriberId;

#[test]
fn subscriber_id_enforces_portable_syntax() {
    assert!(SubscriberId::new("audit-1:primary").is_ok());
    assert!(SubscriberId::new("").is_err());
    assert!(SubscriberId::new(" audit").is_err());
    assert!(SubscriberId::new("_audit").is_err());
    assert!(SubscriberId::new("审计").is_err());
    assert!(SubscriberId::new(&"a".repeat(129)).is_err());
}
