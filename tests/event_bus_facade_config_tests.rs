// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::codec::CodecRegistry;

#[test]
fn test_clone_shares_the_configured_codec_registry() {
    let codecs = Arc::new(CodecRegistry::new());
    let config = EventBusFacadeConfig::new().with_codec_registry(Arc::clone(&codecs));
    let cloned = config.clone();

    assert!(Arc::ptr_eq(
        config.codec_registry(),
        cloned.codec_registry()
    ));
    assert!(Arc::ptr_eq(config.codec_registry(), &codecs));
}
