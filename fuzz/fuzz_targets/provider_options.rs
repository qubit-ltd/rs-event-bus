// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#![no_main]

use libfuzzer_sys::fuzz_target;
use qubit_event_bus::SubscriberId;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;

const OPTION_BUDGET: usize = 4 * 1024;
const SECRET_MARKER: &str = "fuzz-secret-marker-must-not-appear-in-errors";

fuzz_target!(|input: &[u8]| {
    let bounded = &input[..input.len().min(OPTION_BUDGET)];
    let Some(separator) = bounded.iter().position(|byte| *byte == 0xff) else {
        return;
    };
    let (Ok(key), Ok(value)) = (
        std::str::from_utf8(&bounded[..separator]),
        std::str::from_utf8(&bounded[separator + 1..]),
    ) else {
        return;
    };
    let key = key.to_owned();
    let value = value.to_owned();

    let mut options = ProviderOptions::new();
    options.insert(key.clone(), value.clone());
    let result = SubscribeRequest::<Vec<u8>>::builder()
        .subscriber_id(SubscriberId::new("fuzz-subscriber").unwrap())
        .topic(Topic::new("fuzz.provider-options").unwrap())
        .provider_options(options)
        .build();

    match result {
        Ok(request) => {
            for (key, value) in request.options().provider_options() {
                assert!(key.contains('.'));
                assert!(!key.starts_with('.') && !key.ends_with('.'));
                assert!(!key.chars().any(char::is_control));
                assert!(!value.chars().any(char::is_control));
                assert!(key.len() + value.len() <= OPTION_BUDGET);
            }
        }
        Err(_) => {}
    }

    let mut secret_option = ProviderOptions::new();
    secret_option.insert("provider.token".into(), SECRET_MARKER.into());
    let result = SubscribeRequest::<Vec<u8>>::builder()
        .subscriber_id(SubscriberId::new("fuzz-subscriber").unwrap())
        .topic(Topic::new("fuzz.provider-options").unwrap())
        .provider_options(secret_option)
        .provider_option("invalid", "discarded")
        .build();
    if let Err(error) = result {
        assert!(!error.to_string().contains(SECRET_MARKER));
    }
});
