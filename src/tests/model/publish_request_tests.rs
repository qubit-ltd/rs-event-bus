// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Crate-internal publish request policy replacement tests.

use crate::model::DuplicateRiskPolicy;
use crate::model::PublishOptions;
use crate::model::PublishRequest;
use crate::model::Topic;

#[test]
fn test_replacement_options_are_the_values_returned_by_both_access_and_consumption() {
    let options = PublishOptions::<String>::builder()
        .duplicate_risk_policy(DuplicateRiskPolicy::AllowDuplicates)
        .build();
    let request = PublishRequest::new(
        Topic::<String>::new("model.internal.publish").expect("valid topic"),
        "value".into(),
    )
    .expect("request")
    .with_options(options);

    assert_eq!(
        request.options().duplicate_risk_policy(),
        DuplicateRiskPolicy::AllowDuplicates
    );
    let (_, consumed_options) = request.into_parts();
    assert_eq!(
        consumed_options.duplicate_risk_policy(),
        DuplicateRiskPolicy::AllowDuplicates
    );
}
