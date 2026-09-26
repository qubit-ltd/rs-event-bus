// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Conformance result types and shared runner helpers.

use std::any::TypeId;
use std::sync::Arc;
use std::time::SystemTime;

use super::super::EncodedPayload;
use super::super::OutboundMessage;
use super::super::PayloadModes;
use super::super::SettlementCapabilities;
use super::super::TopicAddress;
use super::super::TransportPayload;
pub(super) use super::conformance_case::ConformanceCase;
use crate::model::ContentType;
use crate::model::EventId;
use crate::model::Headers;
use crate::model::ProviderOptions;
use crate::model::StartPosition;
use crate::model::SubscriberId;
use crate::model::SubscriptionDurability;
use crate::spi::SpiSubscriptionRequest;

/// Results collected from one provider conformance run.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ConformanceReport {
    cases: Vec<ConformanceCase>,
}

impl ConformanceReport {
    /// Returns all case results in execution order.
    #[must_use]
    pub fn cases(&self) -> &[ConformanceCase] {
        &self.cases
    }

    /// Returns whether every executed case passed; skipped cases are allowed.
    #[must_use]
    pub fn all_passed(&self) -> bool {
        self.cases
            .iter()
            .all(|case| !matches!(case, ConformanceCase::Failed { .. }))
    }

    /// Panics with failed case details when any check failed.
    pub fn assert_all_passed(&self) {
        let failures = self
            .cases
            .iter()
            .filter_map(|case| match case {
                ConformanceCase::Failed { case_id, detail } => Some(format!("{case_id}: {detail}")),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(failures.is_empty(), "SPI conformance failures: {}", failures.join("; "));
    }

    pub(super) fn push(&mut self, case: ConformanceCase) {
        self.cases.push(case);
    }
}

pub(super) fn payload_probes(mode: PayloadModes) -> Vec<(&'static str, TransportPayload)> {
    match mode {
        PayloadModes::Native => vec![("declared-native-publish", TransportPayload::Native(Arc::new(0_u8)))],
        PayloadModes::Encoded => vec![("declared-encoded-publish", encoded_probe())],
        PayloadModes::NativeAndEncoded => vec![
            ("declared-native-publish", TransportPayload::Native(Arc::new(0_u8))),
            ("declared-encoded-publish", encoded_probe()),
        ],
    }
}

pub(super) fn encoded_probe() -> TransportPayload {
    TransportPayload::Encoded(EncodedPayload::new(
        Arc::<[u8]>::from(&b"spi-conformance"[..]),
        ContentType::new("application/octet-stream").expect("static MIME type is valid"),
        None,
    ))
}

pub(super) fn probe_message(payload: TransportPayload) -> OutboundMessage {
    OutboundMessage::new(
        TopicAddress::new("spi.conformance.probe").expect("static topic is valid"),
        EventId::new("spi-conformance-probe").expect("static event ID is valid"),
        SystemTime::UNIX_EPOCH,
        Headers::new(),
        None,
        None,
        payload,
    )
}

pub(super) fn probe_request(subscription_id: u64) -> SpiSubscriptionRequest {
    SpiSubscriptionRequest::new(
        qubit_id::Id::new(subscription_id),
        TopicAddress::new("spi.conformance.probe").expect("static topic is valid"),
        SubscriberId::new(format!("spi-conformance-{subscription_id}")).expect("probe subscriber ID is valid"),
        None,
        SubscriptionDurability::Ephemeral,
        StartPosition::New,
        ProviderOptions::default(),
        TypeId::of::<u8>(),
    )
}

pub(super) fn payload_matches(payload: &TransportPayload, expected: &str) -> bool {
    matches!(
        (expected, payload),
        ("declared-native-publish", TransportPayload::Native(value)) if value.is::<u8>()
    ) || matches!(
        (expected, payload),
        ("declared-encoded-publish", TransportPayload::Encoded(_))
    )
}

pub(super) fn settlement_case(
    settlement: SettlementCapabilities,
    token: Option<&super::super::SettlementToken>,
    settle: impl FnOnce(&super::super::SettlementToken) -> Result<(), crate::error::SpiError>,
) -> ConformanceCase {
    match (settlement, token) {
        (SettlementCapabilities::None, None) => ConformanceCase::Skipped {
            case_id: "settlement-idempotence".into(),
            reason: "provider does not support settlement".into(),
        },
        (SettlementCapabilities::None, Some(_)) => ConformanceCase::Failed {
            case_id: "settlement-idempotence".into(),
            detail: "provider issued a settlement token without settlement capability".into(),
        },
        (_, None) => ConformanceCase::Failed {
            case_id: "settlement-idempotence".into(),
            detail: "provider omitted a settlement token for a settlement-capable delivery".into(),
        },
        (_, Some(token)) => match settle(token) {
            Ok(()) => ConformanceCase::Passed {
                case_id: "settlement-idempotence".into(),
            },
            Err(error) => ConformanceCase::Failed {
                case_id: "settlement-idempotence".into(),
                detail: format!("repeated accept settlement failed: {error}"),
            },
        },
    }
}

pub(super) fn publish_case(case_id: &str, result: Result<(), crate::error::SpiError>, model: &str) -> ConformanceCase {
    match result {
        Ok(()) => ConformanceCase::Passed {
            case_id: case_id.into(),
        },
        Err(error) => ConformanceCase::Failed {
            case_id: case_id.into(),
            detail: format!("{model} publish rejected a declared payload: {error}"),
        },
    }
}

pub(super) fn push_hook(
    report: &mut ConformanceReport,
    case_id: &str,
    hook: Option<&Arc<dyn Fn() -> Result<(), String> + Send + Sync>>,
) {
    report.push(match hook {
        Some(check) => match check() {
            Ok(()) => ConformanceCase::Passed {
                case_id: case_id.into(),
            },
            Err(detail) => ConformanceCase::Failed {
                case_id: case_id.into(),
                detail,
            },
        },
        None => ConformanceCase::Skipped {
            case_id: case_id.into(),
            reason: "provider-specific fixture was not supplied".into(),
        },
    });
}
