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

use qubit_id::Id;

use super::super::EncodedPayload;
use super::super::OutboundMessage;
use super::super::PayloadModes;
use super::super::SettlementCapabilities;
use super::super::SettlementToken;
use super::super::TopicAddress;
use super::super::TransportPayload;
pub(super) use super::conformance_case::ConformanceCase;
use super::conformance_profile::ConformanceProfile;
use super::conformance_skip_reason::ConformanceSkipReason;
use crate::error::SpiError;
use crate::model::ContentType;
use crate::model::EventId;
use crate::model::Headers;
use crate::model::ProviderOptions;
use crate::model::StartPosition;
use crate::model::SubscriberId;
use crate::model::SubscriptionDurability;
use crate::spi::DurabilityCapability;
use crate::spi::SpiSubscriptionRequest;

/// Results collected from one provider conformance run.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::spi::conformance::ConformanceReport;
///
/// let report = ConformanceReport::default();
/// assert!(report.all_passed());
/// assert!(report.cases().is_empty());
/// ```
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[must_use]
pub struct ConformanceReport {
    /// Pass, failure, and skip outcomes in their execution order.
    cases: Vec<ConformanceCase>,
}

impl ConformanceReport {
    /// Returns all case results in execution order.
    ///
    /// # Returns
    /// The case results in the order the runner produced them.
    #[must_use = "Use the returned cases."]
    #[inline]
    pub fn cases(&self) -> &[ConformanceCase] {
        &self.cases
    }

    /// Returns whether every executed case passed; skipped cases are allowed.
    ///
    /// # Returns
    /// `true` unless at least one case failed.
    #[must_use]
    pub fn all_passed(&self) -> bool {
        self.cases
            .iter()
            .all(|case| !matches!(case, ConformanceCase::Failed { .. }))
    }

    /// Panics with failed case details when any check failed.
    ///
    /// # Panics
    /// Panics when one or more conformance cases failed, including their IDs
    /// and details in the panic message.
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

    /// Appends a case result after previously recorded cases.
    ///
    /// # Parameters
    /// - `case`: conformance outcome to append.
    pub(super) fn push(&mut self, case: ConformanceCase) {
        self.cases.push(case);
    }

    /// Records a check outside the selected API or capability contract.
    ///
    /// # Parameters
    /// - `case_id`: stable identifier of the inapplicable check.
    /// - `reason`: explanation of why the check is outside this contract.
    pub(super) fn not_applicable(&mut self, case_id: &str, reason: &'static str) {
        self.push(ConformanceCase::Skipped {
            case_id: case_id.into(),
            reason: ConformanceSkipReason::NotApplicable { reason },
        });
    }

    /// Converts missing-fixture skips to failures under the strict profile.
    ///
    /// # Parameters
    /// - `profile`: policy applied to skipped checks.
    pub(super) fn apply_profile(&mut self, profile: ConformanceProfile) {
        if profile == ConformanceProfile::Strict {
            for case in &mut self.cases {
                let ConformanceCase::Skipped { case_id, reason } = case else {
                    continue;
                };
                if let ConformanceSkipReason::MissingFixture { .. } = reason {
                    *case = ConformanceCase::Failed {
                        case_id: case_id.clone(),
                        detail: format!("strict profile requires this check: {reason}"),
                    };
                }
            }
        }
    }
}

/// Builds the outbound payload probes required by a provider's declaration.
///
/// # Parameters
/// - `mode`: payload modes declared by the provider.
///
/// # Returns
/// Named native and/or encoded probe payloads.
#[must_use]
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

/// Creates the standard encoded conformance payload.
///
/// # Returns
/// A transport payload containing fixed probe bytes.
pub(super) fn encoded_probe() -> TransportPayload {
    TransportPayload::Encoded(EncodedPayload::new(
        Arc::<[u8]>::from(&b"spi-conformance"[..]),
        ContentType::APPLICATION_OCTET_STREAM,
        None,
    ))
}

/// Creates the standard outbound probe message.
///
/// # Parameters
/// - `payload`: representation under test.
///
/// # Returns
/// A message addressed to the conformance probe topic.
///
/// # Panics
/// Panics only if the fixed probe topic or event ID is rejected as invalid.
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

/// Creates the standard subscription request for a provider probe.
///
/// # Parameters
/// - `subscription_id`: unique bus-local ID for this case.
/// - `durability`: provider durability used to select a compatible mode.
///
/// # Returns
/// A request subscribed to the conformance probe topic.
///
/// # Panics
/// Panics if the generated subscriber ID or the fully configured request is
/// rejected by its constructor or builder.
pub(super) fn probe_request(subscription_id: u64, durability: DurabilityCapability) -> SpiSubscriptionRequest {
    SpiSubscriptionRequest::builder()
        .subscription_id(Id::new(subscription_id))
        .topic(TopicAddress::new("spi.conformance.probe").expect("static topic is valid"))
        .subscriber_id(
            SubscriberId::new(format!("spi-conformance-{subscription_id}")).expect("probe subscriber ID is valid"),
        )
        .group(None)
        .durability(match durability {
            DurabilityCapability::Durable => SubscriptionDurability::Durable,
            DurabilityCapability::Ephemeral => SubscriptionDurability::Ephemeral,
        })
        .start_position(StartPosition::New)
        .provider_options(ProviderOptions::default())
        .payload_type_id(TypeId::of::<u8>())
        .build()
        .expect("all conformance request fields are configured")
}

/// Checks whether a received transport payload matches the named probe.
///
/// # Parameters
/// - `payload`: payload returned by the provider.
/// - `expected`: probe identifier selected before publication.
///
/// # Returns
/// `true` when the received representation and native type match.
#[must_use]
#[inline]
pub(super) fn payload_matches(payload: &TransportPayload, expected: &str) -> bool {
    matches!(
        (expected, payload),
        ("declared-native-publish", TransportPayload::Native(value)) if value.is::<u8>()
    ) || matches!(
        (expected, payload),
        ("declared-encoded-publish", TransportPayload::Encoded(_))
    )
}

/// Produces a conformance result for settlement capability consistency.
///
/// # Parameters
/// - `settlement`: settlement capability declared by the provider.
/// - `token`: token returned with the received delivery, if any.
/// - `settle`: operation that applies an idempotent accept decision.
///
/// # Returns
/// A pass, failure, or unsupported-capability skip case.
pub(super) fn settlement_case(
    settlement: SettlementCapabilities,
    token: Option<&SettlementToken>,
    settle: impl FnOnce(&SettlementToken) -> Result<(), SpiError>,
) -> ConformanceCase {
    match (settlement, token) {
        (SettlementCapabilities::None, None) => ConformanceCase::Skipped {
            case_id: "settlement-idempotence".into(),
            reason: ConformanceSkipReason::UnsupportedCapability {
                capability: "settlement",
            },
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

/// Converts a publish result into a named conformance case.
///
/// # Parameters
/// - `case_id`: stable identifier for the payload probe.
/// - `result`: provider publish result.
/// - `model`: sync/async label included in failure details.
///
/// # Returns
/// A passed or failed conformance case.
pub(super) fn publish_case(case_id: &str, result: Result<(), SpiError>, model: &str) -> ConformanceCase {
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

/// Runs an optional synchronous fixture and appends its outcome.
///
/// # Parameters
/// - `report`: report receiving the result.
/// - `case_id`: stable case identifier.
/// - `hook`: optional provider-specific check.
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
            reason: ConformanceSkipReason::MissingFixture {
                detail: "provider-specific hook was not supplied".into(),
            },
        },
    });
}
