// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Synchronous SPI conformance runner.

use std::sync::Arc;
use std::time::Duration;

use super::conformance_hooks::ConformanceHooks;
use super::conformance_profile::ConformanceProfile;
use super::conformance_report::ConformanceCase;
use super::conformance_report::ConformanceReport;
use super::conformance_report::payload_matches;
use super::conformance_report::payload_probes;
use super::conformance_report::probe_message;
use super::conformance_report::probe_request;
use super::conformance_report::publish_case;
use super::conformance_report::push_hook;
use super::conformance_report::settlement_case;
use super::conformance_skip_reason::ConformanceSkipReason;
use crate::model::SubscriptionDurability;
use crate::spi::DeliveryDisposition;
use crate::spi::EventBusSpi;
use crate::spi::ReceiveOutcome;
use crate::spi::SettlementCapabilities;
use crate::spi::ShutdownMode;
use crate::spi::ShutdownOutcome;

/// Runs common structural checks against a fresh synchronous provider.
///
/// The factory is called once for each case so one check cannot contaminate
/// the next by shutting down shared provider state.
///
/// # Type Parameters
/// - `F`: provider factory type.
///
/// # Parameters
/// - `factory`: creates a fresh synchronous provider instance.
/// - `hooks`: optional provider-specific checks and fixtures.
///
/// # Returns
/// Results for each conformance check that ran or was skipped.
///
/// # Panics
/// Panics if the provider factory or an SPI method unwinds.
#[must_use = "Inspect the conformance results."]
#[inline]
pub fn run_sync<F>(factory: F, hooks: &ConformanceHooks) -> ConformanceReport
where
    F: Fn() -> Arc<dyn EventBusSpi>,
{
    run_sync_with_profile(factory, hooks, ConformanceProfile::Structural)
}

/// Runs synchronous checks with explicit treatment of missing provider
/// fixtures.
///
/// # Type Parameters
/// - `F`: provider factory type.
///
/// # Parameters
/// - `factory`: creates a fresh synchronous provider instance.
/// - `hooks`: optional provider-specific checks and fixtures.
/// - `profile`: policy for missing required fixtures.
///
/// # Returns
/// Results for each conformance check that ran or was skipped.
///
/// # Panics
/// Panics if the provider factory or an SPI method unwinds.
#[must_use = "Inspect the conformance results."]
pub fn run_sync_with_profile<F>(factory: F, hooks: &ConformanceHooks, profile: ConformanceProfile) -> ConformanceReport
where
    F: Fn() -> Arc<dyn EventBusSpi>,
{
    let mut report = ConformanceReport::default();
    let capability_spi = factory();
    let payloads = payload_probes(capability_spi.capabilities().payload_modes());
    report.push(ConformanceCase::Passed {
        case_id: "capability-payload-mode".into(),
    });
    for (index, (case_id, payload)) in payloads.into_iter().enumerate() {
        let spi = if index == 0 { capability_spi.clone() } else { factory() };
        let capabilities = spi.capabilities();
        let mut subscription = match spi.subscribe(probe_request(1, capabilities.durability())) {
            Ok(subscription) => {
                report.push(ConformanceCase::Passed {
                    case_id: "subscribe".into(),
                });
                subscription
            }
            Err(error) => {
                report.push(ConformanceCase::Failed {
                    case_id: "subscribe".into(),
                    detail: format!("sync subscribe failed: {error}"),
                });
                report.push(ConformanceCase::Skipped {
                    case_id: "publish-receive".into(),
                    reason: ConformanceSkipReason::MissingFixture {
                        detail: "subscription could not be created".into(),
                    },
                });
                report.push(match spi.shutdown(ShutdownMode::Immediate) {
                    Ok(ShutdownOutcome::Complete) => ConformanceCase::Passed {
                        case_id: "shutdown".into(),
                    },
                    Ok(outcome) => ConformanceCase::Failed {
                        case_id: "shutdown".into(),
                        detail: format!("immediate shutdown returned {outcome:?}"),
                    },
                    Err(error) => ConformanceCase::Failed {
                        case_id: "shutdown".into(),
                        detail: format!("sync shutdown failed: {error}"),
                    },
                });
                continue;
            }
        };
        let result = spi.publish(probe_message(payload));
        let published = result.is_ok();
        report.push(publish_case(case_id, result.map(|_| ()), "sync"));
        if published {
            match subscription.receive(Duration::from_secs(1)) {
                Ok(ReceiveOutcome::Message(mut message)) => {
                    let payload_matches = payload_matches(message.payload(), case_id);
                    report.push(if payload_matches {
                        ConformanceCase::Passed {
                            case_id: "receive-payload".into(),
                        }
                    } else {
                        ConformanceCase::Failed {
                            case_id: "receive-payload".into(),
                            detail: "received payload mode or type does not match the declaration".into(),
                        }
                    });
                    let token = message.take_settlement();
                    report.push(settlement_case(capabilities.settlement(), token.as_ref(), |token| {
                        subscription.settle(token, DeliveryDisposition::Accept)?;
                        subscription.settle(token, DeliveryDisposition::Accept)
                    }));
                    report.push(match token.as_ref() {
                        Some(token) if capabilities.settlement() != SettlementCapabilities::None => {
                            match subscription.settle(token, DeliveryDisposition::Reject) {
                                Err(error) if error.kind() == "invalid_settlement_token" => ConformanceCase::Passed {
                                    case_id: "settlement-conflicting-disposition".into(),
                                },
                                Err(error) => ConformanceCase::Failed {
                                    case_id: "settlement-conflicting-disposition".into(),
                                    detail: format!("conflicting settlement returned the wrong error: {error}"),
                                },
                                Ok(()) => ConformanceCase::Failed {
                                    case_id: "settlement-conflicting-disposition".into(),
                                    detail: "provider accepted a conflicting terminal disposition".into(),
                                },
                            }
                        }
                        _ => ConformanceCase::Skipped {
                            case_id: "settlement-conflicting-disposition".into(),
                            reason: ConformanceSkipReason::UnsupportedCapability {
                                capability: "settlement",
                            },
                        },
                    });
                }
                Ok(_) => report.push(ConformanceCase::Failed {
                    case_id: "receive-payload".into(),
                    detail: "publish was not followed by a message".into(),
                }),
                Err(error) => report.push(ConformanceCase::Failed {
                    case_id: "receive-payload".into(),
                    detail: format!("sync receive failed: {error}"),
                }),
            }
        } else {
            report.push(ConformanceCase::Skipped {
                case_id: "receive-payload".into(),
                reason: ConformanceSkipReason::MissingFixture {
                    detail: "publish failed".into(),
                },
            });
        }
        report.push(match subscription.close() {
            Ok(()) => ConformanceCase::Passed {
                case_id: "close".into(),
            },
            Err(error) => ConformanceCase::Failed {
                case_id: "close".into(),
                detail: format!("sync receiver close failed: {error}"),
            },
        });
        report.push(match subscription.close() {
            Ok(()) => ConformanceCase::Passed {
                case_id: "close-idempotence".into(),
            },
            Err(error) => ConformanceCase::Failed {
                case_id: "close-idempotence".into(),
                detail: format!("repeated sync receiver close failed: {error}"),
            },
        });
        report.push(match spi.shutdown(ShutdownMode::Immediate) {
            Ok(ShutdownOutcome::Complete) => ConformanceCase::Passed {
                case_id: "shutdown".into(),
            },
            Ok(outcome) => ConformanceCase::Failed {
                case_id: "shutdown".into(),
                detail: format!("immediate shutdown returned {outcome:?}"),
            },
            Err(error) => ConformanceCase::Failed {
                case_id: "shutdown".into(),
                detail: format!("sync shutdown failed: {error}"),
            },
        });
        report.push(match spi.shutdown(ShutdownMode::Immediate) {
            Ok(ShutdownOutcome::Complete) => ConformanceCase::Passed {
                case_id: "shutdown-idempotence".into(),
            },
            Ok(outcome) => ConformanceCase::Failed {
                case_id: "shutdown-idempotence".into(),
                detail: format!("repeated immediate shutdown returned {outcome:?}"),
            },
            Err(error) => ConformanceCase::Failed {
                case_id: "shutdown-idempotence".into(),
                detail: format!("repeated sync shutdown failed: {error}"),
            },
        });
    }
    let capabilities = capability_spi.capabilities();
    if capabilities.settlement() == SettlementCapabilities::None {
        report.push(ConformanceCase::Skipped {
            case_id: "provider-settlement-idempotence".into(),
            reason: ConformanceSkipReason::UnsupportedCapability {
                capability: "settlement",
            },
        });
    } else {
        push_hook(
            &mut report,
            "provider-settlement-idempotence",
            hooks.settlement.as_ref(),
        );
    }
    report.not_applicable("receive-cancellation", "synchronous receive has no cancellable future");
    report.not_applicable(
        "settlement-cancellation",
        "synchronous settlement has no cancellable future",
    );
    report.not_applicable("close-cancellation", "synchronous close has no cancellable future");
    report.not_applicable(
        "shutdown-cancellation",
        "synchronous shutdown has no cancellable future",
    );
    if capabilities
        .subscription_modes()
        .supports(SubscriptionDurability::Ephemeral)
    {
        push_hook(&mut report, "ephemeral-cleanup", hooks.ephemeral_cleanup.as_ref());
    } else {
        report.push(ConformanceCase::Skipped {
            case_id: "ephemeral-cleanup".into(),
            reason: ConformanceSkipReason::UnsupportedCapability {
                capability: "ephemeral-subscription",
            },
        });
    }
    if capabilities
        .subscription_modes()
        .supports(SubscriptionDurability::Durable)
    {
        push_hook(
            &mut report,
            "durable-unsettled-recovery",
            hooks.durable_recovery.as_ref(),
        );
    } else {
        report.push(ConformanceCase::Skipped {
            case_id: "durable-unsettled-recovery".into(),
            reason: ConformanceSkipReason::UnsupportedCapability {
                capability: "durable-subscription",
            },
        });
    }
    report.apply_profile(profile);
    report
}
