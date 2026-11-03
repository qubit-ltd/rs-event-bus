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

use qubit_id::Id;

use super::conformance_case_context::ConformanceCaseContext;
use super::conformance_hooks::ConformanceHooks;
use super::conformance_profile::ConformanceProfile;
use super::conformance_report::ConformanceCase;
use super::conformance_report::ConformanceReport;
use super::conformance_report::message_difference;
use super::conformance_report::payload_probes;
use super::conformance_report::probe_message;
use super::conformance_report::probe_payload;
use super::conformance_report::probe_request;
use super::conformance_report::probe_subscription_modes;
use super::conformance_report::publish_case;
use super::conformance_report::push_hook;
use super::conformance_report::settlement_case;
use super::conformance_report::validate_probe_acknowledgement;
use super::conformance_skip_reason::ConformanceSkipReason;
use super::internal::ProbeScope;
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
    let modes = probe_subscription_modes(capability_spi.capabilities());
    let mut first_combination = true;
    for (case_id, _) in payloads {
        for durability in modes.iter().copied() {
            let payload_mode = if case_id == "declared-native-publish" {
                crate::spi::PayloadModes::Native
            } else {
                crate::spi::PayloadModes::Encoded
            };
            report.set_case_context(Some(ConformanceCaseContext::new("sync", payload_mode, durability)));
            record_pending_close(&factory, case_id, durability, &mut report);
            let spi = if first_combination {
                first_combination = false;
                capability_spi.clone()
            } else {
                factory()
            };
            let capabilities = spi.capabilities();
            let scope = match ProbeScope::new() {
                Ok(scope) => scope,
                Err(error) => {
                    report.push(ConformanceCase::Failed {
                        case_id: "probe-scope".into(),
                        detail: format!("sync probe identity generation failed: {error}"),
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
                            detail: format!("sync shutdown failed after probe identity generation: {error}"),
                        },
                    });
                    continue;
                }
            };
            let mode_name = match durability {
                SubscriptionDurability::Ephemeral => "ephemeral",
                SubscriptionDurability::Durable => "durable",
            };
            let mut subscription = match spi.subscribe(probe_request(1, durability, &scope)) {
                Ok(subscription) => {
                    report.push(ConformanceCase::Passed {
                        case_id: "subscribe".into(),
                    });
                    subscription
                }
                Err(error) => {
                    report.push(ConformanceCase::Failed {
                        case_id: "subscribe".into(),
                        detail: format!("sync subscribe failed for {mode_name} mode: {error}"),
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
            let expected = probe_message(probe_payload(case_id), capabilities, &scope);
            report.push(match subscription.receive(Duration::ZERO) {
                Ok(ReceiveOutcome::TimedOut) => ConformanceCase::Passed {
                    case_id: "receive-empty-zero-timeout".into(),
                },
                Ok(ReceiveOutcome::Message(_)) => ConformanceCase::Failed {
                    case_id: "receive-empty-zero-timeout".into(),
                    detail: format!("sync {mode_name} mode: empty receive returned Message"),
                },
                Ok(ReceiveOutcome::Gap(_)) => ConformanceCase::Failed {
                    case_id: "receive-empty-zero-timeout".into(),
                    detail: format!("sync {mode_name} mode: empty receive returned Gap"),
                },
                Ok(ReceiveOutcome::Closed) => ConformanceCase::Failed {
                    case_id: "receive-empty-zero-timeout".into(),
                    detail: format!("sync {mode_name} mode: empty receive returned Closed"),
                },
                Err(error) => ConformanceCase::Failed {
                    case_id: "receive-empty-zero-timeout".into(),
                    detail: format!("sync {mode_name} mode: empty receive failed: {error}"),
                },
            });
            let result = spi.publish(probe_message(probe_payload(case_id), capabilities, &scope));
            let published = result.is_ok();
            report.push(publish_case(
                case_id,
                result,
                "sync",
                capabilities.publish_visibility(),
                qubit_id::Id::new(1),
                durability,
            ));
            if published {
                match subscription.receive(Duration::from_secs(1)) {
                    Ok(ReceiveOutcome::Message(mut message)) => {
                        let difference = message_difference(&message, &expected);
                        report.push(if difference.is_none() {
                            ConformanceCase::Passed {
                                case_id: "receive-payload".into(),
                            }
                        } else {
                            ConformanceCase::Failed {
                                case_id: "receive-payload".into(),
                                detail: format!(
                                    "{mode_name} mode: received message differs at {}",
                                    difference.unwrap_or("unknown field")
                                ),
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
                                    Err(error) if error.kind() == "invalid_settlement_token" => {
                                        ConformanceCase::Passed {
                                            case_id: "settlement-conflicting-disposition".into(),
                                        }
                                    }
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
                        detail: format!("{mode_name} mode: publish was not followed by a message"),
                    }),
                    Err(error) => report.push(ConformanceCase::Failed {
                        case_id: "receive-payload".into(),
                        detail: format!("{mode_name} mode: sync receive failed: {error}"),
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
            let close_result = subscription.close();
            report.push(match &close_result {
                Ok(()) => ConformanceCase::Passed {
                    case_id: "close".into(),
                },
                Err(error) => ConformanceCase::Failed {
                    case_id: "close".into(),
                    detail: format!("sync receiver close failed: {error}"),
                },
            });
            report.push(match close_result {
                Ok(()) => match subscription.receive(Duration::ZERO) {
                    Ok(ReceiveOutcome::Closed) => ConformanceCase::Passed {
                        case_id: "receive-after-close".into(),
                    },
                    Ok(ReceiveOutcome::TimedOut) => ConformanceCase::Failed {
                        case_id: "receive-after-close".into(),
                        detail: format!("sync {mode_name} mode: receive after close returned TimedOut"),
                    },
                    Ok(ReceiveOutcome::Message(_)) => ConformanceCase::Failed {
                        case_id: "receive-after-close".into(),
                        detail: format!("sync {mode_name} mode: receive after close returned Message"),
                    },
                    Ok(ReceiveOutcome::Gap(_)) => ConformanceCase::Failed {
                        case_id: "receive-after-close".into(),
                        detail: format!("sync {mode_name} mode: receive after close returned Gap"),
                    },
                    Err(error) => ConformanceCase::Failed {
                        case_id: "receive-after-close".into(),
                        detail: format!("sync {mode_name} mode: receive after close failed: {error}"),
                    },
                },
                Err(_) => ConformanceCase::Skipped {
                    case_id: "receive-after-close".into(),
                    reason: ConformanceSkipReason::MissingFixture {
                        detail: "receiver close failed".into(),
                    },
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
    }
    report.set_case_context(None);
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

fn record_pending_close<F>(
    factory: &F,
    case_id: &'static str,
    durability: SubscriptionDurability,
    report: &mut ConformanceReport,
) where
    F: Fn() -> Arc<dyn EventBusSpi>,
{
    let spi = factory();
    let capabilities = spi.capabilities();
    let scope = match ProbeScope::new() {
        Ok(scope) => scope,
        Err(error) => {
            report.push(ConformanceCase::Failed {
                case_id: "close-with-pending-setup".into(),
                detail: format!("sync pending-close probe identity generation failed: {error}"),
            });
            report.push(ConformanceCase::Skipped {
                case_id: "receive-after-close-with-pending".into(),
                reason: ConformanceSkipReason::MissingFixture {
                    detail: "probe identity could not be generated".into(),
                },
            });
            report.push(ConformanceCase::Skipped {
                case_id: "close-with-pending-idempotent".into(),
                reason: ConformanceSkipReason::MissingFixture {
                    detail: "probe identity could not be generated".into(),
                },
            });
            report.push(match spi.shutdown(ShutdownMode::Immediate) {
                Ok(_) => ConformanceCase::Passed {
                    case_id: "close-with-pending-setup-cleanup".into(),
                },
                Err(error) => ConformanceCase::Failed {
                    case_id: "close-with-pending-setup-cleanup".into(),
                    detail: format!("sync pending-close cleanup after identity failure failed: {error}"),
                },
            });
            return;
        }
    };
    let mut subscription = match spi.subscribe(probe_request(1, durability, &scope)) {
        Ok(subscription) => subscription,
        Err(error) => {
            report.push(ConformanceCase::Failed {
                case_id: "close-with-pending-setup".into(),
                detail: format!("sync pending-close subscribe failed: {error}"),
            });
            report.push(ConformanceCase::Skipped {
                case_id: "receive-after-close-with-pending".into(),
                reason: ConformanceSkipReason::MissingFixture {
                    detail: "subscription could not be created".into(),
                },
            });
            report.push(ConformanceCase::Skipped {
                case_id: "close-with-pending-idempotent".into(),
                reason: ConformanceSkipReason::MissingFixture {
                    detail: "subscription could not be created".into(),
                },
            });
            let shutdown_result = spi.shutdown(ShutdownMode::Immediate);
            report.push(match shutdown_result {
                Ok(_) => ConformanceCase::Passed {
                    case_id: "close-with-pending-setup-cleanup".into(),
                },
                Err(error) => ConformanceCase::Failed {
                    case_id: "close-with-pending-setup-cleanup".into(),
                    detail: format!("sync pending-close SPI shutdown failed after setup failure: {error}"),
                },
            });
            return;
        }
    };
    let publish = spi.publish(probe_message(probe_payload(case_id), capabilities, &scope));
    let setup = match publish {
        Ok(acknowledgement) => {
            validate_probe_acknowledgement(&acknowledgement, capabilities.publish_visibility(), Id::new(1))
        }
        Err(error) => Err(error.to_string()),
    };
    if let Err(error) = setup {
        report.push(ConformanceCase::Failed {
            case_id: "close-with-pending-setup".into(),
            detail: format!("sync pending-close probe setup failed: {error}"),
        });
        report.push(ConformanceCase::Skipped {
            case_id: "receive-after-close-with-pending".into(),
            reason: ConformanceSkipReason::MissingFixture {
                detail: "pending-close probe publish failed".into(),
            },
        });
        let close_result = subscription.close();
        let shutdown_result = spi.shutdown(ShutdownMode::Immediate);
        let cleanup_errors = [
            close_result
                .err()
                .map(|error| format!("receiver close failed: {error}")),
            shutdown_result
                .err()
                .map(|error| format!("SPI shutdown failed: {error}")),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        report.push(if cleanup_errors.is_empty() {
            ConformanceCase::Passed {
                case_id: "close-with-pending-setup-cleanup".into(),
            }
        } else {
            ConformanceCase::Failed {
                case_id: "close-with-pending-setup-cleanup".into(),
                detail: format!("sync pending-close setup cleanup errors: {}", cleanup_errors.join("; ")),
            }
        });
        report.push(ConformanceCase::Skipped {
            case_id: "close-with-pending-idempotent".into(),
            reason: ConformanceSkipReason::MissingFixture {
                detail: "pending-close probe publish failed".into(),
            },
        });
    } else {
        report.push(ConformanceCase::Passed {
            case_id: "close-with-pending-setup".into(),
        });
        report.push(match subscription.close() {
            Ok(()) => match subscription.receive(Duration::ZERO) {
                Ok(ReceiveOutcome::Closed) => ConformanceCase::Passed {
                    case_id: "receive-after-close-with-pending".into(),
                },
                Ok(_) => ConformanceCase::Failed {
                    case_id: "receive-after-close-with-pending".into(),
                    detail: "sync close with pending probe returned a non-Closed outcome".into(),
                },
                Err(error) => ConformanceCase::Failed {
                    case_id: "receive-after-close-with-pending".into(),
                    detail: format!("sync receive after pending close failed: {error}"),
                },
            },
            Err(error) => ConformanceCase::Failed {
                case_id: "receive-after-close-with-pending".into(),
                detail: format!("sync pending-close close failed: {error}"),
            },
        });
        report.push(match subscription.close() {
            Ok(()) => ConformanceCase::Passed {
                case_id: "close-with-pending-idempotent".into(),
            },
            Err(error) => ConformanceCase::Failed {
                case_id: "close-with-pending-idempotent".into(),
                detail: format!("repeated sync pending-close failed: {error}"),
            },
        });
    }
    report.push(match spi.shutdown(ShutdownMode::Immediate) {
        Ok(ShutdownOutcome::Complete) => ConformanceCase::Passed {
            case_id: "close-with-pending-shutdown".into(),
        },
        Ok(outcome) => ConformanceCase::Failed {
            case_id: "close-with-pending-shutdown".into(),
            detail: format!("sync pending-close shutdown returned {outcome:?}"),
        },
        Err(error) => ConformanceCase::Failed {
            case_id: "close-with-pending-shutdown".into(),
            detail: format!("sync pending-close shutdown failed: {error}"),
        },
    });
}
