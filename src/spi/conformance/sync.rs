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
use super::conformance_progress::ConformanceProgress;
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
use crate::spi::SettlementToken;
use crate::spi::ShutdownMode;
use crate::spi::ShutdownOutcome;

/// Runs common structural checks against synchronous provider instances.
///
/// The factory is called once for each payload-mode and durability
/// combination. Multiple report cases within a combination intentionally
/// share that provider and receiver; pending-close, foreign-token, and
/// Retry/Reject settlement probes use independent instances. The factory must
/// return independent provider state on each call; the runner cannot prevent
/// factories from sharing an underlying transport.
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
    run_sync_with_profile_and_observer(factory, hooks, ConformanceProfile::Structural, &|_| {})
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
    run_sync_with_profile_and_observer(factory, hooks, profile, &|_| {})
}

/// Runs synchronous checks with progress notifications before potentially
/// blocking provider operations.
///
/// The observer is called synchronously immediately before the reported
/// operation begins. Its callback should return quickly and flush any output
/// that must survive a process timeout. It is diagnostic only: it cannot
/// cancel an operation, and a blocked observer itself requires an external
/// process watchdog. `context` is `None` before the runner establishes a
/// payload/durability combination and is populated with that combination
/// afterward. Existing [`run_sync_with_profile`] and [`run_sync`] entry points
/// retain their behavior and run without an observer.
///
/// # Type Parameters
/// - `F`: factory that creates a synchronous provider instance.
///
/// # Parameters
/// - `factory`: creates independent synchronous provider instances.
/// - `hooks`: optional provider-specific checks and fixtures.
/// - `profile`: policy for missing provider fixtures.
/// - `observer`: callback receiving the pending case, optional context, and
///   stable operation stage.
///
/// # Returns
/// A report containing each conformance check that ran or was skipped.
///
/// # Panics
/// Panics if the provider factory, observer, hook, or an SPI method unwinds.
pub fn run_sync_with_profile_and_observer<F>(
    factory: F,
    hooks: &ConformanceHooks,
    profile: ConformanceProfile,
    observer: &dyn Fn(&ConformanceProgress),
) -> ConformanceReport
where
    F: Fn() -> Arc<dyn EventBusSpi>,
{
    let mut report = ConformanceReport::default();
    observer(&ConformanceProgress::new("capability-payload-mode", None, "factory"));
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
            record_pending_close(&factory, case_id, durability, &mut report, observer);
            let spi = if first_combination {
                first_combination = false;
                capability_spi.clone()
            } else {
                emit_progress(observer, case_id, case_id, durability, "factory");
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
                    emit_progress(observer, "shutdown", case_id, durability, "shutdown");
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
            observer(&ConformanceProgress::new(
                "subscribe",
                Some(ConformanceCaseContext::new("sync", payload_mode, durability)),
                "subscribe",
            ));
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
                    emit_progress(observer, "shutdown", case_id, durability, "shutdown");
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
            record_foreign_settlement(&factory, case_id, durability, &mut report, observer);
            let expected = probe_message(probe_payload(case_id), capabilities, &scope);
            observer(&ConformanceProgress::new(
                "receive-empty-zero-timeout",
                Some(ConformanceCaseContext::new("sync", payload_mode, durability)),
                "receive",
            ));
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
            observer(&ConformanceProgress::new(
                case_id,
                Some(ConformanceCaseContext::new("sync", payload_mode, durability)),
                "publish",
            ));
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
                observer(&ConformanceProgress::new(
                    "receive-payload",
                    Some(ConformanceCaseContext::new("sync", payload_mode, durability)),
                    "receive",
                ));
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
                            emit_progress(observer, "settlement-idempotence", case_id, durability, "settle");
                            subscription.settle(token, DeliveryDisposition::Accept)?;
                            emit_progress(observer, "settlement-idempotence", case_id, durability, "settle");
                            subscription.settle(token, DeliveryDisposition::Accept)
                        }));
                        report.push(match token.as_ref() {
                            Some(token) if capabilities.settlement() != SettlementCapabilities::None => {
                                emit_progress(
                                    observer,
                                    "settlement-conflicting-disposition",
                                    case_id,
                                    durability,
                                    "settle",
                                );
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
            record_retry_reject_settlement(&factory, case_id, durability, &mut report, observer);
            emit_progress(observer, "close", case_id, durability, "close");
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
                Ok(()) => {
                    emit_progress(observer, "receive-after-close", case_id, durability, "receive");
                    match subscription.receive(Duration::ZERO) {
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
                    }
                }
                Err(_) => ConformanceCase::Skipped {
                    case_id: "receive-after-close".into(),
                    reason: ConformanceSkipReason::MissingFixture {
                        detail: "receiver close failed".into(),
                    },
                },
            });
            emit_progress(observer, "close-idempotence", case_id, durability, "close");
            report.push(match subscription.close() {
                Ok(()) => ConformanceCase::Passed {
                    case_id: "close-idempotence".into(),
                },
                Err(error) => ConformanceCase::Failed {
                    case_id: "close-idempotence".into(),
                    detail: format!("repeated sync receiver close failed: {error}"),
                },
            });
            emit_progress(observer, "shutdown", case_id, durability, "shutdown");
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
            emit_progress(observer, "shutdown", case_id, durability, "shutdown");
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
        observer(&ConformanceProgress::new(
            "provider-settlement-idempotence",
            None,
            "hook",
        ));
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
        observer(&ConformanceProgress::new("ephemeral-cleanup", None, "hook"));
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
        observer(&ConformanceProgress::new("durable-unsettled-recovery", None, "hook"));
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

/// Records Retry and Reject settlement probes for supported providers.
///
/// # Type Parameters
/// - F: synchronous provider factory type.
///
/// # Parameters
/// - factory: factory creating independent provider instances.
/// - `payload_case_id`: stable ID of the payload probe.
/// - `durability`: subscription mode under test.
/// - `report`: report receiving probe outcomes.
fn record_retry_reject_settlement<F>(
    factory: &F,
    payload_case_id: &'static str,
    durability: SubscriptionDurability,
    report: &mut ConformanceReport,
    observer: &dyn Fn(&ConformanceProgress),
) where
    F: Fn() -> Arc<dyn EventBusSpi>,
{
    let cases = [
        (
            DeliveryDisposition::Retry,
            "retry",
            "settlement-retry-first",
            "settlement-retry-idempotence",
            "settlement-retry-conflict",
        ),
        (
            DeliveryDisposition::Reject,
            "reject",
            "settlement-reject-first",
            "settlement-reject-idempotence",
            "settlement-reject-conflict",
        ),
    ];
    for (disposition, name, first_id, repeat_id, conflict_id) in cases {
        emit_progress(
            observer,
            "settlement-retry-first",
            payload_case_id,
            durability,
            "factory",
        );
        let spi = factory();
        if spi.capabilities().settlement() != SettlementCapabilities::AcceptRetryReject {
            for id in [first_id, repeat_id, conflict_id] {
                report.push(ConformanceCase::Skipped {
                    case_id: id.into(),
                    reason: ConformanceSkipReason::UnsupportedCapability {
                        capability: "retry-reject-settlement",
                    },
                });
            }
            emit_progress(
                observer,
                "settlement-probe-shutdown",
                payload_case_id,
                durability,
                "shutdown",
            );
            if let Err(error) = spi.shutdown(ShutdownMode::Immediate) {
                report.push(ConformanceCase::Failed {
                    case_id: "settlement-probe-shutdown".into(),
                    detail: format!("sync shutdown after unsupported {name}: {error}"),
                });
            }
            continue;
        }
        let mut subscription = None;
        let outcome = (|| -> Result<SettlementToken, String> {
            let scope = ProbeScope::new().map_err(|e| format!("{name} identity: {e}"))?;
            emit_progress(observer, first_id, payload_case_id, durability, "subscribe");
            subscription = Some(
                spi.subscribe(probe_request(7, durability, &scope))
                    .map_err(|e| format!("{name} subscribe: {e}"))?,
            );
            let expected = probe_message(probe_payload(payload_case_id), spi.capabilities(), &scope);
            emit_progress(observer, first_id, payload_case_id, durability, "publish");
            let ack = spi
                .publish(probe_message(
                    probe_payload(payload_case_id),
                    spi.capabilities(),
                    &scope,
                ))
                .map_err(|e| format!("{name} publish: {e}"))?;
            validate_probe_acknowledgement(&ack, spi.capabilities().publish_visibility(), Id::new(7))?;
            emit_progress(observer, first_id, payload_case_id, durability, "receive");
            let mut message = match subscription
                .as_mut()
                .expect("subscription created")
                .receive(Duration::from_secs(1))
                .map_err(|e| format!("{name} receive: {e}"))?
            {
                ReceiveOutcome::Message(message) => message,
                _ => return Err(format!("{name} receive returned no message")),
            };
            if let Some(difference) = message_difference(&message, &expected) {
                return Err(format!("{name} received message differs at {difference}"));
            }
            message
                .take_settlement()
                .ok_or_else(|| format!("{name} receive omitted settlement token"))
        })();
        match outcome {
            Err(detail) => {
                report.push(ConformanceCase::Failed {
                    case_id: first_id.into(),
                    detail: detail.clone(),
                });
                report.push(ConformanceCase::Skipped {
                    case_id: repeat_id.into(),
                    reason: ConformanceSkipReason::MissingFixture {
                        detail: format!("{name} setup failed"),
                    },
                });
                report.push(ConformanceCase::Skipped {
                    case_id: conflict_id.into(),
                    reason: ConformanceSkipReason::MissingFixture {
                        detail: format!("{name} setup failed"),
                    },
                });
            }
            Ok(token) => {
                emit_progress(observer, first_id, payload_case_id, durability, "settle");
                match subscription
                    .as_mut()
                    .expect("subscription created")
                    .settle(&token, disposition)
                {
                    Ok(()) => report.push(ConformanceCase::Passed {
                        case_id: first_id.into(),
                    }),
                    Err(e) => report.push(ConformanceCase::Failed {
                        case_id: first_id.into(),
                        detail: format!("first {name} failed: {e}"),
                    }),
                }
                emit_progress(observer, repeat_id, payload_case_id, durability, "settle");
                match subscription
                    .as_mut()
                    .expect("subscription created")
                    .settle(&token, disposition)
                {
                    Ok(()) => report.push(ConformanceCase::Passed {
                        case_id: repeat_id.into(),
                    }),
                    Err(e) => report.push(ConformanceCase::Failed {
                        case_id: repeat_id.into(),
                        detail: format!("repeated {name} failed: {e}"),
                    }),
                }
                let conflicts = match disposition {
                    DeliveryDisposition::Retry => [DeliveryDisposition::Accept, DeliveryDisposition::Reject],
                    _ => [DeliveryDisposition::Accept, DeliveryDisposition::Retry],
                };
                let bad = conflicts.into_iter().find_map(|other| {
                    emit_progress(observer, conflict_id, payload_case_id, durability, "settle");
                    match subscription
                        .as_mut()
                        .expect("subscription created")
                        .settle(&token, other)
                    {
                        Err(e) if e.kind() == "invalid_settlement_token" => None,
                        Err(e) => Some(format!("conflicting {other:?} returned wrong error: {e}")),
                        Ok(()) => Some(format!("conflicting {other:?} was accepted")),
                    }
                });
                report.push(match bad {
                    Some(detail) => ConformanceCase::Failed {
                        case_id: conflict_id.into(),
                        detail,
                    },
                    None => ConformanceCase::Passed {
                        case_id: conflict_id.into(),
                    },
                });
            }
        }
        emit_progress(observer, "settlement-probe-close", payload_case_id, durability, "close");
        if let Some(subscription) = subscription.as_mut()
            && let Err(error) = subscription.close()
        {
            report.push(ConformanceCase::Failed {
                case_id: "settlement-probe-close".into(),
                detail: format!("sync {name} close failed: {error}"),
            });
        }
        emit_progress(
            observer,
            "settlement-probe-shutdown",
            payload_case_id,
            durability,
            "shutdown",
        );
        if let Err(error) = spi.shutdown(ShutdownMode::Immediate) {
            report.push(ConformanceCase::Failed {
                case_id: "settlement-probe-shutdown".into(),
                detail: format!("sync {name} probe shutdown failed: {error}"),
            });
        }
    }
}

/// Checks that a provider rejects settlement tokens owned by another
/// subscription.
///
/// # Type Parameters
/// - F: synchronous provider factory type.
///
/// # Parameters
/// - factory: factory creating independent provider instances.
/// - `case_id`: stable ID of the payload probe.
/// - `durability`: subscription mode under test.
/// - `report`: report receiving probe outcomes.
fn record_foreign_settlement<F>(
    factory: &F,
    case_id: &'static str,
    durability: SubscriptionDurability,
    report: &mut ConformanceReport,
    observer: &dyn Fn(&ConformanceProgress),
) where
    F: Fn() -> Arc<dyn EventBusSpi>,
{
    emit_progress(observer, "settlement-foreign-token", case_id, durability, "factory");
    let spi = factory();
    let capabilities = spi.capabilities();
    if capabilities.settlement() == SettlementCapabilities::None {
        for id in ["settlement-foreign-token", "settlement-owner-after-foreign-token"] {
            report.push(ConformanceCase::Skipped {
                case_id: id.into(),
                reason: ConformanceSkipReason::UnsupportedCapability {
                    capability: "settlement",
                },
            });
        }
        emit_progress(observer, "settlement-probe-shutdown", case_id, durability, "shutdown");
        if let Err(error) = spi.shutdown(ShutdownMode::Immediate) {
            report.push(ConformanceCase::Failed {
                case_id: "settlement-probe-shutdown".into(),
                detail: format!("shutdown after unsupported settlement: {error}"),
            });
        }
        return;
    }
    let mut owner = None;
    let mut foreign = None;
    let result = (|| -> Result<(), String> {
        let owner_scope = ProbeScope::new().map_err(|e| format!("identity generation A: {e}"))?;
        let foreign_scope = ProbeScope::new().map_err(|e| format!("identity generation B: {e}"))?;
        emit_progress(observer, "settlement-foreign-token", case_id, durability, "subscribe");
        owner = Some(
            spi.subscribe(probe_request(1, durability, &owner_scope))
                .map_err(|e| format!("subscribe A: {e}"))?,
        );
        emit_progress(observer, "settlement-foreign-token", case_id, durability, "subscribe");
        foreign = Some(
            spi.subscribe(probe_request(2, durability, &foreign_scope))
                .map_err(|e| format!("subscribe B: {e}"))?,
        );
        let expected = probe_message(probe_payload(case_id), capabilities, &owner_scope);
        emit_progress(observer, "settlement-foreign-token", case_id, durability, "publish");
        let ack = spi
            .publish(probe_message(probe_payload(case_id), capabilities, &owner_scope))
            .map_err(|e| format!("publish: {e}"))?;
        validate_probe_acknowledgement(&ack, capabilities.publish_visibility(), Id::new(1))?;
        emit_progress(observer, "settlement-foreign-token", case_id, durability, "receive");
        let mut message = match owner
            .as_mut()
            .expect("owner set")
            .receive(Duration::from_secs(1))
            .map_err(|e| format!("receive A: {e}"))?
        {
            ReceiveOutcome::Message(message) => message,
            _ => return Err("receive A returned no message".into()),
        };
        if let Some(difference) = message_difference(&message, &expected) {
            return Err(format!("receive A message differs at {difference}"));
        }
        let Some(token) = message.take_settlement() else {
            return Err("receive A omitted settlement token".into());
        };
        emit_progress(observer, "settlement-foreign-token", case_id, durability, "settle");
        match foreign
            .as_mut()
            .expect("foreign set")
            .settle(&token, DeliveryDisposition::Accept)
        {
            Err(error) if error.kind() == "invalid_settlement_token" => report.push(ConformanceCase::Passed {
                case_id: "settlement-foreign-token".into(),
            }),
            Err(error) => report.push(ConformanceCase::Failed {
                case_id: "settlement-foreign-token".into(),
                detail: format!("foreign settlement returned wrong error: {error}"),
            }),
            Ok(()) => report.push(ConformanceCase::Failed {
                case_id: "settlement-foreign-token".into(),
                detail: "foreign receiver accepted settlement token".into(),
            }),
        }
        emit_progress(
            observer,
            "settlement-owner-after-foreign-token",
            case_id,
            durability,
            "settle",
        );
        match owner
            .as_mut()
            .expect("owner set")
            .settle(&token, DeliveryDisposition::Accept)
        {
            Ok(()) => report.push(ConformanceCase::Passed {
                case_id: "settlement-owner-after-foreign-token".into(),
            }),
            Err(error) => report.push(ConformanceCase::Failed {
                case_id: "settlement-owner-after-foreign-token".into(),
                detail: format!("owner settlement failed after foreign attempt: {error}"),
            }),
        }
        Ok(())
    })();
    if let Err(detail) = result {
        for id in ["settlement-foreign-token", "settlement-owner-after-foreign-token"] {
            report.push(ConformanceCase::Failed {
                case_id: id.into(),
                detail: detail.clone(),
            });
        }
    }
    emit_progress(observer, "settlement-probe-close", case_id, durability, "close");
    if let Some(subscription) = foreign.as_mut()
        && let Err(e) = subscription.close()
    {
        report.push(ConformanceCase::Failed {
            case_id: "settlement-probe-close".into(),
            detail: format!("close B: {e}"),
        });
    }
    emit_progress(observer, "settlement-probe-close", case_id, durability, "close");
    if let Some(subscription) = owner.as_mut()
        && let Err(e) = subscription.close()
    {
        report.push(ConformanceCase::Failed {
            case_id: "settlement-probe-close".into(),
            detail: format!("close A: {e}"),
        });
    }
    emit_progress(observer, "settlement-probe-shutdown", case_id, durability, "shutdown");
    if let Err(e) = spi.shutdown(ShutdownMode::Immediate) {
        report.push(ConformanceCase::Failed {
            case_id: "settlement-probe-shutdown".into(),
            detail: format!("shutdown: {e}"),
        });
    }
}

/// Checks receiver behavior when close races with a pending receive.
///
/// # Type Parameters
/// - F: synchronous provider factory type.
///
/// # Parameters
/// - factory: factory creating independent provider instances.
/// - `case_id`: stable ID of the payload probe.
/// - `durability`: subscription mode under test.
/// - `report`: report receiving probe outcomes.
fn record_pending_close<F>(
    factory: &F,
    case_id: &'static str,
    durability: SubscriptionDurability,
    report: &mut ConformanceReport,
    observer: &dyn Fn(&ConformanceProgress),
) where
    F: Fn() -> Arc<dyn EventBusSpi>,
{
    emit_progress(observer, "close-with-pending", case_id, durability, "factory");
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
            emit_progress(observer, "shutdown", case_id, durability, "shutdown");
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
    emit_progress(observer, "close-with-pending-setup", case_id, durability, "subscribe");
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
            emit_progress(
                observer,
                "close-with-pending-setup-cleanup",
                case_id,
                durability,
                "shutdown",
            );
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
    emit_progress(observer, "close-with-pending-setup", case_id, durability, "publish");
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
        emit_progress(
            observer,
            "close-with-pending-setup-cleanup",
            case_id,
            durability,
            "close",
        );
        let close_result = subscription.close();
        emit_progress(
            observer,
            "close-with-pending-setup-cleanup",
            case_id,
            durability,
            "shutdown",
        );
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
        emit_progress(
            observer,
            "receive-after-close-with-pending",
            case_id,
            durability,
            "close",
        );
        report.push(match subscription.close() {
            // The following receive is separately announced below.
            Ok(()) => {
                emit_progress(
                    observer,
                    "receive-after-close-with-pending",
                    case_id,
                    durability,
                    "receive",
                );
                match subscription.receive(Duration::ZERO) {
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
                }
            }
            Err(error) => ConformanceCase::Failed {
                case_id: "receive-after-close-with-pending".into(),
                detail: format!("sync pending-close close failed: {error}"),
            },
        });
        emit_progress(observer, "close-with-pending-idempotent", case_id, durability, "close");
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
    emit_progress(observer, "close-with-pending-shutdown", case_id, durability, "shutdown");
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

/// Reports an operation that the synchronous runner is about to perform.
///
/// # Parameters
/// - `observer`: callback receiving the progress snapshot.
/// - `case_id`: stable identifier for the pending case.
/// - `context_case_id`: case identifier used to determine payload mode.
/// - `durability`: subscription durability associated with the case.
/// - `stage`: stable name of the pending operation.
fn emit_progress(
    observer: &dyn Fn(&ConformanceProgress),
    case_id: &'static str,
    context_case_id: &str,
    durability: SubscriptionDurability,
    stage: &'static str,
) {
    let payload_mode = if context_case_id.contains("native") {
        crate::spi::PayloadModes::Native
    } else {
        crate::spi::PayloadModes::Encoded
    };
    observer(&ConformanceProgress::new(
        case_id,
        Some(ConformanceCaseContext::new("sync", payload_mode, durability)),
        stage,
    ));
}
