// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Asynchronous SPI conformance runner.

use std::sync::Arc;
use std::time::Duration;

use super::async_conformance_hooks::AsyncConformanceHooks;
use super::conformance_profile::ConformanceProfile;
use super::conformance_report::ConformanceCase;
use super::conformance_report::ConformanceReport;
use super::conformance_report::payload_matches;
use super::conformance_report::payload_probes;
use super::conformance_report::probe_message;
use super::conformance_report::probe_request;
use super::conformance_report::publish_case;
use crate::model::SubscriptionDurability;
use crate::spi::AsyncEventBusSpi;
use crate::spi::DeliveryDisposition;
use crate::spi::ReceiveOutcome;
use crate::spi::SettlementCapabilities;
use crate::spi::ShutdownMode;
use crate::spi::ShutdownOutcome;

/// Runs common structural checks against a fresh asynchronous provider.
/// The factory is called again for each payload mode so one case cannot
/// contaminate later cases through shared provider state.
///
/// # Type Parameters
/// - `F`: asynchronous provider factory type.
/// - `Fut`: future returned by the provider factory.
///
/// # Parameters
/// - `factory`: asynchronously creates a fresh provider instance.
/// - `hooks`: optional provider-specific checks and fixtures.
///
/// # Returns
/// A report containing each conformance check result.
///
/// # Panics
/// Panics if the provider factory or an SPI method unwinds.
pub async fn run_async<F, Fut>(factory: F, hooks: &AsyncConformanceHooks) -> ConformanceReport
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Arc<dyn AsyncEventBusSpi>>,
{
    run_async_with_profile(factory, hooks, ConformanceProfile::Structural).await
}

/// Runs asynchronous checks with explicit treatment of missing provider
/// fixtures.
///
/// # Type Parameters
/// - `F`: asynchronous provider factory type.
/// - `Fut`: future returned by the provider factory.
///
/// # Parameters
/// - `factory`: asynchronously creates a fresh provider instance.
/// - `hooks`: optional provider-specific checks and fixtures.
/// - `profile`: policy for missing required fixtures.
///
/// # Returns
/// A report containing each conformance check result.
///
/// # Panics
/// Panics if the provider factory or an SPI method unwinds.
pub async fn run_async_with_profile<F, Fut>(
    factory: F,
    hooks: &AsyncConformanceHooks,
    profile: ConformanceProfile,
) -> ConformanceReport
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Arc<dyn AsyncEventBusSpi>>,
{
    let mut report = ConformanceReport::default();
    let capability_spi = factory().await;
    let payloads = payload_probes(capability_spi.capabilities().payload_modes());
    report.push(ConformanceCase::Passed {
        case_id: "capability-payload-mode".into(),
    });
    for (index, (case_id, payload)) in payloads.into_iter().enumerate() {
        let spi = if index == 0 {
            capability_spi.clone()
        } else {
            factory().await
        };
        let capabilities = spi.capabilities();
        let mut subscription = match spi.subscribe(probe_request(1, capabilities.durability())).await {
            Ok(subscription) => {
                report.push(ConformanceCase::Passed {
                    case_id: "subscribe".into(),
                });
                subscription
            }
            Err(error) => {
                report.push(ConformanceCase::Failed {
                    case_id: "subscribe".into(),
                    detail: format!("async subscribe failed: {error}"),
                });
                report.push(ConformanceCase::Skipped {
                    case_id: "publish-receive".into(),
                    reason: super::conformance_skip_reason::ConformanceSkipReason::MissingFixture {
                        detail: "subscription could not be created".into(),
                    },
                });
                report.push(
                    shutdown_case(
                        spi.as_ref(),
                        "shutdown",
                        "immediate shutdown returned",
                        "async shutdown failed",
                    )
                    .await,
                );
                continue;
            }
        };
        let result = spi.publish(probe_message(payload)).await;
        let published = result.is_ok();
        report.push(publish_case(case_id, result.map(|_| ()), "async"));
        if published {
            match subscription.receive(Duration::from_secs(1)).await {
                Ok(ReceiveOutcome::Message(mut message)) => {
                    report.push(if payload_matches(message.payload(), case_id) {
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
                    let case = match (capabilities.settlement(), token.as_ref()) {
                        (SettlementCapabilities::None, None) => ConformanceCase::Skipped {
                            case_id: "settlement-idempotence".into(),
                            reason: super::conformance_skip_reason::ConformanceSkipReason::UnsupportedCapability {
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
                        (_, Some(token)) => match subscription.settle(token, DeliveryDisposition::Accept).await {
                            Ok(()) => match subscription.settle(token, DeliveryDisposition::Accept).await {
                                Ok(()) => ConformanceCase::Passed {
                                    case_id: "settlement-idempotence".into(),
                                },
                                Err(error) => ConformanceCase::Failed {
                                    case_id: "settlement-idempotence".into(),
                                    detail: format!("repeated accept settlement failed: {error}"),
                                },
                            },
                            Err(error) => ConformanceCase::Failed {
                                case_id: "settlement-idempotence".into(),
                                detail: format!("accept settlement failed: {error}"),
                            },
                        },
                    };
                    report.push(case);
                    report.push(match token.as_ref() {
                        Some(token) if capabilities.settlement() != SettlementCapabilities::None => {
                            match subscription.settle(token, DeliveryDisposition::Reject).await {
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
                            reason: super::conformance_skip_reason::ConformanceSkipReason::UnsupportedCapability {
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
                    detail: format!("async receive failed: {error}"),
                }),
            }
        } else {
            report.push(ConformanceCase::Skipped {
                case_id: "receive-payload".into(),
                reason: super::conformance_skip_reason::ConformanceSkipReason::MissingFixture {
                    detail: "publish failed".into(),
                },
            });
        }
        report.push(match subscription.close().await {
            Ok(()) => ConformanceCase::Passed {
                case_id: "close".into(),
            },
            Err(error) => ConformanceCase::Failed {
                case_id: "close".into(),
                detail: format!("async receiver close failed: {error}"),
            },
        });
        report.push(match subscription.close().await {
            Ok(()) => ConformanceCase::Passed {
                case_id: "close-idempotence".into(),
            },
            Err(error) => ConformanceCase::Failed {
                case_id: "close-idempotence".into(),
                detail: format!("repeated async receiver close failed: {error}"),
            },
        });
        report.push(
            shutdown_case(
                spi.as_ref(),
                "shutdown",
                "immediate shutdown returned",
                "async shutdown failed",
            )
            .await,
        );
        report.push(
            shutdown_case(
                spi.as_ref(),
                "shutdown-idempotence",
                "repeated immediate shutdown returned",
                "repeated async shutdown failed",
            )
            .await,
        );
    }
    let capabilities = capability_spi.capabilities();
    if capabilities.settlement() == SettlementCapabilities::None {
        report.push(ConformanceCase::Skipped {
            case_id: "provider-settlement-idempotence".into(),
            reason: super::conformance_skip_reason::ConformanceSkipReason::UnsupportedCapability {
                capability: "settlement",
            },
        });
    } else {
        push_async_hook(
            &mut report,
            "provider-settlement-idempotence",
            hooks.settlement.as_ref(),
        )
        .await;
    }
    push_async_hook(&mut report, "receive-cancellation", hooks.receive_cancellation.as_ref()).await;
    if capabilities.settlement() == SettlementCapabilities::None {
        report.push(ConformanceCase::Skipped {
            case_id: "settlement-cancellation".into(),
            reason: super::conformance_skip_reason::ConformanceSkipReason::UnsupportedCapability {
                capability: "settlement",
            },
        });
    } else {
        push_async_hook(
            &mut report,
            "settlement-cancellation",
            hooks.settlement_cancellation.as_ref(),
        )
        .await;
    }
    push_async_hook(&mut report, "close-cancellation", hooks.close_cancellation.as_ref()).await;
    push_async_hook(
        &mut report,
        "shutdown-cancellation",
        hooks.shutdown_cancellation.as_ref(),
    )
    .await;
    if capabilities
        .subscription_modes()
        .supports(SubscriptionDurability::Ephemeral)
    {
        push_async_hook(&mut report, "ephemeral-cleanup", hooks.ephemeral_cleanup.as_ref()).await;
    } else {
        report.push(ConformanceCase::Skipped {
            case_id: "ephemeral-cleanup".into(),
            reason: super::conformance_skip_reason::ConformanceSkipReason::UnsupportedCapability {
                capability: "ephemeral-subscription",
            },
        });
    }
    if capabilities
        .subscription_modes()
        .supports(SubscriptionDurability::Durable)
    {
        push_async_hook(
            &mut report,
            "durable-unsettled-recovery",
            hooks.durable_recovery.as_ref(),
        )
        .await;
    } else {
        report.push(ConformanceCase::Skipped {
            case_id: "durable-unsettled-recovery".into(),
            reason: super::conformance_skip_reason::ConformanceSkipReason::UnsupportedCapability {
                capability: "durable-subscription",
            },
        });
    }
    report.apply_profile(profile);
    report
}

/// Converts an immediate shutdown result into its conformance case.
///
/// # Parameters
/// - `spi`: Provider instance to shut down.
/// - `case_id`: Stable identifier for this shutdown check.
/// - `outcome_prefix`: Detail prefix for a non-complete shutdown outcome.
/// - `error_prefix`: Detail prefix for a shutdown error.
///
/// # Returns
/// A passed or failed case describing the shutdown result.
async fn shutdown_case(
    spi: &dyn AsyncEventBusSpi,
    case_id: &str,
    outcome_prefix: &str,
    error_prefix: &str,
) -> ConformanceCase {
    match spi.shutdown(ShutdownMode::Immediate).await {
        Ok(ShutdownOutcome::Complete) => ConformanceCase::Passed {
            case_id: case_id.into(),
        },
        Ok(outcome) => ConformanceCase::Failed {
            case_id: case_id.into(),
            detail: format!("{outcome_prefix} {outcome:?}"),
        },
        Err(error) => ConformanceCase::Failed {
            case_id: case_id.into(),
            detail: format!("{error_prefix}: {error}"),
        },
    }
}

/// Runs an optional provider-specific asynchronous check and records its
/// result.
///
/// # Parameters
/// - `report`: report receiving the result.
/// - `case_id`: stable case identifier.
/// - `hook`: optional provider-specific check.
async fn push_async_hook(
    report: &mut ConformanceReport,
    case_id: &str,
    hook: Option<&super::async_conformance_hooks::AsyncConformanceCheck>,
) {
    report.push(match hook {
        Some(check) => match check().await {
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
            reason: super::conformance_skip_reason::ConformanceSkipReason::MissingFixture {
                detail: "provider-specific async hook was not supplied".into(),
            },
        },
    });
}
