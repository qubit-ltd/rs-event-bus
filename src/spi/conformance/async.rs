// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Asynchronous SPI conformance runner.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use super::async_conformance_hooks::AsyncConformanceCheck;
use super::async_conformance_hooks::AsyncConformanceHooks;
use super::conformance_profile::ConformanceProfile;
use super::conformance_report::ConformanceCase;
use super::conformance_report::ConformanceReport;
use super::conformance_report::payload_matches;
use super::conformance_report::payload_probes;
use super::conformance_report::probe_message;
use super::conformance_report::probe_request;
use super::conformance_report::publish_case;
use super::conformance_skip_reason::ConformanceSkipReason;
use crate::model::SubscriptionDurability;
use crate::spi::AsyncEventBusSpi;
use crate::spi::AsyncEventSubscriptionSpi;
use crate::spi::DeliveryDisposition;
use crate::spi::ReceiveOutcome;
use crate::spi::SettlementCapabilities;
use crate::spi::SettlementToken;
use crate::spi::ShutdownMode;
use crate::spi::ShutdownOutcome;
use crate::spi::TransportPayload;

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
    Fut: Future<Output = Arc<dyn AsyncEventBusSpi>>,
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
    Fut: Future<Output = Arc<dyn AsyncEventBusSpi>>,
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
        run_payload_case(spi, case_id, payload, &mut report).await;
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
        push_async_hook(
            &mut report,
            "provider-settlement-idempotence",
            hooks.settlement.as_ref(),
        )
        .await;
    }
    push_async_hook(
        &mut report,
        "receive-cancellation",
        hooks.receive_cancellation.as_ref(),
    )
    .await;
    if capabilities.settlement() == SettlementCapabilities::None {
        report.push(ConformanceCase::Skipped {
            case_id: "settlement-cancellation".into(),
            reason: ConformanceSkipReason::UnsupportedCapability {
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
    push_async_hook(
        &mut report,
        "close-cancellation",
        hooks.close_cancellation.as_ref(),
    )
    .await;
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
        push_async_hook(
            &mut report,
            "ephemeral-cleanup",
            hooks.ephemeral_cleanup.as_ref(),
        )
        .await;
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
        push_async_hook(
            &mut report,
            "durable-unsettled-recovery",
            hooks.durable_recovery.as_ref(),
        )
        .await;
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

/// Runs publish, receive, settlement, close, and shutdown checks for one
/// payload.
///
/// # Parameters
/// - spi: fresh provider instance used only for this payload case.
/// - case_id: identifier of the payload probe being exercised.
/// - payload: transport representation sent through the provider.
/// - report: report receiving the ordered case results.
async fn run_payload_case(
    spi: Arc<dyn AsyncEventBusSpi>,
    case_id: &'static str,
    payload: TransportPayload,
    report: &mut ConformanceReport,
) {
    let capabilities = spi.capabilities();
    let mut subscription = match spi
        .subscribe(probe_request(1, capabilities.durability()))
        .await
    {
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
                reason: ConformanceSkipReason::MissingFixture {
                    detail: "subscription could not be created".into(),
                },
            });
            record_async_shutdown(
                report,
                spi.as_ref(),
                "shutdown",
                "immediate shutdown",
                "async shutdown",
            )
            .await;
            return;
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
                        detail: "received payload mode or type does not match the declaration"
                            .into(),
                    }
                });
                let token = message.take_settlement();
                report.push(
                    async_settlement_idempotence_case(
                        subscription.as_mut(),
                        capabilities.settlement(),
                        token.as_ref(),
                    )
                    .await,
                );
                report.push(
                    async_conflicting_settlement_case(
                        subscription.as_mut(),
                        capabilities.settlement(),
                        token.as_ref(),
                    )
                    .await,
                );
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
            reason: ConformanceSkipReason::MissingFixture {
                detail: "publish failed".into(),
            },
        });
    }

    record_async_subscription_close(
        report,
        subscription.as_mut(),
        "close",
        "async receiver close",
    )
    .await;
    record_async_subscription_close(
        report,
        subscription.as_mut(),
        "close-idempotence",
        "repeated async receiver close",
    )
    .await;
    record_async_shutdown(
        report,
        spi.as_ref(),
        "shutdown",
        "immediate shutdown",
        "async shutdown",
    )
    .await;
    record_async_shutdown(
        report,
        spi.as_ref(),
        "shutdown-idempotence",
        "repeated immediate shutdown",
        "repeated async shutdown",
    )
    .await;
}

/// Checks repeated acceptance when the provider issued a settlement token.
///
/// # Parameters
/// - `subscription`: receiver used for both idempotent settlement calls.
/// - `capabilities`: settlement capability declared by the provider.
/// - `token`: settlement token attached to the received message, if any.
///
/// # Returns
/// The pass, failure, or unsupported-capability result for repeated accept.
async fn async_settlement_idempotence_case(
    subscription: &mut dyn AsyncEventSubscriptionSpi,
    capabilities: SettlementCapabilities,
    token: Option<&SettlementToken>,
) -> ConformanceCase {
    match (capabilities, token) {
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
        (_, Some(token)) => async_repeat_accept_case(subscription, token).await,
    }
}

/// Applies accept twice and records the idempotence result.
///
/// # Parameters
/// - `subscription`: receiver used for the settlement operations.
/// - `token`: provider-issued token for the received message.
///
/// # Returns
/// A pass if both accepts succeed, otherwise a failure with the first error.
async fn async_repeat_accept_case(
    subscription: &mut dyn AsyncEventSubscriptionSpi,
    token: &SettlementToken,
) -> ConformanceCase {
    if let Err(error) = subscription
        .settle(token, DeliveryDisposition::Accept)
        .await
    {
        return ConformanceCase::Failed {
            case_id: "settlement-idempotence".into(),
            detail: format!("accept settlement failed: {error}"),
        };
    }
    match subscription
        .settle(token, DeliveryDisposition::Accept)
        .await
    {
        Ok(()) => ConformanceCase::Passed {
            case_id: "settlement-idempotence".into(),
        },
        Err(error) => ConformanceCase::Failed {
            case_id: "settlement-idempotence".into(),
            detail: format!("repeated accept settlement failed: {error}"),
        },
    }
}

/// Checks that a terminal token rejects a conflicting disposition.
///
/// # Parameters
/// - `subscription`: receiver used for the conflicting settlement operation.
/// - `capabilities`: settlement capability declared by the provider.
/// - `token`: settlement token attached to the received message, if any.
///
/// # Returns
/// A pass for the expected invalid-token error, a failure for another outcome,
/// or an unsupported-capability skip when no settlement token is available.
async fn async_conflicting_settlement_case(
    subscription: &mut dyn AsyncEventSubscriptionSpi,
    capabilities: SettlementCapabilities,
    token: Option<&SettlementToken>,
) -> ConformanceCase {
    if capabilities == SettlementCapabilities::None {
        return ConformanceCase::Skipped {
            case_id: "settlement-conflicting-disposition".into(),
            reason: ConformanceSkipReason::UnsupportedCapability {
                capability: "settlement",
            },
        };
    }
    let Some(token) = token else {
        return ConformanceCase::Skipped {
            case_id: "settlement-conflicting-disposition".into(),
            reason: ConformanceSkipReason::UnsupportedCapability {
                capability: "settlement",
            },
        };
    };
    match subscription
        .settle(token, DeliveryDisposition::Reject)
        .await
    {
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

/// Records a shutdown result, preserving the case-specific error wording.
///
/// # Parameters
/// - report: report receiving the shutdown result.
/// - spi: provider instance to shut down.
/// - case_id: stable result identifier.
/// - outcome_prefix: wording for a non-complete outcome.
/// - error_prefix: wording for an SPI error.
async fn record_async_shutdown(
    report: &mut ConformanceReport,
    spi: &dyn AsyncEventBusSpi,
    case_id: &'static str,
    outcome_prefix: &str,
    error_prefix: &str,
) {
    let result = match spi.shutdown(ShutdownMode::Immediate).await {
        Ok(ShutdownOutcome::Complete) => Ok(()),
        Ok(outcome) => Err(format!("{outcome_prefix} returned {outcome:?}")),
        Err(error) => Err(format!("{error_prefix} failed: {error}")),
    };
    push_async_result(report, case_id, result);
}

/// Records a receiver close result with its case-specific error wording.
///
/// # Parameters
/// - report: report receiving the close result.
/// - subscription: receiver to close.
/// - case_id: stable result identifier.
/// - error_prefix: wording for an SPI error.
async fn record_async_subscription_close(
    report: &mut ConformanceReport,
    subscription: &mut dyn AsyncEventSubscriptionSpi,
    case_id: &'static str,
    error_prefix: &str,
) {
    let result = subscription
        .close()
        .await
        .map_err(|error| format!("{error_prefix} failed: {error}"));
    push_async_result(report, case_id, result);
}

/// Pushes a pass or failure result into the conformance report.
///
/// # Parameters
/// - report: report receiving the result.
/// - case_id: stable result identifier.
/// - result: successful completion or its failure detail.
fn push_async_result(report: &mut ConformanceReport, case_id: &str, result: Result<(), String>) {
    report.push(match result {
        Ok(()) => ConformanceCase::Passed {
            case_id: case_id.into(),
        },
        Err(detail) => ConformanceCase::Failed {
            case_id: case_id.into(),
            detail,
        },
    });
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
    hook: Option<&AsyncConformanceCheck>,
) {
    match hook {
        Some(check) => push_async_result(report, case_id, check().await),
        None => report.push(ConformanceCase::Skipped {
            case_id: case_id.into(),
            reason: ConformanceSkipReason::MissingFixture {
                detail: "provider-specific async hook was not supplied".into(),
            },
        }),
    };
}
