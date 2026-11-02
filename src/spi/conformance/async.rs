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

use qubit_id::Id;

use super::async_conformance_hooks::AsyncConformanceCheck;
use super::async_conformance_hooks::AsyncConformanceHooks;
use super::conformance_case_context::ConformanceCaseContext;
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
use super::conformance_report::validate_probe_acknowledgement;
use super::conformance_skip_reason::ConformanceSkipReason;
use super::internal::ProbeScope;
use crate::model::SubscriptionDurability;
use crate::spi::AsyncEventBusSpi;
use crate::spi::AsyncEventSubscriptionSpi;
use crate::spi::DeliveryDisposition;
use crate::spi::EventBusCapabilities;
use crate::spi::ReceiveOutcome;
use crate::spi::SettlementCapabilities;
use crate::spi::SettlementToken;
use crate::spi::ShutdownMode;
use crate::spi::ShutdownOutcome;
use crate::spi::TransportPayload;

/// Runs common structural checks against a fresh asynchronous provider.
/// The factory is called again for each payload and subscription-mode
/// combination so one case cannot contaminate later cases through shared
/// provider state.
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
    let modes = probe_subscription_modes(capability_spi.capabilities());
    let mut first_combination = true;
    for (case_id, _) in payloads {
        for durability in modes.iter().copied() {
            let payload_mode = if case_id == "declared-native-publish" {
                crate::spi::PayloadModes::Native
            } else {
                crate::spi::PayloadModes::Encoded
            };
            report.set_case_context(Some(ConformanceCaseContext::new("async", payload_mode, durability)));
            record_pending_close(&factory, case_id, durability, &mut report).await;
            let spi = if first_combination {
                first_combination = false;
                capability_spi.clone()
            } else {
                factory().await
            };
            run_payload_case(spi, case_id, durability, &mut report).await;
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
        push_async_hook(
            &mut report,
            "provider-settlement-idempotence",
            hooks.settlement.as_ref(),
        )
        .await;
    }
    push_async_hook(&mut report, "receive-cancellation", hooks.receive_cancellation.as_ref()).await;
    push_async_hook(
        &mut report,
        "subscribe-cancellation",
        hooks.subscribe_cancellation.as_ref(),
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

async fn record_pending_close<F, Fut>(
    factory: &F,
    case_id: &'static str,
    durability: SubscriptionDurability,
    report: &mut ConformanceReport,
) where
    F: Fn() -> Fut,
    Fut: Future<Output = Arc<dyn AsyncEventBusSpi>>,
{
    let spi = factory().await;
    let capabilities = spi.capabilities();
    let scope = match ProbeScope::new() {
        Ok(scope) => scope,
        Err(error) => {
            report.push(ConformanceCase::Failed {
                case_id: "close-with-pending-setup".into(),
                detail: format!("async pending-close probe identity generation failed: {error}"),
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
            report.push(match spi.shutdown(ShutdownMode::Immediate).await {
                Ok(_) => ConformanceCase::Passed {
                    case_id: "close-with-pending-setup-cleanup".into(),
                },
                Err(error) => ConformanceCase::Failed {
                    case_id: "close-with-pending-setup-cleanup".into(),
                    detail: format!("async pending-close cleanup after identity failure failed: {error}"),
                },
            });
            return;
        }
    };
    let mut subscription = match spi.subscribe(probe_request(1, durability, &scope)).await {
        Ok(subscription) => subscription,
        Err(error) => {
            report.push(ConformanceCase::Failed {
                case_id: "close-with-pending-setup".into(),
                detail: format!("async pending-close subscribe failed: {error}"),
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
            report.push(match spi.shutdown(ShutdownMode::Immediate).await {
                Ok(_) => ConformanceCase::Passed {
                    case_id: "close-with-pending-setup-cleanup".into(),
                },
                Err(error) => ConformanceCase::Failed {
                    case_id: "close-with-pending-setup-cleanup".into(),
                    detail: format!("async pending-close SPI shutdown failed after setup failure: {error}"),
                },
            });
            return;
        }
    };
    let publish = spi
        .publish(probe_message(probe_payload(case_id), capabilities, &scope))
        .await;
    let setup = match publish {
        Ok(acknowledgement) => {
            validate_probe_acknowledgement(&acknowledgement, capabilities.publish_visibility(), Id::new(1))
        }
        Err(error) => Err(error.to_string()),
    };
    if let Err(error) = setup {
        report.push(ConformanceCase::Failed {
            case_id: "close-with-pending-setup".into(),
            detail: format!("async pending-close probe setup failed: {error}"),
        });
        report.push(ConformanceCase::Skipped {
            case_id: "receive-after-close-with-pending".into(),
            reason: ConformanceSkipReason::MissingFixture {
                detail: "pending-close probe publish failed".into(),
            },
        });
        let close_result = subscription.close().await;
        let shutdown_result = spi.shutdown(ShutdownMode::Immediate).await;
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
                detail: format!(
                    "async pending-close setup cleanup errors: {}",
                    cleanup_errors.join("; ")
                ),
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
        let close_result = subscription.close().await;
        report.push(match close_result {
            Ok(()) => match subscription.receive(Duration::ZERO).await {
                Ok(ReceiveOutcome::Closed) => ConformanceCase::Passed {
                    case_id: "receive-after-close-with-pending".into(),
                },
                Ok(_) => ConformanceCase::Failed {
                    case_id: "receive-after-close-with-pending".into(),
                    detail: "async close with pending probe returned a non-Closed outcome".into(),
                },
                Err(error) => ConformanceCase::Failed {
                    case_id: "receive-after-close-with-pending".into(),
                    detail: format!("async receive after pending close failed: {error}"),
                },
            },
            Err(error) => ConformanceCase::Failed {
                case_id: "receive-after-close-with-pending".into(),
                detail: format!("async pending-close close failed: {error}"),
            },
        });
        report.push(match subscription.close().await {
            Ok(()) => ConformanceCase::Passed {
                case_id: "close-with-pending-idempotent".into(),
            },
            Err(error) => ConformanceCase::Failed {
                case_id: "close-with-pending-idempotent".into(),
                detail: format!("repeated async pending-close failed: {error}"),
            },
        });
    }
    let _ = spi.shutdown(ShutdownMode::Immediate).await;
}

/// Runs publish, receive, settlement, close, and shutdown checks for one
/// payload and subscription mode.
///
/// # Parameters
/// - `spi`: fresh provider instance used only for this payload case.
/// - `case_id`: identifier of the payload probe being exercised.
/// - `durability`: declared subscription mode used by the probe.
/// - `report`: report receiving the ordered case results.
async fn run_payload_case(
    spi: Arc<dyn AsyncEventBusSpi>,
    case_id: &'static str,
    durability: SubscriptionDurability,
    report: &mut ConformanceReport,
) {
    let capabilities = spi.capabilities();
    let scope = match ProbeScope::new() {
        Ok(scope) => scope,
        Err(error) => {
            report.push(ConformanceCase::Failed {
                case_id: "probe-scope".into(),
                detail: format!("async probe identity generation failed: {error}"),
            });
            record_async_shutdown(
                report,
                spi.as_ref(),
                "shutdown",
                "immediate shutdown after probe identity failure",
                "async shutdown",
            )
            .await;
            return;
        }
    };
    let mode_name = match durability {
        SubscriptionDurability::Ephemeral => "ephemeral",
        SubscriptionDurability::Durable => "durable",
    };
    let mut subscription = match spi.subscribe(probe_request(1, durability, &scope)).await {
        Ok(subscription) => {
            report.push(ConformanceCase::Passed {
                case_id: "subscribe".into(),
            });
            subscription
        }
        Err(error) => {
            report.push(ConformanceCase::Failed {
                case_id: "subscribe".into(),
                detail: format!("async subscribe failed for {mode_name} mode: {error}"),
            });
            report.push(ConformanceCase::Skipped {
                case_id: "publish-receive".into(),
                reason: ConformanceSkipReason::MissingFixture {
                    detail: "subscription could not be created".into(),
                },
            });
            record_async_shutdown(report, spi.as_ref(), "shutdown", "immediate shutdown", "async shutdown").await;
            return;
        }
    };

    report.push(match subscription.receive(Duration::ZERO).await {
        Ok(ReceiveOutcome::TimedOut) => ConformanceCase::Passed {
            case_id: "receive-empty-zero-timeout".into(),
        },
        Ok(ReceiveOutcome::Message(_)) => ConformanceCase::Failed {
            case_id: "receive-empty-zero-timeout".into(),
            detail: format!("async {mode_name} mode: empty receive returned Message"),
        },
        Ok(ReceiveOutcome::Gap(_)) => ConformanceCase::Failed {
            case_id: "receive-empty-zero-timeout".into(),
            detail: format!("async {mode_name} mode: empty receive returned Gap"),
        },
        Ok(ReceiveOutcome::Closed) => ConformanceCase::Failed {
            case_id: "receive-empty-zero-timeout".into(),
            detail: format!("async {mode_name} mode: empty receive returned Closed"),
        },
        Err(error) => ConformanceCase::Failed {
            case_id: "receive-empty-zero-timeout".into(),
            detail: format!("async {mode_name} mode: empty receive failed: {error}"),
        },
    });

    record_async_publish_receive(
        report,
        spi.as_ref(),
        subscription.as_mut(),
        AsyncPublishProbe {
            case_id,
            payload: probe_payload(case_id),
            capabilities,
            durability,
            scope: &scope,
        },
    )
    .await;

    let close_result = subscription.close().await;
    let close_summary = match &close_result {
        Ok(()) => Ok(()),
        Err(error) => Err(format!("async receiver close failed: {error}")),
    };
    push_async_result(report, "close", close_summary);
    report.push(match close_result {
        Ok(()) => match subscription.receive(Duration::ZERO).await {
            Ok(ReceiveOutcome::Closed) => ConformanceCase::Passed {
                case_id: "receive-after-close".into(),
            },
            Ok(ReceiveOutcome::TimedOut) => ConformanceCase::Failed {
                case_id: "receive-after-close".into(),
                detail: format!("async {mode_name} mode: receive after close returned TimedOut"),
            },
            Ok(ReceiveOutcome::Message(_)) => ConformanceCase::Failed {
                case_id: "receive-after-close".into(),
                detail: format!("async {mode_name} mode: receive after close returned Message"),
            },
            Ok(ReceiveOutcome::Gap(_)) => ConformanceCase::Failed {
                case_id: "receive-after-close".into(),
                detail: format!("async {mode_name} mode: receive after close returned Gap"),
            },
            Err(error) => ConformanceCase::Failed {
                case_id: "receive-after-close".into(),
                detail: format!("async {mode_name} mode: receive after close failed: {error}"),
            },
        },
        Err(_) => ConformanceCase::Skipped {
            case_id: "receive-after-close".into(),
            reason: ConformanceSkipReason::MissingFixture {
                detail: "receiver close failed".into(),
            },
        },
    });
    record_async_subscription_close(
        report,
        subscription.as_mut(),
        "close-idempotence",
        "repeated async receiver close",
    )
    .await;
    record_async_shutdown(report, spi.as_ref(), "shutdown", "immediate shutdown", "async shutdown").await;
    record_async_shutdown(
        report,
        spi.as_ref(),
        "shutdown-idempotence",
        "repeated immediate shutdown",
        "repeated async shutdown",
    )
    .await;
}

struct AsyncPublishProbe<'a> {
    case_id: &'static str,
    payload: TransportPayload,
    capabilities: EventBusCapabilities,
    durability: SubscriptionDurability,
    scope: &'a ProbeScope,
}

/// Publishes one probe, observes its delivery, and checks settlement behavior.
///
/// # Parameters
/// - `report`: report receiving the publish, receive, and settlement results.
/// - `spi`: provider used to publish the probe.
/// - `subscription`: receiver used to observe and settle the probe.
/// - `probe`: payload identity, transport representation, and provider context.
async fn record_async_publish_receive(
    report: &mut ConformanceReport,
    spi: &dyn AsyncEventBusSpi,
    subscription: &mut dyn AsyncEventSubscriptionSpi,
    probe: AsyncPublishProbe<'_>,
) {
    let mode_name = match probe.durability {
        SubscriptionDurability::Ephemeral => "ephemeral",
        SubscriptionDurability::Durable => "durable",
    };
    let expected = probe_message(probe_payload(probe.case_id), probe.capabilities, probe.scope);
    let result = spi
        .publish(probe_message(probe.payload, probe.capabilities, probe.scope))
        .await;
    let published = result.is_ok();
    report.push(publish_case(
        probe.case_id,
        result,
        "async",
        probe.capabilities.publish_visibility(),
        qubit_id::Id::new(1),
        probe.durability,
    ));
    if published {
        match subscription.receive(Duration::from_secs(1)).await {
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
                report.push(
                    async_settlement_idempotence_case(subscription, probe.capabilities.settlement(), token.as_ref())
                        .await,
                );
                report.push(
                    async_conflicting_settlement_case(subscription, probe.capabilities.settlement(), token.as_ref())
                        .await,
                );
            }
            Ok(_) => report.push(ConformanceCase::Failed {
                case_id: "receive-payload".into(),
                detail: format!("{mode_name} mode: publish was not followed by a message"),
            }),
            Err(error) => report.push(ConformanceCase::Failed {
                case_id: "receive-payload".into(),
                detail: format!("{mode_name} mode: async receive failed: {error}"),
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
    if let Err(error) = subscription.settle(token, DeliveryDisposition::Accept).await {
        return ConformanceCase::Failed {
            case_id: "settlement-idempotence".into(),
            detail: format!("accept settlement failed: {error}"),
        };
    }
    match subscription.settle(token, DeliveryDisposition::Accept).await {
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

/// Records a shutdown result, preserving the case-specific error wording.
///
/// # Parameters
/// - `report`: report receiving the shutdown result.
/// - `spi`: provider instance to shut down.
/// - `case_id`: stable result identifier.
/// - `outcome_prefix`: wording for a non-complete outcome.
/// - `error_prefix`: wording for an SPI error.
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
/// - `report`: report receiving the close result.
/// - `subscription`: receiver to close.
/// - `case_id`: stable result identifier.
/// - `error_prefix`: wording for an SPI error.
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
/// - `report`: report receiving the result.
/// - `case_id`: stable result identifier.
/// - `result`: successful completion or its failure detail.
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
async fn push_async_hook(report: &mut ConformanceReport, case_id: &str, hook: Option<&AsyncConformanceCheck>) {
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
