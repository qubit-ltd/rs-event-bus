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

use super::conformance_hooks::ConformanceHooks;
use super::conformance_report::ConformanceCase;
use super::conformance_report::ConformanceReport;
use super::conformance_report::payload_matches;
use super::conformance_report::payload_probes;
use super::conformance_report::probe_message;
use super::conformance_report::probe_request;
use super::conformance_report::publish_case;
use super::conformance_report::push_hook;
use crate::spi::AsyncEventBusSpi;
use crate::spi::DeliveryDisposition;
use crate::spi::ReceiveOutcome;
use crate::spi::SettlementCapabilities;
use crate::spi::ShutdownMode;
use crate::spi::ShutdownOutcome;

/// Runs common structural checks against a fresh asynchronous provider.
pub async fn run_async<F, Fut>(factory: F, hooks: &ConformanceHooks) -> ConformanceReport
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
        let mut subscription = match spi.subscribe(probe_request(1)).await {
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
                    reason: "subscription could not be created".into(),
                });
                report.push(match spi.shutdown(ShutdownMode::Immediate).await {
                    Ok(ShutdownOutcome::Complete) => ConformanceCase::Passed {
                        case_id: "shutdown".into(),
                    },
                    Ok(outcome) => ConformanceCase::Failed {
                        case_id: "shutdown".into(),
                        detail: format!("immediate shutdown returned {outcome:?}"),
                    },
                    Err(error) => ConformanceCase::Failed {
                        case_id: "shutdown".into(),
                        detail: format!("async shutdown failed: {error}"),
                    },
                });
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
                reason: "publish failed".into(),
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
        report.push(match spi.shutdown(ShutdownMode::Immediate).await {
            Ok(ShutdownOutcome::Complete) => ConformanceCase::Passed {
                case_id: "shutdown".into(),
            },
            Ok(outcome) => ConformanceCase::Failed {
                case_id: "shutdown".into(),
                detail: format!("immediate shutdown returned {outcome:?}"),
            },
            Err(error) => ConformanceCase::Failed {
                case_id: "shutdown".into(),
                detail: format!("async shutdown failed: {error}"),
            },
        });
    }
    push_hook(
        &mut report,
        "provider-settlement-idempotence",
        hooks.settlement.as_ref(),
    );
    push_hook(&mut report, "receive-cancellation", hooks.receive_cancellation.as_ref());
    report
}
