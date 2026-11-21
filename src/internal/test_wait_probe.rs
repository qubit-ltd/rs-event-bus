// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private synchronization probe for large-timeout unit tests.

use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;

struct WaitProbe {
    site: &'static str,
    sender: Sender<()>,
    gate: Option<Receiver<()>>,
}

type GatedProbe = (Receiver<()>, Sender<()>);

static PROBE: OnceLock<Mutex<Vec<WaitProbe>>> = OnceLock::new();

/// Installs a one-shot signal for the named wait site.
pub(crate) fn install(wait_site: &'static str) -> Receiver<()> {
    let (sender, receiver) = mpsc::channel();
    let mut probes = PROBE
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(probes.is_empty(), "only one wait probe may be active");
    probes.push(WaitProbe {
        site: wait_site,
        sender,
        gate: None,
    });
    receiver
}

/// Installs a blocking gate and a second one-shot signal for a controlled race.
pub(crate) fn install_gated(
    gate_site: &'static str,
    observed_site: &'static str,
) -> (Receiver<()>, Receiver<()>, Sender<()>) {
    let (gate_sender, gate_receiver) = mpsc::channel();
    let (gate_release_sender, gate_release_receiver) = mpsc::channel();
    let (observed_sender, observed_receiver) = mpsc::channel();
    let mut probes = PROBE
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(probes.is_empty(), "only one wait probe may be active");
    probes.push(WaitProbe {
        site: gate_site,
        sender: gate_sender,
        gate: Some(gate_release_receiver),
    });
    probes.push(WaitProbe {
        site: observed_site,
        sender: observed_sender,
        gate: None,
    });
    (gate_receiver, observed_receiver, gate_release_sender)
}

/// Installs two independently controlled gates for one deterministic race.
pub(crate) fn install_two_gated(first_site: &'static str, second_site: &'static str) -> (GatedProbe, GatedProbe) {
    let (first_seen_tx, first_seen_rx) = mpsc::channel();
    let (first_release_tx, first_release_rx) = mpsc::channel();
    let (second_seen_tx, second_seen_rx) = mpsc::channel();
    let (second_release_tx, second_release_rx) = mpsc::channel();
    let mut probes = PROBE
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(probes.is_empty(), "only one wait probe may be active");
    probes.push(WaitProbe {
        site: first_site,
        sender: first_seen_tx,
        gate: Some(first_release_rx),
    });
    probes.push(WaitProbe {
        site: second_site,
        sender: second_seen_tx,
        gate: Some(second_release_rx),
    });
    ((first_seen_rx, first_release_tx), (second_seen_rx, second_release_tx))
}

/// Signals at the named synchronization point, blocking if a gate was
/// installed.
pub(crate) fn announce(wait_site: &'static str) {
    let mut probes = PROBE
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut gates = Vec::new();
    let mut index = 0;
    while index < probes.len() {
        if probes[index].site == wait_site {
            let probe = probes.remove(index);
            let _ = probe.sender.send(());
            if let Some(gate) = probe.gate {
                gates.push(gate);
            }
        } else {
            index += 1;
        }
    }
    drop(probes);
    for gate in gates {
        let _ = gate.recv();
    }
}
