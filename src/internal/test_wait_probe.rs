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

type WaitProbe = Option<(&'static str, Sender<()>)>;

static PROBE: OnceLock<Mutex<WaitProbe>> = OnceLock::new();

/// Installs a one-shot signal for the named wait site.
pub(crate) fn install(wait_site: &'static str) -> Receiver<()> {
    let (sender, receiver) = mpsc::channel();
    let previous = PROBE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .replace((wait_site, sender));
    assert!(previous.is_none(), "only one wait probe may be active");
    receiver
}

/// Signals immediately before the named condition-variable wait.
pub(crate) fn announce(wait_site: &'static str) {
    let mut probe = PROBE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if probe.as_ref().is_some_and(|(site, _)| *site == wait_site)
        && let Some((_, sender)) = probe.take()
    {
        let _ = sender.send(());
    }
}
