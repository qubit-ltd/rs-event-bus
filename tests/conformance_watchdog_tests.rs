// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#![cfg(feature = "conformance")]

use std::fs;
use std::fs::File;
use std::future::Future;
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::pin::Pin;
use std::process::Command;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use qubit_event_bus::SpiError;
use qubit_event_bus::local::AsyncLocalEventBusProvider;
use qubit_event_bus::local::LocalEventBusProvider;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::registry::EventBusConfig;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::PayloadModes;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::conformance::AsyncConformanceHooks;
use qubit_event_bus::spi::conformance::ConformanceHooks;
use qubit_event_bus::spi::conformance::ConformanceProfile;
use qubit_event_bus::spi::conformance::ConformanceProgress;
use qubit_event_bus::spi::conformance::run_async_with_profile;
use qubit_event_bus::spi::conformance::run_async_with_profile_and_observer;
use qubit_event_bus::spi::conformance::run_sync_with_profile;
use qubit_event_bus::spi::conformance::run_sync_with_profile_and_observer;
use qubit_spi::AsyncServiceProvider;
use qubit_spi::ServiceProvider;

#[test]
fn test_observer_reports_factory_then_operation_with_context_and_matches_report() {
    let factory = || {
        let inner = LocalEventBusProvider
            .create_configured(&EventBusConfig::default())
            .unwrap();
        Arc::new(BothPayloadSyncSpi { inner }) as Arc<dyn EventBusSpi>
    };
    let ordinary = run_sync_with_profile(factory, &ConformanceHooks::default(), ConformanceProfile::Structural);
    let events = Mutex::new(Vec::<(String, Option<String>, String)>::new());
    let observer = |progress: &ConformanceProgress| {
        events.lock().unwrap().push((
            progress.case_id().to_owned(),
            progress
                .context()
                .map(|c| format!("{}:{:?}:{:?}", c.api(), c.payload_mode(), c.durability())),
            progress.stage().to_owned(),
        ));
    };
    let observed = run_sync_with_profile_and_observer(
        factory,
        &ConformanceHooks::default(),
        ConformanceProfile::Structural,
        &observer,
    );
    assert_eq!(ordinary.cases(), observed.cases());
    for index in 0..ordinary.cases().len() {
        assert_eq!(ordinary.case_context(index), observed.case_context(index));
    }
    let events = events.into_inner().unwrap();
    assert_eq!(events.first().map(|e| e.2.as_str()), Some("factory"));
    assert!(events.iter().any(|event| event.2 == "publish" && event.1.is_some()));
    assert!(
        events
            .iter()
            .any(|event| event.2 == "publish" && event.1.as_deref().is_some_and(|c| c.contains("Native")))
    );
    assert!(
        events
            .iter()
            .any(|event| event.2 == "publish" && event.1.as_deref().is_some_and(|c| c.contains("Encoded")))
    );

    let async_factory = || async {
        AsyncLocalEventBusProvider
            .create_configured(&EventBusConfig::default())
            .await
            .unwrap()
    };
    let ordinary_async = block_on(run_async_with_profile(
        async_factory,
        &AsyncConformanceHooks::default(),
        ConformanceProfile::Structural,
    ));
    let async_events = Mutex::new(Vec::<(String, Option<String>, String)>::new());
    let async_observer = |progress: &ConformanceProgress| {
        async_events.lock().unwrap().push((
            progress.case_id().to_owned(),
            progress
                .context()
                .map(|c| format!("{}:{:?}:{:?}", c.api(), c.payload_mode(), c.durability())),
            progress.stage().to_owned(),
        ));
    };
    let observed_async = block_on(run_async_with_profile_and_observer(
        async_factory,
        &AsyncConformanceHooks::default(),
        ConformanceProfile::Structural,
        &async_observer,
    ));
    assert_eq!(ordinary_async.cases(), observed_async.cases());
    for index in 0..ordinary_async.cases().len() {
        assert_eq!(ordinary_async.case_context(index), observed_async.case_context(index));
    }
    let async_events = async_events.into_inner().unwrap();
    assert!(async_events.first().is_some_and(|event| event.1.is_none()));
    assert!(
        async_events
            .iter()
            .any(|event| event.2 == "publish" && event.1.is_some())
    );
}

#[test]
fn test_runner_fixture_child() {
    let Ok(mode) = std::env::var("CONFORMANCE_WATCHDOG_FIXTURE") else {
        return;
    };
    let observer = |progress: &ConformanceProgress| {
        let context = progress
            .context()
            .map(|c| format!("Some({}:{:?}:{:?})", c.api(), c.payload_mode(), c.durability()))
            .unwrap_or_else(|| "None".to_owned());
        eprintln!("ENTERED:{}:{}:{}", progress.stage(), progress.case_id(), context);
        let _ = std::io::Write::flush(&mut std::io::stderr());
    };
    match mode.as_str() {
        "sync-blocking" => {
            let inner = LocalEventBusProvider
                .create_configured(&EventBusConfig::default())
                .unwrap();
            let factory = move || Arc::new(BlockingSyncSpi { inner: inner.clone() }) as Arc<dyn EventBusSpi>;
            let _ = run_sync_with_profile_and_observer(
                factory,
                &ConformanceHooks::default(),
                ConformanceProfile::Structural,
                &observer,
            );
        }
        "async-pending" => {
            let factory = || {
                eprintln!("ACTUAL:factory-pending");
                let _ = std::io::Write::flush(&mut std::io::stderr());
                std::future::pending::<Arc<dyn AsyncEventBusSpi>>()
            };
            let _ = block_on(run_async_with_profile_and_observer(
                factory,
                &AsyncConformanceHooks::default(),
                ConformanceProfile::Structural,
                &observer,
            ));
        }
        "async-poll-blocking" => {
            let factory = || async {
                eprintln!("ACTUAL:factory-poll");
                let _ = std::io::Write::flush(&mut std::io::stderr());
                thread::park();
                panic!("unreachable")
            };
            let _ = block_on(run_async_with_profile_and_observer(
                factory,
                &AsyncConformanceHooks::default(),
                ConformanceProfile::Structural,
                &observer,
            ));
        }
        "hook-blocking" => {
            let factory = || async {
                AsyncLocalEventBusProvider
                    .create_configured(&EventBusConfig::default())
                    .await
                    .expect("async local SPI should construct")
            };
            let hooks = AsyncConformanceHooks {
                receive_cancellation: Some(Arc::new(|| {
                    Box::pin(async {
                        eprintln!("ACTUAL:hook");
                        let _ = std::io::Write::flush(&mut std::io::stderr());
                        thread::park();
                        Ok::<(), String>(())
                    })
                })),
                ..AsyncConformanceHooks::default()
            };
            let _ = block_on(run_async_with_profile_and_observer(
                factory,
                &hooks,
                ConformanceProfile::Structural,
                &observer,
            ));
        }
        other => panic!("unknown fixture {other}"),
    }
}

#[test]
fn test_external_watchdog_bounds_all_runner_stuck_modes_and_observes_entry() {
    for (mode, progress, actual) in [
        (
            "sync-blocking",
            "ENTERED:publish:close-with-pending-setup:Some(sync:Native:Ephemeral)",
            "ACTUAL:sync-publish",
        ),
        (
            "async-pending",
            "ENTERED:factory:capability-payload-mode:None",
            "ACTUAL:factory-pending",
        ),
        (
            "async-poll-blocking",
            "ENTERED:factory:capability-payload-mode:None",
            "ACTUAL:factory-poll",
        ),
        ("hook-blocking", "ENTERED:hook:receive-cancellation:None", "ACTUAL:hook"),
    ] {
        let (mut child, paths) = launch_watchdog_fixture(mode, 1.5, false);
        let Some(status) = wait_until_with_cleanup(&mut child, Duration::from_secs(15), &paths, false) else {
            panic!("outer deadline expired for {mode}; nested process groups were terminated and reaped");
        };
        let _reaped = child.wait().expect("completed watchdog should be reaped");
        let stderr = fs::read_to_string(&paths.stderr).expect("captured stderr should be readable");
        assert_eq!(status.code(), Some(124), "{mode}: {stderr}");
        assert!(stderr.contains(progress), "{mode} did not report progress: {stderr}");
        assert!(
            stderr.contains(actual),
            "{mode} operation/hook was not entered: {stderr}"
        );
        assert!(stderr.contains(mode), "{mode} timeout label missing: {stderr}");
        remove_capture_files(&paths);
    }
}

#[test]
fn test_independent_outer_deadline_kills_nested_target_group_without_blocking_on_logs() {
    let (mut child, paths) = launch_watchdog_fixture("sync-blocking", 60.0, true);
    let Some(target_pid) = wait_for_pid_file(&paths.target_pid, Duration::from_secs(6)) else {
        let _ = cleanup_nested_processes(&mut child, &paths);
        remove_capture_files(&paths);
        panic!("watchdog did not publish target PID before outer-fixture setup expired");
    };
    let Some(descendant_pid) = wait_for_pid_file(&paths.descendant_pid, Duration::from_secs(6)) else {
        let _ = cleanup_nested_processes(&mut child, &paths);
        remove_capture_files(&paths);
        panic!("blocked SPI did not start descendant before outer-fixture setup expired");
    };
    let status = wait_until_with_cleanup(&mut child, Duration::from_secs(6), &paths, true);
    assert!(
        status.is_none(),
        "long inner deadline should still be active before outer cleanup"
    );
    let stderr = fs::read_to_string(&paths.stderr).expect("captured stderr should be readable");
    assert!(stderr.contains("ENTERED:publish:close-with-pending-setup:Some(sync:Native:Ephemeral)"));
    assert!(stderr.contains("ACTUAL:sync-publish"));
    assert_process_stopped(target_pid);
    assert_process_stopped(descendant_pid);
    remove_capture_files(&paths);
}

struct CapturePaths {
    stdout: PathBuf,
    stderr: PathBuf,
    target_pid: PathBuf,
    descendant_pid: PathBuf,
}

fn launch_watchdog_fixture(
    mode: &str,
    inner_seconds: f64,
    with_descendant: bool,
) -> (std::process::Child, CapturePaths) {
    static NEXT_CAPTURE: AtomicUsize = AtomicUsize::new(0);
    let unique = NEXT_CAPTURE.fetch_add(1, Ordering::Relaxed);
    let base = std::env::temp_dir().join(format!("conformance-watchdog-{}-{mode}-{unique}", std::process::id()));
    let paths = CapturePaths {
        stdout: base.with_extension("stdout"),
        stderr: base.with_extension("stderr"),
        target_pid: base.with_extension("target-pid"),
        descendant_pid: base.with_extension("descendant-pid"),
    };
    remove_capture_files(&paths);
    let mut command = Command::new("python3");
    command.args([
        "scripts/run_conformance_watchdog.py",
        "--timeout-seconds",
        &inner_seconds.to_string(),
        "--label",
        mode,
        "--",
    ]);
    command
        .arg(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "test_runner_fixture_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("CONFORMANCE_WATCHDOG_FIXTURE", mode)
        .env("CONFORMANCE_WATCHDOG_CHILD_PID_FILE", &paths.target_pid)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .stdout(File::create(&paths.stdout).expect("stdout capture file should open"))
        .stderr(File::create(&paths.stderr).expect("stderr capture file should open"));
    if with_descendant {
        command.env("CONFORMANCE_WATCHDOG_DESCENDANT_PID_FILE", &paths.descendant_pid);
    }
    #[cfg(unix)]
    command.process_group(0);
    let child = command.spawn().expect("watchdog should start");
    (child, paths)
}

fn wait_until_with_cleanup(
    child: &mut std::process::Child,
    duration: Duration,
    paths: &CapturePaths,
    retain_capture: bool,
) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + duration;
    loop {
        if let Some(status) = child.try_wait().expect("child should remain waitable") {
            return Some(status);
        }
        if Instant::now() >= deadline {
            let reaped = cleanup_nested_processes(child, paths);
            if !retain_capture {
                remove_capture_files(paths);
            }
            if !reaped {
                remove_capture_files(paths);
            }
            assert!(reaped, "outer cleanup could not reap watchdog process {}", child.id());
            return None;
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn cleanup_nested_processes(child: &mut std::process::Child, paths: &CapturePaths) -> bool {
    if let Some(target_pid) = wait_for_pid_file(&paths.target_pid, Duration::from_millis(100)) {
        kill_process_group(target_pid);
    }
    kill_process_group(child.id());
    if child.try_wait().expect("watchdog should remain waitable").is_none() {
        let _ = child.kill();
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if child.try_wait().expect("watchdog should remain waitable").is_some() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_pid_file(path: &PathBuf, duration: Duration) -> Option<u32> {
    let deadline = Instant::now() + duration;
    loop {
        if let Ok(contents) = fs::read_to_string(path)
            && let Ok(pid) = contents.trim().parse()
        {
            return Some(pid);
        }
        if Instant::now() >= deadline {
            return None;
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn kill_process_group(pid: u32) {
    #[cfg(unix)]
    {
        let _ = Command::new("kill").args(["-KILL", "--", &format!("-{pid}")]).status();
    }
    #[cfg(windows)]
    {
        let _ = Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .status();
    }
}

fn assert_process_stopped(pid: u32) {
    #[cfg(target_os = "linux")]
    {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match fs::read_to_string(format!("/proc/{pid}/stat")) {
                Err(_) => return,
                Ok(stat) if stat.split_whitespace().nth(2) == Some("Z") => return,
                Ok(stat) if Instant::now() >= deadline => panic!("process {pid} remained live: {stat}"),
                Ok(_) => thread::sleep(Duration::from_millis(10)),
            }
        }
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    {
        let status = Command::new("kill").args(["-0", &pid.to_string()]).status().unwrap();
        assert!(!status.success(), "process {pid} remained live");
    }
}

fn remove_capture_files(paths: &CapturePaths) {
    for path in [&paths.stdout, &paths.stderr, &paths.target_pid, &paths.descendant_pid] {
        let _ = fs::remove_file(path);
    }
}

struct BothPayloadSyncSpi {
    inner: Arc<dyn EventBusSpi>,
}
impl EventBusSpi for BothPayloadSyncSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        let c = self.inner.capabilities();
        EventBusCapabilities::new(
            PayloadModes::NativeAndEncoded,
            c.settlement(),
            c.ordering(),
            c.delayed_delivery(),
            c.durability(),
            c.subscription_modes(),
            c.consumer_groups(),
            c.replay(),
            c.publish_guarantee(),
            c.publish_visibility(),
        )
    }
    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        self.inner.publish(message)
    }
    fn subscribe(
        &self,
        request: SpiSubscriptionRequest,
    ) -> Result<Box<dyn qubit_event_bus::spi::EventSubscriptionSpi>, SpiError> {
        self.inner.subscribe(request)
    }
    fn shutdown(&self, mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        self.inner.shutdown(mode)
    }
}

struct BlockingSyncSpi {
    inner: Arc<dyn EventBusSpi>,
}
impl EventBusSpi for BlockingSyncSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        self.inner.capabilities()
    }
    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        eprintln!("ACTUAL:sync-publish");
        let _ = std::io::Write::flush(&mut std::io::stderr());
        if let Ok(pid_path) = std::env::var("CONFORMANCE_WATCHDOG_DESCENDANT_PID_FILE") {
            let descendant_code = format!(
                "import subprocess,sys,time; child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)']); open({pid_path:?},'w').write(str(child.pid)); time.sleep(60)"
            );
            let _ = Command::new("python3").arg("-c").arg(descendant_code).status();
        }
        thread::park();
        self.inner.publish(message)
    }
    fn subscribe(
        &self,
        request: SpiSubscriptionRequest,
    ) -> Result<Box<dyn qubit_event_bus::spi::EventSubscriptionSpi>, SpiError> {
        self.inner.subscribe(request)
    }
    fn shutdown(&self, mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        self.inner.shutdown(mode)
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = std::pin::pin!(future);
    loop {
        match Pin::as_mut(&mut future).poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::park_timeout(Duration::from_millis(10)),
        }
    }
}
