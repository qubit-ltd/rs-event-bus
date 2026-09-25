// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Measures synchronous subscription creation and receiver-thread teardown.

use std::io;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use qubit_event_bus::EventBus;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;

const COUNTS: [usize; 3] = [1, 16, 128];
const WARMUPS: usize = 2;
const SAMPLES: usize = 7;
const SAMPLE_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// One child-process sample result, with an empty thread count off Linux.
struct Sample {
    status: &'static str,
    creation_ns: u128,
    close_ns: u128,
    baseline_threads: Option<usize>,
    peak_threads: Option<usize>,
}

/// Counts process threads on Linux and leaves the metric absent elsewhere.
fn process_thread_count() -> io::Result<Option<usize>> {
    #[cfg(target_os = "linux")]
    {
        Ok(Some(std::fs::read_dir("/proc/self/task")?.count()))
    }

    #[cfg(not(target_os = "linux"))]
    {
        Ok(None)
    }
}

/// Updates the observed thread high-water mark and reports `/proc` failures.
fn update_peak(peak: &mut Option<usize>) -> io::Result<()> {
    if let Some(count) = process_thread_count()? {
        *peak = Some(peak.map_or(count, |previous| previous.max(count)));
    }
    Ok(())
}

/// Creates, subscribes, then cancels one fresh bus; only this work is timed.
fn sample(subscription_count: usize) -> Sample {
    let started = Instant::now();
    let bus = match EventBus::local(Default::default()) {
        Ok(bus) => bus,
        Err(_) => {
            return Sample {
                status: "provider_failed",
                creation_ns: started.elapsed().as_nanos(),
                close_ns: 0,
                baseline_threads: None,
                peak_threads: None,
            };
        }
    };

    let baseline_threads = process_thread_count().ok().flatten();
    let mut peak_threads = baseline_threads;
    let topic = match Topic::<u32>::new("thread-profile.empty") {
        Ok(topic) => topic,
        Err(_) => {
            return Sample {
                status: "topic_failed",
                creation_ns: started.elapsed().as_nanos(),
                close_ns: 0,
                baseline_threads,
                peak_threads,
            };
        }
    };
    let mut subscriptions = Vec::with_capacity(subscription_count);
    let mut status = "success";

    for index in 0..subscription_count {
        let subscriber_id = match SubscriberId::new(format!("thread-profile-{index}")) {
            Ok(subscriber_id) => subscriber_id,
            Err(_) => {
                status = "subscriber_id_failed";
                break;
            }
        };
        match bus.subscribe(SubscribeRequest::new(subscriber_id, topic.clone()), |_| {}) {
            Ok(subscription) => subscriptions.push(subscription),
            Err(_) => {
                status = "subscribe_failed";
                break;
            }
        }
        if update_peak(&mut peak_threads).is_err() {
            status = "thread_sample_failed";
            break;
        }
    }
    let creation_ns = started.elapsed().as_nanos();

    let close_started = Instant::now();
    for subscription in &subscriptions {
        if subscription.cancel().is_err() && status == "success" {
            status = "cancel_failed";
        }
    }
    if bus.shutdown(ShutdownMode::Immediate).is_err() && status == "success" {
        status = "shutdown_failed";
    }
    let close_ns = close_started.elapsed().as_nanos();

    Sample {
        status,
        creation_ns,
        close_ns,
        baseline_threads,
        peak_threads,
    }
}

/// Runs a single sample in a child process so a hung sample cannot poison later
/// ones.
fn run_child_sample(subscription_count: usize) {
    let result = std::panic::catch_unwind(|| sample(subscription_count));
    match result {
        Ok(sample) => println!(
            "{},{},{},{},{}",
            sample.status,
            sample.creation_ns,
            sample.close_ns,
            optional_number(sample.baseline_threads),
            optional_number(sample.peak_threads)
        ),
        Err(_) => println!("panic,,,,"),
    }
}

/// Formats an optional counter as an empty CSV cell when it is unavailable.
fn optional_number(value: Option<usize>) -> String {
    value.map_or_else(String::new, |number| number.to_string())
}

/// Waits for a child to finish or kills it after the per-sample deadline.
fn wait_with_timeout(child: &mut Child) -> io::Result<Option<std::process::ExitStatus>> {
    let deadline = Instant::now() + SAMPLE_TIMEOUT;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(Some(status));
        }
        if Instant::now() >= deadline {
            child.kill()?;
            let _ = child.wait()?;
            return Ok(None);
        }
        thread::sleep(POLL_INTERVAL);
    }
}

/// Spawns one independently isolated measurement and returns its CSV fields.
fn run_isolated_sample(subscription_count: usize) -> io::Result<(bool, String)> {
    let executable = std::env::current_exe()?;
    let mut child = Command::new(executable)
        .arg("--sample")
        .arg(subscription_count.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    let Some(status) = wait_with_timeout(&mut child)? else {
        return Ok((true, "timeout,,,,".to_owned()));
    };
    let output = child.wait_with_output()?;
    let line = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if status.success() && line.split(',').count() == 5 {
        Ok((false, line))
    } else {
        Ok((false, "child_failed,,,,".to_owned()))
    }
}

/// Runs two discarded warmups followed by seven recorded child samples.
fn run(subscription_count: usize) -> io::Result<()> {
    for _ in 0..WARMUPS {
        let (timed_out, fields) = run_isolated_sample(subscription_count)?;
        if timed_out || fields.split(',').next() != Some("success") {
            return Err(io::Error::other(format!(
                "warmup failed for {subscription_count} subscriptions"
            )));
        }
    }

    for iteration in 1..=SAMPLES {
        let (timed_out, fields) = run_isolated_sample(subscription_count)?;
        let mut fields = fields.split(',');
        let status = fields.next().unwrap_or("child_failed");
        let creation_ns = fields.next().unwrap_or("");
        let close_ns = fields.next().unwrap_or("");
        let baseline_threads = fields.next().unwrap_or("");
        let peak_threads = fields.next().unwrap_or("");
        let outcome = if timed_out { "timeout" } else { status };
        println!(
            "{subscription_count},{iteration},{outcome},{creation_ns},{close_ns},{baseline_threads},{peak_threads}"
        );
    }
    Ok(())
}

/// Dispatches child-sample mode or prints the documented seven-sample CSV.
fn main() {
    let args = std::env::args()
        .skip(1)
        .filter(|argument| argument != "--bench")
        .collect::<Vec<_>>();
    if args.first().is_some_and(|argument| argument == "--sample") {
        let Some(count) = args.get(1).and_then(|value| value.parse::<usize>().ok()) else {
            eprintln!("usage: local_threads --sample <subscription-count>");
            std::process::exit(2);
        };
        run_child_sample(count);
        return;
    }
    if !args.is_empty() {
        eprintln!("usage: local_threads");
        std::process::exit(2);
    }

    println!("subscriptions,iteration,status,creation_ns,cancel_shutdown_ns,baseline_threads,peak_threads");
    for subscription_count in COUNTS {
        if let Err(error) = run(subscription_count) {
            eprintln!("thread-profile benchmark failed: {error}");
            std::process::exit(1);
        }
    }
}
