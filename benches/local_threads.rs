// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Measures sync and async local subscription creation and teardown resources.

use std::any::TypeId;
use std::env;
use std::fs;
use std::future::Future;
use std::hint::black_box;
use std::io;
use std::panic;
use std::pin::Pin;
use std::pin::pin;
use std::process;
use std::process::Child;
use std::process::Command;
use std::process::ExitStatus;
use std::process::Stdio;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::EventBus;
use qubit_event_bus::error::ConfigurationError;
use qubit_event_bus::error::ReceiveError;
use qubit_event_bus::local::AsyncLocalEventBusSpi;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_id::Id;

const COUNTS: [usize; 3] = [16, 64, 256];
const WARMUPS: usize = 2;
const SAMPLES: usize = 7;
const SAMPLE_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Pinned runner future retained while measuring async subscription shutdown.
///
/// # Type Parameters
/// - `'a`: Lifetime of the borrowed async subscription handle.
type ReceiveRunner<'a> = Pin<Box<dyn Future<Output = Result<(), ReceiveError>> + 'a>>;

/// One child-process sample result, with an empty thread count off Linux.
struct Sample {
    /// Outcome label reported by the benchmark process.
    status: &'static str,
    /// Time spent creating subscriptions.
    creation_ns: u128,
    /// Time spent cancelling subscriptions and shutting down the bus.
    close_ns: u128,
    /// Process thread count before subscription creation when available.
    baseline_threads: Option<usize>,
    /// Highest process thread count observed during creation when available.
    peak_threads: Option<usize>,
}

/// Counts process threads on Linux and leaves the metric absent elsewhere.
///
/// # Returns
/// The process thread count on Linux, or `None` on other platforms.
///
/// # Errors
/// Returns an I/O error when Linux `/proc/self/task` cannot be read.
fn process_thread_count() -> io::Result<Option<usize>> {
    #[cfg(target_os = "linux")]
    {
        Ok(Some(fs::read_dir("/proc/self/task")?.count()))
    }

    #[cfg(not(target_os = "linux"))]
    {
        Ok(None)
    }
}

/// Updates the observed thread high-water mark and reports `/proc` failures.
///
/// # Parameters
/// - `peak`: Current maximum count, updated when a Linux count is available.
///
/// # Returns
/// `Ok(())` when sampling succeeds or the platform has no thread-count metric.
///
/// # Errors
/// Returns an I/O error from the process thread-count read.
fn update_peak(peak: &mut Option<usize>) -> io::Result<()> {
    if let Some(count) = process_thread_count()? {
        *peak = Some(peak.map_or(count, |previous| previous.max(count)));
    }
    Ok(())
}

/// Creates, subscribes, then cancels one fresh bus while measuring thread
/// growth.
///
/// # Parameters
/// - `subscription_count`: Number of empty subscriptions to create.
///
/// # Returns
/// Creation, close, and process-thread measurements for the isolated sample.
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
        match bus.subscribe(
            SubscribeRequest::new(subscriber_id.as_str(), topic.clone()).expect("validated subscriber ID"),
            |_| {},
        ) {
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

/// Creates and closes async-local subscriptions while measuring executor
/// thread use.
///
/// # Parameters
/// - `subscription_count`: Number of receive runners to drive.
///
/// # Returns
/// Creation, close, and process-thread measurements for the isolated sample.
fn sample_async(subscription_count: usize) -> Sample {
    let started = Instant::now();
    let bus = match block_on(AsyncEventBus::local(Default::default())) {
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
    let topic = Topic::<u32>::new("thread-profile.async-empty").expect("benchmark topic is valid");
    let mut subscriptions = Vec::with_capacity(subscription_count);
    for index in 0..subscription_count {
        let request = SubscribeRequest::new(&format!("async-thread-profile-{index}"), topic.clone())
            .expect("subscriber ID is valid");
        match block_on(bus.subscribe(request)) {
            Ok(subscription) => subscriptions.push(subscription),
            Err(_) => {
                return Sample {
                    status: "subscribe_failed",
                    creation_ns: started.elapsed().as_nanos(),
                    close_ns: 0,
                    baseline_threads,
                    peak_threads,
                };
            }
        }
        if update_peak(&mut peak_threads).is_err() {
            return Sample {
                status: "thread_sample_failed",
                creation_ns: started.elapsed().as_nanos(),
                close_ns: 0,
                baseline_threads,
                peak_threads,
            };
        }
    }
    let creation_ns = started.elapsed().as_nanos();
    let mut runners: Vec<ReceiveRunner<'_>> = subscriptions
        .iter_mut()
        .map(|subscription| Box::pin(subscription.run(|_| async { Ok(()) })) as _)
        .collect();
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut pending = 0;
    for runner in &mut runners {
        if runner.as_mut().poll(&mut context).is_pending() {
            pending += 1;
        }
    }
    if pending != subscription_count {
        return Sample {
            status: "receiver_not_pending",
            creation_ns,
            close_ns: 0,
            baseline_threads,
            peak_threads,
        };
    }
    let close_started = Instant::now();
    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    let mut shutdown_done = false;
    let mut status = "success";
    let mut completed = vec![false; runners.len()];
    while completed.iter().any(|ready| !ready) || !shutdown_done {
        let mut pending = 0;
        for (index, runner) in runners.iter_mut().enumerate() {
            if completed[index] {
                continue;
            }
            match runner.as_mut().poll(&mut context) {
                Poll::Ready(Ok(())) => completed[index] = true,
                Poll::Ready(Err(_)) => {
                    completed[index] = true;
                    status = "receiver_failed";
                }
                Poll::Pending => pending += 1,
            }
        }
        if !shutdown_done {
            match shutdown.as_mut().poll(&mut context) {
                Poll::Ready(Ok(_)) => shutdown_done = true,
                Poll::Ready(Err(_)) => {
                    shutdown_done = true;
                    status = "shutdown_failed";
                }
                Poll::Pending => pending += 1,
            }
        }
        if pending != 0 {
            thread::park_timeout(Duration::from_millis(1));
        }
    }
    Sample {
        status,
        creation_ns,
        close_ns: close_started.elapsed().as_nanos(),
        baseline_threads,
        peak_threads,
    }
}

/// Checks dropped async subscriptions leave no publish destinations behind.
///
/// # Returns
/// `Ok(())` when all dropped handles are removed from routing.
///
/// # Errors
/// Returns an I/O-wrapped facade or SPI failure, or a failed routing assertion.
fn run_async_churn_probe() -> io::Result<()> {
    let bus = block_on(AsyncEventBus::local(Default::default())).map_err(io::Error::other)?;
    let topic = Topic::<u32>::new("thread-profile.async-churn").map_err(io::Error::other)?;
    let started = Instant::now();
    for index in 0..100 {
        let request =
            SubscribeRequest::new(&format!("async-churn-{index}"), topic.clone()).map_err(io::Error::other)?;
        let subscription = block_on(bus.subscribe(request)).map_err(io::Error::other)?;
        drop(subscription);
    }
    let elapsed = started.elapsed().as_nanos();
    let threads = process_thread_count()?;
    let receipt =
        block_on(bus.publish(PublishRequest::new(topic, 1).map_err(io::Error::other)?)).map_err(io::Error::other)?;
    let admissions = match receipt.acknowledgement() {
        PublishAcknowledgement::DestinationAdmissions(admissions) => admissions.len(),
        _ => {
            return Err(io::Error::other("local provider did not report destination admissions"));
        }
    };
    if admissions != 0 {
        return Err(io::Error::other("dropped async subscriptions remained publish targets"));
    }
    println!(
        "async_churn,100,{},admissions={},threads={}",
        elapsed,
        admissions,
        optional_number(threads)
    );
    block_on(bus.shutdown(ShutdownMode::Immediate)).map_err(io::Error::other)?;
    Ok(())
}

/// Measures hot-topic publish latency as unrelated subscriptions accumulate.
///
/// # Returns
/// `Ok(())` after printing the routing measurements.
///
/// # Errors
/// Returns an I/O-wrapped provider, message, or settlement failure.
fn run_async_routing_probe() -> io::Result<()> {
    for cold_topics in [0_usize, 127, 1023] {
        let mut samples = Vec::with_capacity(SAMPLES);
        for _ in 0..WARMUPS + SAMPLES {
            let spi = AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new()).map_err(io::Error::other)?;
            let hot = TopicAddress::new("bench.hot").map_err(io::Error::other)?;
            let mut hot_receiver = block_on(spi.subscribe(SpiSubscriptionRequest::new(
                Id::new(1),
                hot.clone(),
                SubscriberId::new("hot").map_err(io::Error::other)?,
                None,
                SubscriptionDurability::Ephemeral,
                StartPosition::New,
                ProviderOptions::default(),
                TypeId::of::<String>(),
            )))
            .map_err(io::Error::other)?;
            let mut cold_receivers = Vec::with_capacity(cold_topics);
            for index in 0..cold_topics {
                let request = SpiSubscriptionRequest::new(
                    Id::new(index as u64 + 2),
                    TopicAddress::new(&format!("bench.cold.{index}")).map_err(io::Error::other)?,
                    SubscriberId::new(format!("cold-{index}")).map_err(io::Error::other)?,
                    None,
                    SubscriptionDurability::Ephemeral,
                    StartPosition::New,
                    ProviderOptions::default(),
                    TypeId::of::<String>(),
                );
                cold_receivers.push(block_on(spi.subscribe(request)).map_err(io::Error::other)?);
            }
            let messages = (0..128)
                .map(|index| -> Result<OutboundMessage, ConfigurationError> {
                    Ok(OutboundMessage::new(
                        hot.clone(),
                        EventId::new(format!("bench-event-{index}"))?,
                        SystemTime::now(),
                        Headers::new(),
                        None,
                        None,
                        TransportPayload::Native(Arc::new(String::from("payload"))),
                    ))
                })
                .collect::<Result<Vec<_>, ConfigurationError>>()
                .map_err(io::Error::other)?;
            let mut elapsed = 0;
            for message in messages {
                let message = black_box(message);
                let started = Instant::now();
                let result = block_on(spi.publish(message));
                elapsed += started.elapsed().as_nanos();
                result.map_err(io::Error::other)?;
                let outcome = block_on(hot_receiver.receive(Duration::ZERO)).map_err(io::Error::other)?;
                let ReceiveOutcome::Message(mut inbound) = outcome else {
                    return Err(io::Error::other("hot topic message was not received"));
                };
                let token = inbound
                    .take_settlement()
                    .ok_or_else(|| io::Error::other("missing settlement token"))?;
                block_on(hot_receiver.settle(&token, DeliveryDisposition::Accept)).map_err(io::Error::other)?;
            }
            let elapsed = elapsed / 128;
            samples.push(elapsed);
            block_on(spi.shutdown(ShutdownMode::Immediate)).map_err(io::Error::other)?;
            drop(cold_receivers);
        }
        let mut measured = samples.split_off(WARMUPS);
        measured.sort_unstable();
        println!(
            "async_local_route,cold_topics={cold_topics},median_ns_per_publish={}",
            measured[measured.len() / 2]
        );
    }
    Ok(())
}

/// Wakes the benchmark executor thread after an async task makes progress.
struct ThreadWake(thread::Thread);
impl Wake for ThreadWake {
    /// Unparks the thread that owns this waker.
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    /// Unparks the thread without consuming this shared waker.
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

/// Drives a future on the benchmark thread without an async runtime.
///
/// # Type Parameters
/// - `F`: Future to poll.
///
/// # Parameters
/// - `future`: Operation to poll until it completes.
///
/// # Returns
/// The future's output.
fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::park(),
        }
    }
}

/// Runs a single sample in a child process so a hung sample cannot poison later
/// ones.
///
/// # Parameters
/// - `subscription_count`: Number of subscriptions used by the sample.
/// - `asynchronous`: Whether to run the async local provider sample.
///
/// # Side Effects
/// Prints one status record to stdout; a caught panic becomes a `panic` record.
fn run_child_sample(subscription_count: usize, asynchronous: bool) {
    let result = panic::catch_unwind(|| {
        if asynchronous {
            sample_async(subscription_count)
        } else {
            sample(subscription_count)
        }
    });
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
///
/// # Parameters
/// - `value`: Optional count to render.
///
/// # Returns
/// The decimal count or an empty string.
fn optional_number(value: Option<usize>) -> String {
    value.map_or_else(String::new, |number| number.to_string())
}

/// Waits for a child to finish or kills it after the per-sample deadline.
///
/// # Parameters
/// - `child`: Child process being monitored.
///
/// # Returns
/// The exit status when it finishes, or `None` after timeout and termination.
///
/// # Errors
/// Returns an I/O error if polling, killing, or waiting for the child fails.
fn wait_with_timeout(child: &mut Child) -> io::Result<Option<ExitStatus>> {
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
///
/// # Parameters
/// - `subscription_count`: Number of subscriptions used by the sample.
/// - `asynchronous`: Whether to run the async local provider sample.
///
/// # Returns
/// Whether it timed out and the child's one-line result record.
///
/// # Errors
/// Returns process creation, waiting, or output collection failures.
fn run_isolated_sample(subscription_count: usize, asynchronous: bool) -> io::Result<(bool, String)> {
    let executable = env::current_exe()?;
    let mut child = Command::new(executable)
        .arg("--sample")
        .arg(subscription_count.to_string())
        .args(if asynchronous { vec!["--async"] } else { Vec::new() })
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
///
/// # Parameters
/// - `subscription_count`: Number of subscriptions per isolated sample.
/// - `asynchronous`: Whether to measure the async local provider.
///
/// # Returns
/// `Ok(())` after writing all seven CSV records.
///
/// # Errors
/// Returns child process I/O errors or an error when a warmup fails.
fn run(subscription_count: usize, asynchronous: bool) -> io::Result<()> {
    for _ in 0..WARMUPS {
        let (timed_out, fields) = run_isolated_sample(subscription_count, asynchronous)?;
        if timed_out || fields.split(',').next() != Some("success") {
            return Err(io::Error::other(format!(
                "warmup failed for {subscription_count} subscriptions"
            )));
        }
    }

    for iteration in 1..=SAMPLES {
        let (timed_out, fields) = run_isolated_sample(subscription_count, asynchronous)?;
        let mut fields = fields.split(',');
        let status = fields.next().unwrap_or("child_failed");
        let creation_ns = fields.next().unwrap_or("");
        let close_ns = fields.next().unwrap_or("");
        let baseline_threads = fields.next().unwrap_or("");
        let peak_threads = fields.next().unwrap_or("");
        let outcome = if timed_out { "timeout" } else { status };
        println!(
            "{},{subscription_count},{iteration},{outcome},{creation_ns},{close_ns},{baseline_threads},{peak_threads}",
            if asynchronous { "async" } else { "sync" }
        );
    }
    Ok(())
}

/// Dispatches child-sample mode or prints the documented seven-sample CSV.
fn main() {
    let args = env::args()
        .skip(1)
        .filter(|argument| argument != "--bench")
        .collect::<Vec<_>>();
    if args.as_slice() == ["--help"] {
        println!("usage: local_threads [--sample <subscription-count> [--async] | --routing]");
        return;
    }
    if args.first().is_some_and(|argument| argument == "--sample") {
        let Some(count) = args.get(1).and_then(|value| value.parse::<usize>().ok()) else {
            eprintln!("usage: local_threads --sample <subscription-count> [--async]");
            process::exit(2);
        };
        run_child_sample(count, args.iter().any(|argument| argument == "--async"));
        return;
    }
    if args.as_slice() == ["--routing"] {
        if let Err(error) = run_async_routing_probe() {
            eprintln!("async routing probe failed: {error}");
            process::exit(1);
        }
        return;
    }
    if !args.is_empty() {
        eprintln!("usage: local_threads");
        process::exit(2);
    }

    println!("mode,subscriptions,iteration,status,creation_ns,cancel_shutdown_ns,baseline_threads,peak_threads");
    for subscription_count in COUNTS {
        if let Err(error) = run(subscription_count, false).and_then(|()| run(subscription_count, true)) {
            eprintln!("thread-profile benchmark failed: {error}");
            process::exit(1);
        }
    }
    if let Err(error) = run_async_churn_probe() {
        eprintln!("async churn probe failed: {error}");
        process::exit(1);
    }
    if let Err(error) = run_async_routing_probe() {
        eprintln!("async routing probe failed: {error}");
        process::exit(1);
    }
}
