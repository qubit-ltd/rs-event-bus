# Synchronous facade capacity measurements

These measurements describe one local run. They are evidence for the current
synchronous scheduler's resource shape, not performance guarantees or portable
capacity thresholds.

## Environment and commands

- Rust: `rustc 1.94.0 (4a4ef493e 2026-03-02)`
- `uname -a`: `Linux starfish-personal-os 7.0.0-34-generic #34~24.04.1-Ubuntu SMP PREEMPT_DYNAMIC Fri Sep 4 15:38:29 UTC 2026 x86_64 x86_64 x86_64 GNU/Linux`
- Logical CPUs: `6`
- Thread measurements use Linux `/proc/self/task`.
- Commands:

  ```sh
  cargo bench --bench local_threads
  cargo bench --bench retry_saturation
  ```

- Complete command output, including every sample, is stored in the temporary
  review workspace:
  - `/tmp/superpowers-rs-event-bus-9971szrw/local_threads-baseline.txt`
  - `/tmp/superpowers-rs-event-bus-9971szrw/retry_saturation-baseline.txt`

## Subscription creation and shutdown

`local_threads` used two warmups and seven measured samples for each count. All
seven samples succeeded at 16, 64, and 256 subscriptions in both modes. Times
below are medians across those seven samples. Peak thread counts are the
high-water marks observed during each sample; each sample started with one
process thread.

| Mode | Subscriptions | Create median (ms) | Cancel and shutdown median (ms) | Peak threads (all samples) |
| --- | ---: | ---: | ---: | ---: |
| Sync | 16 | 7.966 | 457.303 | 21 |
| Sync | 64 | 25.476 | 1,680.067 | 69 |
| Sync | 256 | 113.681 | 7,055.459 | 261 |
| Async | 16 | 0.154 | 0.070 | 1 |
| Async | 64 | 0.432 | 0.245 | 1 |
| Async | 256 | 1.415 | 1.142 | 1 |

The synchronous process thread high-water mark grew with subscription count in
this run. The async samples used one process thread throughout, including
subscription creation and shutdown. These figures are observations of this
machine and this benchmark implementation; they do not establish a universal
subscription limit.

## Retry saturation

`retry_saturation` used four synchronous handler slots and the local provider.
Four independent handlers each failed their first attempt, synchronized at a
barrier, and retried after a fixed one-second backoff. Ten milliseconds after
releasing the barrier, the benchmark published to a separate topic and measured
until that handler began. Each sample created and shut down a fresh bus; two
warmups were discarded and all seven measured samples completed within the
five-second receive timeout.

| Sample | Independent handler wait (ms) |
| ---: | ---: |
| 1 | 995.104 |
| 2 | 1,062.418 |
| 3 | 990.063 |
| 4 | 1,004.063 |
| 5 | 1,021.555 |
| 6 | 997.781 |
| 7 | 1,027.879 |
| Median | 1,004.063 |

This median exceeds the 800 ms decision threshold used for this assessment. A
workload with long synchronous retry delays should use the async facade or move
retry scheduling to a provider requeue mechanism so a sleeping retry does not
hold a synchronous handler slot. A workload with many synchronous subscriptions
should also account for the observed per-subscription thread growth. Validate
the choice against the application's own workload and deployment environment;
these measurements do not set cross-machine thresholds.
