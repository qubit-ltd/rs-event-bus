# Synchronous facade capacity measurements

These local measurements compare the pre-change baseline with the 2026-10-07
retry-scheduler run. They describe resource shape on one machine, not
performance guarantees or portable capacity thresholds.

## Pre-change baseline: environment and commands

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

## Pre-change baseline: subscription creation and shutdown

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

## Pre-change baseline: retry saturation

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

This pre-change median exceeded the 800 ms assessment threshold: backoff held
all four synchronous handler execution slots. The baseline also showed one
receiver thread per synchronous subscription. These are historical observations,
not the behavior of the revised retry scheduler.

## 2026-10-07 run: environment and commands

- Local time: `2026-10-07T00:47:04+08:00` (environment capture).
- Machine: `Linux starfish-personal-os 7.0.0-34-generic x86_64`; 6 logical CPUs.
- Rust: `rustc 1.94.0 (4a4ef493e 2026-03-02)`.
- Benchmarks ran serially in the `rs-event-bus` worktree:

  ```sh
  cargo bench --offline --bench retry_saturation
  cargo bench --offline --bench local_threads
  ```

- Full raw output: `/tmp/superpowers-event-bus-7wcmlrsy/task-5-retry-saturation.log`
  and `/tmp/superpowers-event-bus-7wcmlrsy/task-5-local-threads.log`.
  Environment capture: `/tmp/superpowers-event-bus-7wcmlrsy/task-5-environment.txt`.

### Retry saturation after the scheduler change

The workload and four handler slots match the baseline. Two warmups were
discarded; all seven measured samples completed. The independent handler
started after the other four handlers entered a one-second retry backoff.

| Sample | Independent handler wait (ms) |
| ---: | ---: |
| 1 | 0.467011 |
| 2 | 0.121257 |
| 3 | 42.268486 |
| 4 | 39.823806 |
| 5 | 0.056125 |
| 6 | 38.327791 |
| 7 | 40.744051 |
| Median | **38.327791** |

The median is **161.672209 ms below** the new 200 ms decision threshold;
the prior median was 1,004.063 ms. This result supports the narrow conclusion
that retry backoff no longer occupies synchronous handler execution slots in
this workload. It does not establish general publish latency, consumer
throughput, or a subscription-count limit.

### Subscription threads and lifecycle after the change

`local_threads` again used two warmups and seven measured samples for each
mode/count. All 42 measurements succeeded. The lists below are samples 1–7 in
milliseconds, rounded to three decimals for display; medians use the raw
nanosecond values. Every sample began with one process thread.

| Mode | Subscriptions | Create samples (ms) | Create median (ms) | Cancel/shutdown samples (ms) | Shutdown median (ms) | Peak threads in each sample |
| --- | ---: | --- | ---: | --- | ---: | --- |
| Sync | 16 | 0.813, 1.452, 0.981, 0.870, 1.092, 0.981, 1.106 | 0.980850 | 651.761, 554.754, 401.022, 652.165, 451.475, 751.400, 601.192 | 601.192454 | 21, 21, 21, 21, 21, 21, 21 |
| Sync | 64 | 2.756, 3.501, 2.920, 2.760, 3.482, 9.877, 8.869 | 3.482352 | 2503.895, 2155.072, 1858.637, 2006.414, 2058.798, 1811.113, 1913.119 | 2006.413723 | 69, 69, 69, 69, 69, 69, 69 |
| Sync | 256 | 56.672, 18.288, 16.749, 16.241, 16.237, 17.753, 41.072 | 17.753002 | 7277.713, 7321.997, 7128.229, 8414.668, 7370.432, 7171.877, 7073.335 | 7277.713184 | 261, 261, 261, 261, 261, 261, 261 |
| Async | 16 | 0.148, 0.145, 0.137, 0.140, 0.134, 0.136, 0.140 | 0.139986 | 0.066, 0.070, 0.066, 0.066, 0.068, 0.066, 0.068 | 0.066494 | 1, 1, 1, 1, 1, 1, 1 |
| Async | 64 | 0.378, 0.388, 0.375, 0.501, 0.362, 0.372, 0.372 | 0.374852 | 0.240, 0.239, 0.243, 0.232, 0.228, 0.237, 0.233 | 0.237423 | 1, 1, 1, 1, 1, 1, 1 |
| Async | 256 | 1.648, 4.394, 1.314, 1.322, 1.325, 3.392, 4.626 | 1.648367 | 1.165, 0.977, 3.149, 1.111, 1.114, 0.997, 1.015 | 1.111065 | 1, 1, 1, 1, 1, 1, 1 |

The synchronous peak counts remain **21 / 69 / 261** at 16 / 64 / 256
subscriptions, the same as the baseline. Synchronous SPI receiver threads
therefore still grow linearly with subscription count; the retry change did not
remove them. The async local samples stayed at one thread in this benchmark.
Applications should measure their own workload and deployment environment.
