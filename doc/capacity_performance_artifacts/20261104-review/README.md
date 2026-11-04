# 2026-11-04 review measurements

This archive records the current-version capacity benchmarks and the async waiter A/B evaluation. It keeps source identity, raw process output, environment details, and checksums together. The dates in the directory name identify the review; use the UTC timestamp files and manifest for actual run times.

## Commands and outcomes

Run from the repository root with the recorded Rust toolchain and lockfile:

- `cargo bench --locked --bench local_threads` — exit code 0; two warmups and seven samples per mode/count.
- `cargo bench --locked --bench retry_saturation` — exit code 0; two warmups and seven measured samples.
- `cargo bench --locked --bench async_receive_waiter` — exit code 0 for the baseline and candidate builds. Each version uses two warmups and seven samples for same-waker repeated poll, alternating-waker repeated poll, and publish/receive/settle.

Complete stdout and stderr are retained beside each exit-code file. The waiter paired directory contains seven alternating baseline/candidate process pairs. Each run kept its seven raw samples; no sample was removed. A run's waker callback count is printed with each scenario. `async-waiter/run_interleaved.sh` reruns the saved benchmark executables and writes full stdout/stderr for all rounds.

## Reconstructing measured source

The baseline waiter driver is stored in `async-waiter/baseline/async_receive_waiter.rs`; its Cargo manifest and lockfile are stored beside it. Its production code is the baseline commit recorded in `HEAD.txt` in that directory. The candidate starts from `async-waiter/candidate/base-HEAD.txt`; apply `candidate/source.patch`, then add `candidate/untracked-bench-source.rs` as `benches/async_receive_waiter.rs`. Candidate Cargo.toml and Cargo.lock are also archived. `tracked-source.patch` captures all tracked task changes against the repository HEAD used for this measurement archive.

The source patches intentionally contain only task changes. The lockfile was unchanged. The design and implementation plan documents are not part of either patch.

## Summary method

- `local_threads`: median of the seven measured raw nanosecond values for creation and cancel/shutdown; failures remain in the raw output and are not counted as successful samples. Thread counts use Linux `/proc/self/task`; unsupported platforms report no thread metric.
- `retry_saturation`: use the seven emitted wait values and the benchmark's reported median.
- `async_receive_waiter`: pool all 49 raw samples per variant/scenario from the seven interleaved runs. Report median and median absolute deviation (MAD). Percent change is `(candidate median / baseline median - 1) × 100`; negative is faster. The acceptance rule is greater than 5% improvement and greater than the larger variant MAD for repeated poll, while publish/receive/settle must not regress by more than 5% beyond dispersion.

## Environment and limits

The captured host is Linux x86_64 on an Intel Xeon E5-2680 v4 with 56 visible logical CPUs. Rust is 1.94.0 and Cargo is 1.94.0. The host was busy: captured load averages ranged around 20–25 while other builds ran. Timing values are observations of this host and harness, not throughput promises. Interpret high-level tails with the raw samples and environment capture. No credentials or complete environment-variable dump is included.
