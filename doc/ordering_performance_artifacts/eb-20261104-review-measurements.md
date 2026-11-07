# EB-001 / EB-002 implementation measurements

## Environment and method

- Project source: `qubit-event-bus` at baseline commit `31effda34cf16bc6f8a43af9010854992d061062`.
- Host: x86_64 Linux 6.8.0-147-generic, dual Intel Xeon E5-2680 v4, 56 logical CPUs, 2.40 GHz nominal.
- Toolchain: rustc/cargo 1.94.0; locked dependencies; release profile for benchmarks.
- Timed matrix pinned to CPU 2 with `taskset`; 2 warmups and 7 independent accepted samples for each of 24 configurations. The benchmark rejected samples below its 98% timed CPU/wall gate; this run rejected 0 samples.
- `ordering_keys` exercised balanced, same-key and churn workloads at case sizes 4, 64, 1024 and 4096, with running/owned limits 4 and `max(case_size, 64)`. It reports observed concurrent handlers, so the high-key/limit-4 cases distinguish input key count from active admission. The public facade does not expose an internal locked-lane counter; that boundary is not directly measured.
- Middleware baseline and post-change runs used 500 deliveries per sample at 0, 1, 8 and 32 async/sync middleware layers, both pinned to CPU 2. Concurrent CI work on the host caused visible scheduling variance, so use these results as contextual comparison only.

## Commands and results

| Command | Exit | Result |
| --- | ---: | --- |
| `bash .infra/bin/align-ci.sh` | 0 | Repository style alignment completed before edits. |
| `bash .infra/bin/ci-check.sh --help` | 0 | Confirmed the current rs-infra CI workflow and options. |
| `cargo bench --locked --bench subscriber_middleware` (initial baseline) | 0 | Completed before fixed-affinity reruns; async medians were 15,616 / 13,115 / 16,071 / 27,025 ns per delivery at 0 / 1 / 8 / 32 layers. |
| `taskset -c 2 cargo bench --locked --bench subscriber_middleware` (baseline worktree at `31effda`) | 0 | Async medians were 26,480 / 27,147 / 29,960 / 40,495 ns per delivery at 0 / 1 / 8 / 32 layers. Raw samples are in `eb-20261104-middleware-baseline.txt`. |
| `taskset -c 2 cargo bench --locked --bench subscriber_middleware` (implementation worktree) | 0 | Async medians were 29,674 / 35,205 / 28,300 / 64,150 ns per delivery at 0 / 1 / 8 / 32 layers. Raw samples are in `eb-20261104-middleware-after.txt`. |
| `ORDERING_SMOKE=1 cargo bench --locked --bench ordering_keys` | 0 | Smoke invariants passed; smoke timings discarded as specified. |
| `cargo bench --locked --bench ordering_keys` | 0 | Full matrix completed before pinning; retained only as a preliminary run. |
| `taskset -c 2 cargo bench --locked --bench ordering_keys` | 0 | Final baseline; 24 configurations, 7 accepted samples each. Raw samples and per-case dispersion are in `eb-20261104-ordering-baseline.txt`. |
| `CARGO_TARGET_DIR=/tmp/eb-ordering-profile-target cargo +nightly-2026-06-05 rustc --locked --release --bench ordering_keys -- -Zinstrument-mcount=yes -Cdebuginfo=2 -Cstrip=none -Cforce-frame-pointers=yes -Clink-arg=-pg` | 0 | Built the debug-bearing optimized profile binary. |
| `ORDERING_PROFILE=1 ORDERING_PROFILE_OPERATIONS=262144 taskset -c 2 <profile-binary>` | 0 | Profile workload completed. Raw gmon data and flat/line reports are retained alongside this record. |
| `python3 -B scripts/check_documentation_examples.py` | 0 | Documentation examples, embedded sources and snapshot links match. |
| `python3 scripts/test_documentation_examples.py` | 0 | All 48 documentation checker cases passed. |
| `cargo test --locked --manifest-path tests/fixtures/documentation_consumer/Cargo.toml` | 0 | Consumer fixture tests and doctests passed. |
| `cargo test --locked --lib catch_async_panic -- --test-threads=1` | 0 | Three wrapper tests passed: one allocation, Pending/wake/Ready propagation and pending-cancellation drop. The allocation assertion was first run against baseline and failed with 2 allocations as expected. |
| `cargo test --locked --test subscriber_middleware_regression_tests` | 0 | All six middleware behavior regressions passed, including construction/poll panic isolation and chain ordering. |
| `cargo metadata --offline --all-features --format-version 1 --manifest-path Cargo.toml` in `rs-task` | 0 | Diagnosed and generated the required root lock update without network access. |
| `cargo metadata --offline --all-features --format-version 1 --manifest-path tests/fixtures/doc-examples/Cargo.toml` in the baseline `rs-task` checkout | 0 | Generated a second required lock update, changing the same Redis package's source from branch notation to its already-pinned exact revision. |
| The same offline fixture metadata command in the `rs-task` worktree with temporary sibling dependency links | 0 | Generated the fixture lock refresh; pre-existing sibling links were retained, and the one temporary Redis link was later removed. |
| `cargo metadata --offline --locked --all-features --format-version 1 --manifest-path tests/fixtures/doc-examples/Cargo.toml` in the `rs-task` implementation worktree | 0 | Verified the refreshed fixture lock is current. |
| `bash .infra/bin/ci-check.sh` (first full run before review-added wrapper tests) | 0 | Full configured CI passed. Coverage: functions 95.55%, lines 91.61%, regions 93.14%. |
| `bash .infra/bin/ci-check.sh` (after review fixes) | 0 | Full configured CI passed, including style, verify, workspace tests, feature matrix, Clippy, Rustdoc, strict docs, package verification, audit and project hook. Coverage thresholds passed: functions 95.51%, lines 91.63%, regions 93.15%. `coverage-cfg-clippy` was not configured and the CI runner reported it skipped. The isolated-process contract test intentionally printed a failing child case; its parent assertion passed and the target reported 4/4 tests passed. |
| `perf stat -e cycles,instructions taskset -c 2 true` | 1 | Host policy denied perf events (`perf_event_paranoid=4`); no sysctl or privilege changes attempted. The existing benchmark profile path with gprof succeeded. |

The pinned ordering baseline medians range from 6,556 to 9,238 ns per delivery across the matrix. Default 4-key / limit-4 medians (MAD) were:

| Workload | Median ns/delivery | MAD ns |
| --- | ---: | ---: |
| balanced | 8,394 | 46 |
| same-key | 8,413 | 21 |
| churn | 8,751 | 90 |

These figures are this host's baseline, not a cross-machine performance claim. Middleware throughput did not yield a reliable before/after conclusion: other repository CI jobs were active on the host, and the 32-layer async sample range expanded from 40,024–40,835 ns before to 36,759–65,812 ns after. No fixed throughput gain is claimed; the allocation probe is the direct acceptance measure.

## EB-002 decision

The profile completed 262,144 public-facade deliveries using one balanced high-key / limit-64 workload. It was not repeated across the full matrix or across subscription-count/locked-lane dimensions. The 0.01-second gprof sampling bins are coarse; this is attribution evidence, not a precise CPU percentage comparison. The flat profile's largest entries were formatting (5.05%), hashing (4.04%) and scheduler notifications (3.54%). The identified scheduler scans were small in this sample: `refresh_ready` 1.01%, the `eligible_lane` predicate 1.01%, and ready-queue selection 1.52%. The full elapsed matrix exercised balanced, same-key and churn workloads at high key counts under admission limit 4 and larger; it does not expose the exact internal locked-lane count. This evidence does not show a reproducible dominant scan hotspot or establish a candidate improvement above baseline variation. The matrix also has broad high-key dispersion relative to default cases.

**Decision: do not modify the scheduling core.** Complexity exists, but this evidence does not justify an index or membership-set change. The public-facade workload exercises backlogs from many distinct keys under admission limit 4, but cannot report the exact internal locked-lane count. No fairness or resource-release behavior was changed.

## EB-001 allocation probe

The focused unit probe creates the input `SpiFuture` before enabling a thread-local allocation counter, then measures only construction of the panic wrapper. At the baseline, it failed as expected with 2 allocations. After passing the existing pinned future directly into `CatchUnwindFuture`, it passed with exactly 1 allocation. Thread-local counting prevents allocations on concurrently running test threads from contaminating the measurement.

The first staged `git diff --cached --check` flagged trailing spaces in the gprof report's column headings. Those spaces were removed from the retained text reports; the repeated check passed. The binary gmon profile is unchanged.
