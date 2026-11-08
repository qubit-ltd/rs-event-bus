# EB-R02 Large Timeout Verification

Status: focused verification completed on the current Linux host. No production wait behavior has been changed; the only source additions outside tests are `cfg(test)` wait probes.

## Environment

- OS: Linux x86_64, Ubuntu kernel `6.8.0-147-generic`.
- Rust: `rustc 1.94.0 (4a4ef493e 2026-03-02)`.
- Integration test: `cargo test --locked --test large_timeout_tests` — final run after review fixes exited 0; 5 passed, 0 failed; test binary completed in 0.17 s.
- Probe-backed unit test: `cargo test --locked --lib large_timeout` — final run after review fixes exited 0; 4 passed, 0 failed. Each test loops over `Duration::MAX`, 100 years, and 3 seconds; each probe case also runs in its own child process with a 10-second watchdog.
- Standard library source inspected in the installed Rust 1.94 sysroot: Unix `pthread_cond_timedwait` computes a checked absolute `Timespec` and falls back to `TIMESPEC_MAX` if addition or conversion fails. This explains the current Unix behavior only; it is not used as evidence for untested targets.

## Coverage drafted

`tests/large_timeout_tests.rs` runs each potentially blocking scenario in a child test process with the existing `tests/support/isolated_process.rs` 10-second watchdog. Each scenario exercises `Duration::MAX`, 100 years, and 3 seconds:

- Local receive on an empty queue; publish wakes it, then the returned token is accepted and the provider is shut down.
- Topic-idle wait while a delivery is pending; accepting the delivery must return `WaitOutcome::Idle`.
- Graceful shutdown while an accepted delivery remains unsettled; accepting it must return `ShutdownOutcome::Complete`, and repeated shutdown must remain complete.
- Notification close while the provider's first publish is held behind a channel/condition-variable gate; releasing the provider must let close complete, drain the queued item, and allow repeated close.
- Zero-time polls and 40 ms exhaustion for receive, topic idle, graceful shutdown, and notification close.


The integration-test channel signals are sent immediately before invoking each wait-bearing call, so they do not claim to prove exact wait entry. Because integration tests cannot access library `cfg(test)` state, `src/tests/large_timeout_wait_tests.rs` supplies the exact wait-entry proof. Each of its four probe-backed scenarios loops over `Duration::MAX`, 100 years, and 3 seconds. A one-shot private `cfg(test)` signal fires at each Condvar call site while the associated mutex is still held. Only after receiving that signal does the test perform the operation that needs the same mutex: publish for receive, accept settlement for topic-idle and shutdown, or release the blocked provider for notification close. That lock dependency proves the wait path releases its mutex before the triggering operation can finish and notify it. Each probe case runs in a child test process with a 10-second kill-and-reap watchdog. The probe adds no public interface and is absent from non-test builds.

## Results and conclusions

The integration command had two initial failures, both caused by test code and corrected before the successful run:

1. First attempt: `cargo test --locked --test large_timeout_tests` exited 101 during compilation because four test call sites used nonexistent `InboundMessage::settlement_token()`. They were changed to the existing `settlement()` accessor.
2. Second attempt: the same command exited 101 after four scenarios passed. The finite/zero fixture tried settling a token after graceful shutdown had timed out; the implementation's existing timeout cleanup had already removed the in-flight token, so the fixture received `InvalidSettlementToken { reason: "unknown_token" }`. The fixture now ends that scenario after asserting the timeout.
3. Third attempt: the same command exited 0 with 5 passed, 0 failed. Runtime covered all four wait scenarios at `Duration::MAX`, 100 years, and 3 seconds, plus zero and 40 ms expiration for all four wait families.
4. Initial `cargo test --locked --lib large_timeout` exited 0 with 4 passed, 0 failed, but review found that its exact wait-entry probe covered only `Duration::MAX`; integration handshakes for 100 years and 3 seconds proved only that those workers were about to call the wait.
5. Final review fix parameterized all four probe-backed unit tests over the three timeout values and enqueued a second notification while the first provider call was blocked. The integration test now asserts both queued notifications publish after release.
6. Final `cargo test --locked --test large_timeout_tests` exited 0 with 5 passed, 0 failed; `cargo test --locked --lib large_timeout` exited 0 with 4 passed, 0 failed. Exact wait-entry synchronization now covers all three durations in each of the four paths, and notification close proves it drains the queued second item.

- **Verified on this host:** Linux x86_64, Rust 1.94.0. All tested waits accepted the extreme budgets, returned after the explicit triggering operation, and cleanup assertions passed. Exact probe coverage verifies `Duration::MAX`, 100 years, and 3 seconds at each of the four wait sites. Zero and ordinary finite budgets expired as expected. The topic-idle probe also verified capacity was reusable after Accept settlement, and notification close drained a second queued item.
- **Not reproduced:** no EB-R02 contract violation was reproduced on this Linux/Rust combination across these four paths.
- **Not verified:** other operating systems and architectures. In particular, results on macOS or 32-bit targets must not be inferred from Linux or from the local standard-library implementation.
- **Static checks:** `rustfmt --edition 2024 --check` passed for the changed Rust files (stable toolchain warned that two repository rustfmt options require nightly); `git diff --check` passed for the T3 files.

No production wait fix is supported by the current evidence. T4 should record the Linux result as not reproduced and retain other platforms as unverified unless additional platform runs produce evidence.

## T5 integrated validation

- `./.infra/bin/align-ci.sh`: first run exited 1 because three new Rust files lacked the repository's complete license header; after adding the header, the next run exited 1 on Clippy findings (`manual_div_ceil`, two `needless_range_loop` instances, `type_complexity`, and `collapsible_if`). Those issues were corrected without suppressing lints. The final run exited 0, including style alignment and Clippy.
- `./.infra/bin/ci-check.sh`: exited 0. Workspace tests, four feature-matrix configurations, doctests, strict documentation, README checks, package verification, coverage, and audit passed. Coverage was 95.21% functions, 92.08% lines, and 93.31% regions. The configured CI explicitly reports `coverage-cfg-clippy: not configured; skipping`.

## T2 platform verification (2026-11-08)

This entry records a new Linux run and a separate read-only check of platform evidence. It does not supersede or rewrite the historical entries above.

### Linux run

- Baseline commit: `ecf2d79a897a8d8db8fc3160305a90522d7ca9b7` (`test(notification): cover detailed reservation recovery`).
- Environment: Linux x86_64; `Linux mas-server 6.8.0-147-generic #147-Ubuntu SMP PREEMPT_DYNAMIC Thu Sep 24 16:12:16 UTC 2026 x86_64 x86_64 x86_64 GNU/Linux`.
- `rustc -Vv` output:

  ```text
  rustc 1.94.0 (4a4ef493e 2026-03-02)
  binary: rustc
  commit-hash: 4a4ef493e3a1488c6e321570238084b38948f6db
  commit-date: 2026-03-02
  host: x86_64-unknown-linux-gnu
  release: 1.94.0
  LLVM version: 21.1.8
  ```

- Verification log location: command output is retained in the current Paseo task transcript; no separate log file or CI artifact was produced for this local run.
- `cargo +1.94.0 test --locked --test large_timeout_tests`: exit code 0; 5 passed, 0 failed; test binary completed in 0.17 s. This covers the four wait scenarios and the zero/finite exhaustion case using the existing isolated-process watchdogs.
- `cargo +1.94.0 test --locked --lib large_timeout`: exit code 0; 4 passed, 0 failed; four exact wait-entry probe tests completed in 0.01 s. Each probe covers `Duration::MAX`, 100 years, and 3 seconds.
- No production wait conversion defect was reproduced on this Linux host. No production code or test was changed for T2.

### macOS evidence and status

- The project workflow initially called pinned shared workflow `qubit-ltd/rs-infra-ci/.github/workflows/github-ci.yml@22e34b76a52b6a9c5c09ddf3fa1fa58b07c46f06` and did not override its `verify-os` input. Its default verify runner was `ubuntu-latest`; the shared workflow also defined macOS 14 Miri and AddressSanitizer infrastructure jobs, which are not the two large-timeout test commands.
- GitHub Actions run [42440479860](https://github.com/qubit-ltd/rs-event-bus/actions/runs/42440479860) is for this baseline commit. Its Linux verify job succeeded; its `miri on macos-14` and `address-sanitizer on macos-14` jobs were cancelled. Recent earlier runs inspected likewise had only cancelled macOS infrastructure jobs. No trustworthy macOS execution log for either large-timeout command was found.
- The current execution host is Linux and no local macOS runner is available. The first focused workflow dispatch, [run 42493466399](https://github.com/qubit-ltd/rs-event-bus/actions/runs/42493466399), failed before runner setup: GitHub rejected `macos-14-arm64-us-default` as unsupported, so neither test command ran. GitHub's runner image project records macOS 14 retirement for GitHub Actions on November 2, 2026 ([retirement notice](https://github.com/actions/runner-images/issues/13518)). The job now uses `macos-15`. It checks the centralized infrastructure snapshot, installs Rust 1.94.0 with `rust-src` via the pinned toolchain action, prepares local path dependencies, records `rustc +1.94.0 -Vv`, then runs the two commands above. The job has a 15-minute timeout, read-only repository permissions, and uses the repository's pinned checkout and Rust toolchain action revisions.
- Workflow dispatch [run 42494236486](https://github.com/qubit-ltd/rs-event-bus/actions/runs/42494236486) completed the focused job successfully on macOS 15 arm64 (runner image `20261104.0514.1`, Rust `1.94.0`, host `aarch64-apple-darwin`). `cargo +1.94.0 test --locked --test large_timeout_tests` exited 0 with 5 passed; `cargo +1.94.0 test --locked --lib large_timeout` exited 0 with 4 passed. No large-timeout defect was reproduced, so no production wait logic changed.
- The overall run was cancelled after its older shared-workflow pin attempted to schedule Miri and AddressSanitizer infrastructure suites on retired macOS 14 labels; both failed before execution, while a Loom suite was still running. This was a runner configuration failure, not a test failure. To keep all existing checks enabled and runnable, the project pin was updated to `rs-infra-ci` revision `2c965cad450e9f0e4d01cf1e5985e6d5e11e75b4`. Comparing the pinned workflow inputs found no contract change; the revision adds the project toolchain's Clippy component and moves the existing Miri/AddressSanitizer matrix to macOS 26 arm64 and Intel labels. The focused large-timeout tests remain on `macos-15`.
- **EB-REV-002 verified on Linux x86_64 and macOS 15 arm64:** neither platform reproduced the suspected large-timeout defect. No production wait fix is supported by the evidence.

### Final CI rerun (2026-11-08)

- Workflow dispatch [run 42495605806](https://github.com/qubit-ltd/rs-event-bus/actions/runs/42495605806) at commit `0fa397dd63c73390a0de9cbd860cfec01b1493c9` completed successfully after rerunning one failed job.
- macOS 15 arm64 runner image: `20261104.0514.1`; OS: macOS 15. Rust compiler output:

  ```text
  rustc 1.94.0 (4a4ef493e 2026-03-02)
  binary: rustc
  commit-hash: 4a4ef493e3a1488c6e321570238084b38948f6db
  commit-date: 2026-03-02
  host: aarch64-apple-darwin
  release: 1.94.0
  LLVM version: 21.1.8
  ```

- `cargo +1.94.0 test --locked --test large_timeout_tests`: exit code 0; 5 passed, 0 failed.
- `cargo +1.94.0 test --locked --lib large_timeout`: exit code 0; 4 passed, 0 failed.
- The first complete run's macOS 26 Intel AddressSanitizer job could not install Rust because `static.rust-lang.org` DNS resolution failed. Rerunning that failed job succeeded, and the complete workflow then concluded success. This was a transient runner network failure; no repository change was needed to address it.
- The updated shared workflow's existing macOS 26 arm64 and Intel Miri/AddressSanitizer suites, Linux verification, feature matrix, audit, and Loom checks all completed successfully in the final run. No verification gate was skipped or weakened by this update.
- **Final EB-REV-002 result:** Linux x86_64 and macOS 15 arm64 both passed all large-timeout scenarios; the suspected defect was not reproduced. No production wait fix was made.
