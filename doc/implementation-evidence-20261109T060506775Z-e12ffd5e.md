# Implementation evidence: REB-001, REB-002, REB-003

Repository: `/data/working/qubit/rust-common/rs-event-bus`

Plan: `doc/implementation-plan-20261109T060506775Z-e12ffd5e.md`

Base: `97556d9` (`dev-starfish` at task start)
Integration head before this evidence file: `8426b03`

## Scope results

- **REB-001:** Restored lightweight ordering and capacity evidence navigation pages pinned to `e8a1ed724954756d48c3d5216cbd13ac39a0cb81`. Added real-checkout checks. No original benchmark artifacts were restored; package exclusions remain enforced.
- **REB-002:** Restored bilingual migration entry pages, added README links, and extended the offline documentation/package contract. Redis's existing core migration URLs resolve to these paths. Historical codec registration and payload limit transitions are version-scoped and linked to the current 0.20-to-0.21 guide sections.
- **REB-003:** Preserved `sync` and `async`; added `sync-direct-shutdown`, lifecycle/routing/failure tests, a separate performance table, and 63 raw measurement rows with environment and source provenance in `doc/local-shutdown-measurements-20261109.md`. The optimized run had 63/63 successful measured samples and no timeout/failure samples. No production cancellation behavior or public API changed.

## Validation record

All commands below ran from the integration worktree at the explicit target project checkout. Exit codes are recorded as observed.

| Command | Exit | Result |
| --- | ---: | --- |
| `python3 -B -m unittest discover -s scripts/tests -p performance_evidence_tests.py` | 0 | 12 tests passed |
| `python3 -B -m unittest discover -s scripts/tests -p documentation_contract_tests.py` | 0 | 10 tests passed; expected negative-case diagnostics emitted by test fixtures |
| `cargo +1.94.0 test --locked --test local_threads_batch_tests` | 0 | 9 tests passed |
| `cargo +1.94.0 bench --locked --bench local_threads --no-run` | 0 | Optimized benchmark compiled |
| `cargo +1.94.0 bench --locked --bench local_threads` | 0 | 63/63 measured samples passed; recorded in the measurement document |
| `.infra/bin/align-ci.sh` | 1, then 0 | First run identified 5 test names violating repository `test_*` style; renamed tests and reran successfully |
| `.infra/bin/style-check.sh` | 0 | Mechanical style checks passed after formatting and naming correction |
| `python3 -B scripts/check_documentation_examples.py` | 0 | Documentation examples and links passed |
| `python3 -B scripts/test_documentation_examples.py` | 0 | 51 regression cases passed |
| `cargo +1.94.0 test --locked --manifest-path tests/fixtures/documentation_consumer/Cargo.toml` | 0 | Consumer tests and doctests passed |
| `cargo +1.94.0 package --list --locked --allow-dirty` | 0 | Real package listing generated at `/tmp/rs-event-bus-package-list-final-20261109.txt` |
| `python3 -B scripts/check_performance_evidence.py --package-list /tmp/rs-event-bus-package-list-final-20261109.txt` | 0 | Archive/package contract passed |
| `python3 -B scripts/check_documentation_contract.py --package-list /tmp/rs-event-bus-package-list-final-20261109.txt` | 0 | Migration/package/link contract passed |
| `python3 -B scripts/check_documentation_examples.py --check-snapshot-content --fetch-snapshot` | 0 | All four pinned documents match the fixed snapshot |
| `cargo +1.94.0 run --locked --example local_delivery` | 0 | Example ran |
| `cargo +1.94.0 test --locked --no-run --features conformance --test conformance_watchdog_tests` | 0 | Watchdog test binary built |
| `python3 scripts/run_conformance_watchdog.py --timeout-seconds 30 --label conformance-watchdog -- target/debug/deps/conformance_watchdog_tests-d462c891cc750b4a --exact test_external_watchdog_bounds_all_runner_stuck_modes_and_observes_entry --nocapture --test-threads=1` | 0 | 1 watchdog test passed |
| `cargo +1.94.0 test --locked --features conformance --test spi_conformance_tests` | 0 | 45 tests passed |
| `cargo +1.94.0 run --locked --manifest-path tests/fixtures/documentation_consumer/Cargo.toml --bin async_local` | 0 | Async consumer example ran |
| `cargo +1.94.0 run --locked --manifest-path tests/fixtures/discovery_consumer/Cargo.toml` | 0 | Discovery consumer ran |
| `bash .infra/bin/ci-check.sh` | 0 | Full configured CI passed, including style, Clippy, verify, strict docs, README, release build, feature matrix, project hook, package, coverage and audit |
| `git diff --check` | 0 | No whitespace errors |

Full CI coverage: functions 95.28% (1,937/2,033), lines 92.15% (16,978/18,425), regions 93.31% (25,367/27,187). The configured thresholds were satisfied. Audit scanned 65 locked dependencies and reported no vulnerability failure. Shared Cargo and advisory database locks caused waits during execution; checks completed successfully.

## Baseline correction

The workflow snapshot-content check initially found that the four fixed document links in `src/lib.rs` referenced `b078cdf`, while the bilingual user guides had later existing changes from `9a24b09fa25dd96289b6e9008f0ad486bbd1ef10`. The latter commit is present in the remote branch history and its four guide/design files exactly match the current checkout. Updated the Rustdoc links to that verified commit; both snapshot-content validation and its 51-case regression suite then passed.

## Git handling

Task commits: `c74ed0b` (REB-001), `83e14d5` (REB-002), `c77dce4` (REB-003), and `8426b03` (snapshot/style integration fixes). Source design and plan documents were not included. Original checkout lockfile modifications, performance archive deletions, and those two untracked source documents remain untouched.
