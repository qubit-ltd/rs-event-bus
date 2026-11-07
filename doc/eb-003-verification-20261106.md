# EB-003 Async shutdown session retention verification

## Question

After an asynchronous receiver close fails, does successful provider shutdown
release the receiver and handler session if the caller retains the subscription
handle, while preserving the close error and handle observations?

## Evidence

- Before the lifecycle change, `cargo test --locked --test async_coverage_tests
  shutdown_disposes_failed_close_session_after_provider_termination` failed at
  the receiver drop assertion: expected one receiver destruction, observed zero.
- The fixture first polls and pauses a runner with a retained handler capture,
  then injects a receiver close failure and lets provider shutdown return
  `ShutdownOutcome::Complete`. It keeps the public handle alive, checks the
  canonical close error, receiver and capture drop counts, handle metrics/run
  state, and repeated shutdown behavior.
- The test fixture also checks provider shutdown calls. A cancelled provider
  shutdown attempt leaves the session intact for retry; disposal is reached
  only after the provider shutdown future succeeds.
- The unsettled-delivery regression gives one subscription two messages with
  one handler slot. One handler remains controlled and pending while the second
  delivery is held by facade admission. After shutdown begins, releasing the
  first handler permits exactly its one settlement; the buffered delivery's
  token is never settled. This is checked for ephemeral and durable providers.
- The Redis async receiver's `close` implementation marks it closed and takes
  its dedicated receive connection and receiver permit. Its SPI documentation
  says close/drop releases receiver admission; it issues no Redis command and
  does not implicitly acknowledge pending records. Redis provider shutdown
  returns `ShutdownOutcome::Complete`. This supports dropping the receiver as
  a local resource fallback after provider termination, without claiming that
  an asynchronous broker close completed.

## Conclusion

There was unnecessary local session retention after terminal provider shutdown.
The minimal fix disposes the remaining controls only after runner cleanup and
successful provider shutdown. The existing control disposal protocol preserves
the public handle's run state, metrics, terminal failure, and canonical close
error. It drops receiver/session ownership outside the bus controls map lock.
No public API or broker settlement semantics change, and no durable delivery
is acknowledged by this disposal.

## Verification

| Command | Result |
| --- | --- |
| `./.infra/bin/align-ci.sh` | Passed after formatting and Clippy checks. |
| `./.infra/bin/ci-check.sh` | Passed (exit 0), including workspace tests, feature matrix, docs, package, coverage, and audit. One earlier run exposed a test timing/observation issue; the deterministic single-subscription test was corrected and the complete check rerun successfully. |
| `timeout 30s cargo test --locked --test async_coverage_tests terminal_session_disposal_preserves_unsettled_delivery_semantics` | Passed (1 test). |
| `cargo test --locked --test async_coverage_tests` | Passed (29 tests) before the final equivalent deterministic test arrangement; the complete CI run above includes the final arrangement. |
| `python3 -B -m unittest discover -s scripts/tests -p '*tests.py'` | Passed (54 tests), including dirty-layout gates and no-Cargo/no-Docker behavior. |
| `python3 -B scripts/check_documentation_examples.py` | Passed. |
| `python3 -B scripts/test_documentation_examples.py` | Passed (48 cases). |
| `python3 -B scripts/check_documentation_examples.py --check-snapshot-content` | Passed after committing the bilingual docs and updating all four links to snapshot commit `cd1ad429d2d96d7905215d4c37577431688a786d`. An initial abbreviated SHA was rejected by the checker and corrected to the full SHA. |
`coverage-cfg-clippy` was skipped by the repository CI configuration because
`coverage_cfg_clippy` is disabled there.
