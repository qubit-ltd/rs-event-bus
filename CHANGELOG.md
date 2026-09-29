# Changelog

## 0.18.0 (prepared; unpublished)

- Add `EventBus::request_shutdown(ShutdownMode) -> EventBusShutdown` for a
  nonblocking shutdown request and generation-bound result observation.
- Add reusable `EventBusShutdown::wait(Option<Duration>)` and runtime-neutral
  `wait_async()`. Cancelling a wait or dropping its ticket does not cancel the
  background shutdown; a later wait on a retained ticket observes the same
  generation. An Immediate request may strengthen a running Graceful attempt.
- Keep synchronous `EventBus::shutdown` as request plus blocking wait, including
  Immediate mode. Use request/ticket in managed abort and rollback callbacks.
- Document the 0.17-to-0.18 migration and the current IoC 0.3 consumer. The
  historical pinned EventBus 0.15 lane does not gain the new behavior.

No release, tag, or final external source SHA is recorded here; those require
actual versioned commits and publishing steps outside this documentation change.
