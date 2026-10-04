# Changelog

## 0.20.0 (unreleased)

This entry describes the current source version; it does not claim publication
or a release tag.

- Add optional declared native-payload weight accounting to both local
  providers. When enabled, each published native payload type must supply a
  positive weight; each admitted fanout copy reserves that amount until
  settlement or cleanup. The estimate is not a process-memory measurement.
- Reject asynchronous local subscription-ID exhaustion before registration.
- Preserve a local delivery when settlement-token allocation is exhausted and
  return a structured receive error instead of losing the queued event.
- Check `publish_checked` visibility requirements before interceptors, codec
  callbacks, metrics, or provider publication when a provider cannot expose
  destination admission.
- Coordinate with Redis provider 0.7 and Task 0.10. See the 0.19-to-0.20
  migration guide for opt-in weight-budget configuration and checked-publish
  behavior.

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

The notes above describe the historical 0.18.0 preparation only.
