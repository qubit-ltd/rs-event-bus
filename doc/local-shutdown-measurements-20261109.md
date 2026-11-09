# Local shutdown benchmark measurements (2026-11-09)

## Scope and interpretation

This records `local_threads` results for the existing synchronous cancel-then-shutdown scenario (`sync`), existing async receive/shutdown scenario (`async`), and added synchronous direct-shutdown scenario (`sync-direct-shutdown`). Each quantity used 2 discarded warmups followed by 7 measured child processes per scenario. The measured command completed with 63/63 successful rows, zero failed samples, and no watchdog timeouts. Timing values are nanoseconds. The new `shutdown_only_ns` rows measure only `EventBus::shutdown(Immediate)` while all subscription handles remain alive and uncancelled until it returns; the harness then checks their cancelled terminal state outside the timed interval.

The legacy `sync` values still include individual cancellation plus shutdown. Async values still include receive-runner termination with shutdown. The values are therefore reported in separate scenario and `duration_kind` groups.

## Reproduction and provenance

- Command: `cargo +1.94.0 bench --locked --bench local_threads`
- Run date: 2026-11-09 UTC.
- Target source base HEAD: `97556d9fd1d9758db9462efc38f5e31cc47cb448`.
- Measured source patch SHA-256 (`git diff --binary -- benches/local_threads.rs benches/local_threads_support/sample_batch_tests.rs`): `06ab85cf9eaab283cf289efb785c2fb68e43d9b617f4149d1e2970f00b5bec39`.
- Measured benchmark source SHA-256: `4e22d5f7b562fd6b96bff8b11c2319f5bf2f00540cc20596850adf6c7d5f0dca`.
- Test/support source SHA-256: `0ed665819efff5ee30390de8f85324252bc429a78e3a0fa329cc26903123f785`.
- `Cargo.lock` SHA-256: `07b21e4f6f5fa2bd1f51fb4e08bf5dcc243dc878f30213ecdd6a71e1affce8f6`; lockfile was unchanged from base.
- Rust: `rustc 1.94.0 (4a4ef493e 2026-03-02)`, host `x86_64-unknown-linux-gnu`; Cargo `1.94.0 (85eff7c80 2026-01-15)`.
- Host: Linux `6.8.0-147-generic`, x86_64; Intel Xeon E5-2680 v4 at 2.40 GHz, 2 sockets × 14 cores × 2 threads (56 online CPUs), 70 MiB L3 total. CPU scaling range reported by the host was 1.2–3.3 GHz. `perf_event_paranoid=4`.
- Build: Cargo optimized bench profile, locked dependencies, no profile or RUSTFLAGS overrides. This run did not pin CPU affinity or isolate the host from unrelated load; timing variability must be interpreted accordingly.
- Each sample is a fresh child process with the existing 30-second watchdog. Thread counts come from `/proc/self/task` on this Linux host; baseline and peak include the full sample process, including provider worker threads.
- Full command output SHA-256: `7b0d4976a18d04c02d645e91ee2cffe0e51a39cdb812deacd1bd92f7b137dfd9`. The committed raw measured rows and trailing probe summaries are below; Cargo build progress lines are excluded from the raw-data block.

## Independent per-scenario summary

Median is the middle of the seven measured `duration_ns` values, grouped by both scenario and quantity. Min/max show observed sample spread, not confidence intervals.

| Scenario | Subscriptions | Duration kind | Samples | Median (ns) | Min–max (ns) |
| --- | ---: | --- | ---: | ---: | ---: |
| sync | 16 | `cancel_shutdown_ns` | 7 | 551,453,825 | 301,030,715–651,373,343 |
| sync | 64 | `cancel_shutdown_ns` | 7 | 2,356,283,189 | 1,853,223,293–2,754,200,312 |
| sync | 256 | `cancel_shutdown_ns` | 7 | 7,110,563,686 | 6,715,191,796–8,112,638,976 |
| sync-direct-shutdown | 16 | `shutdown_only_ns` | 7 | 50,272,303 | 50,210,285–50,327,010 |
| sync-direct-shutdown | 64 | `shutdown_only_ns` | 7 | 50,861,210 | 50,678,198–51,063,523 |
| sync-direct-shutdown | 256 | `shutdown_only_ns` | 7 | 53,402,074 | 52,768,147–54,589,782 |
| async | 16 | `async_receive_shutdown_ns` | 7 | 118,389 | 112,378–128,666 |
| async | 64 | `async_receive_shutdown_ns` | 7 | 408,070 | 310,842–446,762 |
| async | 256 | `async_receive_shutdown_ns` | 7 | 1,267,206 | 1,229,499–1,612,982 |

The direct-shutdown samples establish a distinct, repeatable idle-subscription shutdown-only measurement on this host. The much longer legacy sync values include cancellation work and cannot identify its cause from this benchmark alone. This evidence does not establish a cancellation-wakeup defect or justify a production cancellation/API change; follow-up would first need causal profiling and repeated measurements under controlled CPU/load conditions.

## Raw measured rows and probe summaries

Columns: `mode,subscriptions,iteration,status,creation_ns,duration_ns,duration_kind,baseline_threads,peak_threads`. All 63 measured rows are retained verbatim.

```text
sync,16,1,success,1871717,351023302,cancel_shutdown_ns,1,21
sync,16,2,success,2111987,451036647,cancel_shutdown_ns,1,21
sync,16,3,success,2196058,301030715,cancel_shutdown_ns,1,21
sync,16,4,success,2070814,601434893,cancel_shutdown_ns,1,21
sync,16,5,success,2032097,651373343,cancel_shutdown_ns,1,21
sync,16,6,success,2199715,601209124,cancel_shutdown_ns,1,21
sync,16,7,success,2051733,551453825,cancel_shutdown_ns,1,21
sync-direct-shutdown,16,1,success,2008645,50216442,shutdown_only_ns,1,21
sync-direct-shutdown,16,2,success,2214795,50272303,shutdown_only_ns,1,21
sync-direct-shutdown,16,3,success,2137469,50277232,shutdown_only_ns,1,21
sync-direct-shutdown,16,4,success,2130507,50327010,shutdown_only_ns,1,21
sync-direct-shutdown,16,5,success,2199196,50274725,shutdown_only_ns,1,21
sync-direct-shutdown,16,6,success,2419954,50266304,shutdown_only_ns,1,21
sync-direct-shutdown,16,7,success,2430798,50210285,shutdown_only_ns,1,21
async,16,1,success,332515,127122,async_receive_shutdown_ns,1,1
async,16,2,success,340566,117044,async_receive_shutdown_ns,1,1
async,16,3,success,402571,128666,async_receive_shutdown_ns,1,1
async,16,4,success,352626,123273,async_receive_shutdown_ns,1,1
async,16,5,success,368103,118389,async_receive_shutdown_ns,1,1
async,16,6,success,351486,112378,async_receive_shutdown_ns,1,1
async,16,7,success,369019,117751,async_receive_shutdown_ns,1,1
sync,64,1,success,6021181,2754200312,cancel_shutdown_ns,1,69
sync,64,2,success,7462188,1853698613,cancel_shutdown_ns,1,69
sync,64,3,success,6197187,2403763795,cancel_shutdown_ns,1,69
sync,64,4,success,5979132,2356283189,cancel_shutdown_ns,1,69
sync,64,5,success,6965144,2403628644,cancel_shutdown_ns,1,69
sync,64,6,success,7537341,1853223293,cancel_shutdown_ns,1,69
sync,64,7,success,6993153,2203690622,cancel_shutdown_ns,1,69
sync-direct-shutdown,64,1,success,6729852,50678198,shutdown_only_ns,1,69
sync-direct-shutdown,64,2,success,5703194,50959704,shutdown_only_ns,1,69
sync-direct-shutdown,64,3,success,7101936,50688635,shutdown_only_ns,1,69
sync-direct-shutdown,64,4,success,5776941,50761794,shutdown_only_ns,1,69
sync-direct-shutdown,64,5,success,6918452,51063523,shutdown_only_ns,1,69
sync-direct-shutdown,64,6,success,5949375,50861210,shutdown_only_ns,1,69
sync-direct-shutdown,64,7,success,5555599,50971826,shutdown_only_ns,1,69
async,64,1,success,923264,408070,async_receive_shutdown_ns,1,1
async,64,2,success,1026325,430740,async_receive_shutdown_ns,1,1
async,64,3,success,814892,331190,async_receive_shutdown_ns,1,1
async,64,4,success,1001840,446762,async_receive_shutdown_ns,1,1
async,64,5,success,1001496,438033,async_receive_shutdown_ns,1,1
async,64,6,success,808122,310842,async_receive_shutdown_ns,1,1
async,64,7,success,732759,311921,async_receive_shutdown_ns,1,1
sync,256,1,success,42165726,6715191796,cancel_shutdown_ns,1,261
sync,256,2,success,30833435,8020242369,cancel_shutdown_ns,1,261
sync,256,3,success,81377008,6909455681,cancel_shutdown_ns,1,261
sync,256,4,success,29234856,7110563686,cancel_shutdown_ns,1,261
sync,256,5,success,33364334,7065112263,cancel_shutdown_ns,1,261
sync,256,6,success,29945104,7812290804,cancel_shutdown_ns,1,261
sync,256,7,success,31228295,8112638976,cancel_shutdown_ns,1,261
sync-direct-shutdown,256,1,success,31027916,52887098,shutdown_only_ns,1,261
sync-direct-shutdown,256,2,success,30982425,52768147,shutdown_only_ns,1,261
sync-direct-shutdown,256,3,success,34999447,53917560,shutdown_only_ns,1,261
sync-direct-shutdown,256,4,success,31191429,54589782,shutdown_only_ns,1,261
sync-direct-shutdown,256,5,success,32043660,52780516,shutdown_only_ns,1,261
sync-direct-shutdown,256,6,success,30496871,53402074,shutdown_only_ns,1,261
sync-direct-shutdown,256,7,success,34010362,53681951,shutdown_only_ns,1,261
async,256,1,success,2715504,1264638,async_receive_shutdown_ns,1,1
async,256,2,success,3290621,1556246,async_receive_shutdown_ns,1,1
async,256,3,success,2688547,1236143,async_receive_shutdown_ns,1,1
async,256,4,success,2821275,1229499,async_receive_shutdown_ns,1,1
async,256,5,success,2686498,1267206,async_receive_shutdown_ns,1,1
async,256,6,success,3518090,1555754,async_receive_shutdown_ns,1,1
async,256,7,success,3285583,1612982,async_receive_shutdown_ns,1,1
async_churn,100,565470,admissions=0,threads=1
async_local_route,cold_topics=0,median_ns_per_publish=1517
async_local_route,cold_topics=127,median_ns_per_publish=875
async_local_route,cold_topics=1023,median_ns_per_publish=874
```
