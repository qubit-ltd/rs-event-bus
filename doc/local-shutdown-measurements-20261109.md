# Local shutdown benchmark measurements (2026-11-09)

## Scope and interpretation

### T2 causal check addendum

The benchmark distinguishes `sync` serial cancel+shutdown (`cancel_shutdown_ns`), `sync-concurrent-cancel` concurrent public `cancel()` calls with all calls joined and shutdown afterward (`cancel_request_wait_ns`), and `sync-direct-shutdown` (`shutdown_only_ns`). Each ran with two discarded warmups and seven fresh child processes per count; the final `cargo +1.94.0 bench --locked --bench local_threads` exited 0 with all measured rows successful and no watchdog timeouts. Exact serial medians were 502,747,660 / 1,719,967,933 / 7,260,405,806 ns; direct shutdown medians were 50,256,673 / 50,855,358 / 53,916,208 ns; and concurrent cancellation medians were 50,303,194 / 50,226,514 / 74,447,104 ns at 16 / 64 / 256 subscriptions. These small samples are noisy and do not establish a defect.

The public `Subscription::cancel()` combines setting cancellation state with waiting for worker completion. Therefore the concurrent scenario measures concurrent calls, not a strict request-all-then-wait-all phase. At the time of the uninstrumented benchmark, no safe test-only request/receive-return/worker-completion hook or interval override existed. The separate isolated test probe below now adds those per-instance `cfg(test)` facilities; its results are reported separately and do not alter or replace the uninstrumented 84-row benchmark. The original benchmark did not include a blocked-handler control or idle wake-frequency measurement; the later blocked-handler synchronization control is documented below. The evidence supports a receive-polling contribution for idle serial cancellation in this test setup, but is limited by three instrumented runs per cell, probe overhead, host contention, and the direct-shutdown 10 ms/256 variability. No production cancellation change is justified.

Two exploratory invocations of `cargo +1.94.0 bench --locked --bench local_threads` are excluded: the first ended with indeterminate tool status before count 256; the second was stopped with Ctrl-C (exit 130) while T1 was using the shared Cargo resource and had reached the async 64-subscription samples. Their partial output was not retained, so it is not included as evidence. The single final captured run above is the valid series and had no failed child samples.

- Final run source base HEAD: `64dde486eec5db59c6b24ab807626a72c7e99844`.
- Measured benchmark source SHA-256: `eb475f3455337b3258beec1a78e36863ab2e4683029690d568a435a50d739ac2`.
- Current benchmark source SHA-256 after the run: `238ff327a82aaf3a50a0a6c07adbe9ba972d87bcfff10ea835d31632ed4998a9`; the post-run change only corrects the child runner's Rustdoc parameter name.
- Measured support source SHA-256: `8436dcecd2f0ca7133f7b40bb9784cbe1337761f09e1b3e5e1f6cc1d6da75bb2`.
- Current support source SHA-256 after the run: `b46e6280e3fa24b2b6547a40ffafb52736e77966c35ca9b09020c92c5c33cb90`; post-run changes only expand parser help, clarify the measured boundary, and add failure-status assertions to the harness tests.
- `Cargo.lock` SHA-256: `07b21e4f6f5fa2bd1f51fb4e08bf5dcc243dc878f30213ecdd6a71e1affce8f6`.
- Toolchain: rustc 1.94.0, host `x86_64-unknown-linux-gnu`. Host load was not independently sampled; no CPU affinity was set.

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

## T2 recorded run: complete measured data

Run output SHA-256: `a7cd5f5903e7630c85c573d3600718bc4931e39c02d55c9a262a5b6ef7e8dc98`. The command exited 0; all 84 rows (four modes × three counts × seven samples) report success. The four trailing lines are probe summaries. Harness test `cargo +1.94.0 test --locked --test local_threads_batch_tests` exited 0 (9 passed).

| Scenario | Subscriptions | Duration kind | Samples | Median (ns) | Min–max (ns) |
| --- | ---: | --- | ---: | ---: | ---: |
| sync | 16 | `cancel_shutdown_ns` | 7 | 502,747,660 | 200,715,445–657,755,841 |
| sync | 64 | `cancel_shutdown_ns` | 7 | 1,719,967,933 | 1,611,098,622–2,253,642,143 |
| sync | 256 | `cancel_shutdown_ns` | 7 | 7,260,405,806 | 6,827,920,070–8,371,617,367 |
| sync-direct-shutdown | 16 | `shutdown_only_ns` | 7 | 50,256,673 | 50,205,539–52,611,993 |
| sync-direct-shutdown | 64 | `shutdown_only_ns` | 7 | 50,855,358 | 50,648,985–55,387,951 |
| sync-direct-shutdown | 256 | `shutdown_only_ns` | 7 | 53,916,208 | 52,689,026–54,555,667 |
| sync-concurrent-cancel | 16 | `cancel_request_wait_ns` | 7 | 50,303,194 | 50,222,268–52,967,241 |
| sync-concurrent-cancel | 64 | `cancel_request_wait_ns` | 7 | 50,226,514 | 50,137,897–50,304,111 |
| sync-concurrent-cancel | 256 | `cancel_request_wait_ns` | 7 | 74,447,104 | 50,295,246–108,217,247 |
| async | 16 | `async_receive_shutdown_ns` | 7 | 113,389 | 97,716–156,282 |
| async | 64 | `async_receive_shutdown_ns` | 7 | 332,303 | 322,876–472,683 |
| async | 256 | `async_receive_shutdown_ns` | 7 | 2,754,497 | 2,376,926–5,854,846 |

The recorded raw CSV follows; columns retain the command header.

```text
mode,subscriptions,iteration,status,creation_ns,duration_ns,duration_kind,baseline_threads,peak_threads
sync,16,1,success,1680394,551113835,cancel_shutdown_ns,1,21
sync,16,2,success,1865217,657755841,cancel_shutdown_ns,1,21
sync,16,3,success,2597822,502747660,cancel_shutdown_ns,1,21
sync,16,4,success,2629508,452536393,cancel_shutdown_ns,1,21
sync,16,5,success,2244646,551628973,cancel_shutdown_ns,1,21
sync,16,6,success,1761794,451277436,cancel_shutdown_ns,1,21
sync,16,7,success,2696461,200715445,cancel_shutdown_ns,1,21
sync-direct-shutdown,16,1,success,2569034,50205539,shutdown_only_ns,1,21
sync-direct-shutdown,16,2,success,2285206,50232426,shutdown_only_ns,1,21
sync-direct-shutdown,16,3,success,2661942,50290681,shutdown_only_ns,1,21
sync-direct-shutdown,16,4,success,2735207,50227006,shutdown_only_ns,1,21
sync-direct-shutdown,16,5,success,2500384,50256673,shutdown_only_ns,1,21
sync-direct-shutdown,16,6,success,2367249,50278198,shutdown_only_ns,1,21
sync-direct-shutdown,16,7,success,2585393,52611993,shutdown_only_ns,1,21
sync-concurrent-cancel,16,1,success,3207600,50929301,cancel_request_wait_ns,1,21
sync-concurrent-cancel,16,2,success,2161224,52967241,cancel_request_wait_ns,1,21
sync-concurrent-cancel,16,3,success,2485173,50224206,cancel_request_wait_ns,1,21
sync-concurrent-cancel,16,4,success,2643127,50303194,cancel_request_wait_ns,1,21
sync-concurrent-cancel,16,5,success,2622866,50280153,cancel_request_wait_ns,1,21
sync-concurrent-cancel,16,6,success,2141472,50323623,cancel_request_wait_ns,1,21
sync-concurrent-cancel,16,7,success,2744864,50222268,cancel_request_wait_ns,1,21
async,16,1,success,367636,113389,async_receive_shutdown_ns,1,1
async,16,2,success,413648,133526,async_receive_shutdown_ns,1,1
async,16,3,success,353786,97716,async_receive_shutdown_ns,1,1
async,16,4,success,365558,113187,async_receive_shutdown_ns,1,1
async,16,5,success,330739,101581,async_receive_shutdown_ns,1,1
async,16,6,success,491460,156282,async_receive_shutdown_ns,1,1
async,16,7,success,321881,117270,async_receive_shutdown_ns,1,1
sync,64,1,success,6987552,2253642143,cancel_shutdown_ns,1,69
sync,64,2,success,7522330,1960536054,cancel_shutdown_ns,1,69
sync,64,3,success,6809159,1824777601,cancel_shutdown_ns,1,69
sync,64,4,success,38070741,1611098622,cancel_shutdown_ns,1,69
sync,64,5,success,63191678,1622273880,cancel_shutdown_ns,1,69
sync,64,6,success,14928884,1719967933,cancel_shutdown_ns,1,69
sync,64,7,success,9564294,1712210737,cancel_shutdown_ns,1,69
sync-direct-shutdown,64,1,success,10418516,55387951,shutdown_only_ns,1,69
sync-direct-shutdown,64,2,success,15674090,50855358,shutdown_only_ns,1,69
sync-direct-shutdown,64,3,success,9107948,51203472,shutdown_only_ns,1,69
sync-direct-shutdown,64,4,success,7996524,51378404,shutdown_only_ns,1,69
sync-direct-shutdown,64,5,success,6870572,50852795,shutdown_only_ns,1,69
sync-direct-shutdown,64,6,success,5580989,50648985,shutdown_only_ns,1,69
sync-direct-shutdown,64,7,success,8675794,50755594,shutdown_only_ns,1,69
sync-concurrent-cancel,64,1,success,6099963,50294051,cancel_request_wait_ns,1,69
sync-concurrent-cancel,64,2,success,6126409,50221697,cancel_request_wait_ns,1,69
sync-concurrent-cancel,64,3,success,8426613,50137897,cancel_request_wait_ns,1,69
sync-concurrent-cancel,64,4,success,5687301,50192078,cancel_request_wait_ns,1,69
sync-concurrent-cancel,64,5,success,5949533,50304111,cancel_request_wait_ns,1,69
sync-concurrent-cancel,64,6,success,7048509,50226514,cancel_request_wait_ns,1,69
sync-concurrent-cancel,64,7,success,6854694,50228489,cancel_request_wait_ns,1,69
async,64,1,success,844922,335679,async_receive_shutdown_ns,1,1
async,64,2,success,1041494,472683,async_receive_shutdown_ns,1,1
async,64,3,success,896881,332303,async_receive_shutdown_ns,1,1
async,64,4,success,785784,330253,async_receive_shutdown_ns,1,1
async,64,5,success,799140,329051,async_receive_shutdown_ns,1,1
async,64,6,success,839053,339572,async_receive_shutdown_ns,1,1
async,64,7,success,787363,322876,async_receive_shutdown_ns,1,1
sync,256,1,success,34100069,7860877645,cancel_shutdown_ns,1,261
sync,256,2,success,31437240,7260405806,cancel_shutdown_ns,1,261
sync,256,3,success,30850156,8371617367,cancel_shutdown_ns,1,261
sync,256,4,success,45818655,6827920070,cancel_shutdown_ns,1,261
sync,256,5,success,34235074,7111004837,cancel_shutdown_ns,1,261
sync,256,6,success,30359317,7110045236,cancel_shutdown_ns,1,261
sync,256,7,success,32631853,7511108320,cancel_shutdown_ns,1,261
sync-direct-shutdown,256,1,success,32254241,53532210,shutdown_only_ns,1,261
sync-direct-shutdown,256,2,success,30424826,54115316,shutdown_only_ns,1,261
sync-direct-shutdown,256,3,success,32462193,54555667,shutdown_only_ns,1,261
sync-direct-shutdown,256,4,success,32623945,52689026,shutdown_only_ns,1,261
sync-direct-shutdown,256,5,success,33915151,53503915,shutdown_only_ns,1,261
sync-direct-shutdown,256,6,success,31203662,54140031,shutdown_only_ns,1,261
sync-direct-shutdown,256,7,success,29872348,53916208,shutdown_only_ns,1,261
sync-concurrent-cancel,256,1,success,30026544,50295246,cancel_request_wait_ns,1,261
sync-concurrent-cancel,256,2,success,31256121,50340962,cancel_request_wait_ns,1,261
sync-concurrent-cancel,256,3,success,31879505,50480864,cancel_request_wait_ns,1,261
sync-concurrent-cancel,256,4,success,48160948,85910250,cancel_request_wait_ns,1,261
sync-concurrent-cancel,256,5,success,43008717,108217247,cancel_request_wait_ns,1,261
sync-concurrent-cancel,256,6,success,52199102,74447104,cancel_request_wait_ns,1,261
sync-concurrent-cancel,256,7,success,53515505,91775570,cancel_request_wait_ns,1,261
async,256,1,success,8762529,2615374,async_receive_shutdown_ns,1,1
async,256,2,success,5279967,2376926,async_receive_shutdown_ns,1,1
async,256,3,success,7517183,2754497,async_receive_shutdown_ns,1,1
async,256,4,success,3781164,2990071,async_receive_shutdown_ns,1,1
async,256,5,success,4994008,2995599,async_receive_shutdown_ns,1,1
async,256,6,success,4603649,2659963,async_receive_shutdown_ns,1,1
async,256,7,success,5773179,5854846,async_receive_shutdown_ns,1,1
async_churn,100,514963,admissions=0,threads=1
async_local_route,cold_topics=0,median_ns_per_publish=1478
async_local_route,cold_topics=127,median_ns_per_publish=1511
async_local_route,cold_topics=1023,median_ns_per_publish=1680
```

## Instrumented cancellation causal probe

Command: `cargo +1.94.0 test --locked --lib test_sync_cancel_causal_probe_and_poll_intervals -- --nocapture` (exit 0; one test passed, 219 filtered). The isolated child used a 120-second watchdog; the original four wait probes retain their 10-second watchdog. It covered idle subscriptions at 16, 64, and 256, using per-instance test-only receive intervals of 10 ms and 50 ms. Each configuration ran three times. The 12 mode/interval/count combinations were interleaved in a fixed order rotated by five entries per trial. Workers had a 100 ms warmup before timing; measurements remained on one host, with no affinity or host-load isolation.

| Mode | Interval | Subscriptions | Elapsed median (range), ns | Receive returns, n | Return latency median (range), ms | CPU ticks median (range) | Wake median |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Serial cancel | 10 ms | 16 | 112,193,578 (103,105,247–112,609,413) | 43 | 9.709 (0.050–10.067) | 1 (1–1) | 1 |
| Serial cancel | 10 ms | 64 | 378,749,856 (365,822,272–385,537,185) | 179 | 9.204 (0.002–10.267) | 10 (10–11) | 1 |
| Serial cancel | 10 ms | 256 | 1,499,457,333 (1,426,801,251–1,666,956,166) | 724 | 4.965 (0.015–13.577) | 227 (180–297) | 1 |
| Serial cancel | 50 ms | 16 | 557,885,514 (551,700,044–602,852,911) | 45 | 49.362 (0.021–50.104) | 1 (1–2) | 1 |
| Serial cancel | 50 ms | 64 | 1,859,585,161 (1,856,080,219–1,905,669,376) | 179 | 48.916 (0.006–50.078) | 13 (12–15) | 1 |
| Serial cancel | 50 ms | 256 | 7,123,206,398 (6,726,327,729–7,372,247,617) | 746 | 47.308 (0.005–51.519) | 155 (145–179) | 1 |
| Direct shutdown | 10 ms | 16 | 10,364,709 (2,370,303–10,668,021) | 45 | 1.322 (0.177–9.922) | 1 (0–1) | 1 |
| Direct shutdown | 10 ms | 64 | 11,723,951 (11,633,871–12,282,385) | 187 | 4.976 (0.033–9.739) | 2 (1–2) | 1 |
| Direct shutdown | 10 ms | 256 | 98,829,821 (48,168,686–120,948,163) | 206 | 2.676 (0.019–9.844) | 35 (17–43) | 0 |
| Direct shutdown | 50 ms | 16 | 51,879,481 (50,446,561–52,184,217) | 46 | 49.142 (0.061–50.879) | 0 (0–0) | 1 |
| Direct shutdown | 50 ms | 64 | 51,597,424 (51,099,228–51,652,738) | 192 | 46.030 (40.204–49.615) | 2 (2–2) | 1 |
| Direct shutdown | 50 ms | 256 | 55,041,608 (54,835,443–62,007,640) | 765 | 31.072 (7.439–50.476) | 5 (5–6) | 1 |

Elapsed time and process CPU ticks are medians/ranges across the three trials; receive-return latency and wake counts pool the three trials. The serial elapsed window ends immediately after the last public `cancel()` returns; the subsequent immediate bus shutdown only cleans up the already-cancelled bus and is outside that duration. The direct-shutdown window covers the complete `bus.shutdown(Immediate)` call. CPU ticks sum user and system time for the entire child process, including all test threads, and are coarse process accounting rather than worker-only CPU.

Each cancellation timestamp is captured immediately after the `cancelled` release store while holding the per-instance trace mutex. The worker trace hook takes that mutex, preventing a receive return from passing the marker. Serial receive-return latency tracks the test poll interval, and worker completion follows those returns closely. Direct shutdown at 10 ms returned before some workers reached a post-cancel receive; its 10 ms / 256 range is wide. This supports a polling contribution to idle serial cancellation in this test, without establishing production gains or justifying a default change.

The same isolated test exercises a deliberately blocked handler for both public `cancel()` and bus `shutdown()`. A test-only signal confirms the waiter has reached the worker join or active-worker condition-variable wait while the handler remains behind its release gate. The test verifies the lifecycle call has not completed at that point, releases the handler, then verifies success. This uses synchronization rather than a fixed latency assertion; it demonstrates that an intentionally blocked handler can dominate both operations. The two controls are in the raw log (they are synchronization checks, not timing rows).

There are 4,032 per-subscription samples across the 36 idle configurations. The [normalized CSV](cancellation-causal-probe-20261109.csv) includes a trial number; the [raw test log](cancellation-causal-probe-20261109.raw.txt) includes the exact per-run elapsed and CPU tick outputs plus blocked-handler control outcomes. The command completed in 39.85 seconds; exit 0. The host was not isolated and host load was not sampled; prior probe attempts observed unrelated repository Cargo jobs, so these host timings and process CPU ticks should not be treated as isolated-machine estimates. Source baseline: `64dde486eec5db59c6b24ab807626a72c7e99844`; `Cargo.lock` SHA-256: `07b21e4f6f5fa2bd1f51fb4e08bf5dcc243dc878f30213ecdd6a71e1affce8f6`; SHA-256 of instrumented Rust source diffs: `63ed6c618768890dbbb224d24c2fba6ce0f6cc9453ec0048e16e4251c737388a`. Toolchain: rustc 1.94.0, cargo 1.94.0. Host: x86_64 Ubuntu Linux 6.8.0-147-generic (`mas-server`). Probe instrumentation uses per-control mutexes, timestamp reads, and trace storage, so its results are instrumented and should remain distinct from the uninstrumented 84-row benchmark.
