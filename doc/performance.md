# Event bus performance measurements

This file consolidates the crate’s recorded benchmark decisions and capacity observations. Results are workload- and host-specific; they are not portable guarantees. See each section for its method, limitations, and evidence links.


## Admission summary

### Baseline identity

This is the **pre-cache baseline** for EB-002. It measures the implementation at source commit `1a0d47cfbbb5e23c840af78416a26a6cb997a907`; no `PublishReceipt` admission-outcome cache was present. The benchmark file was an uncommitted working-tree change with SHA-256 patch digest `a51d7fdb09525944f2da6d259e9cae90f7cd153d2de7fde00f972dc9040f34bc` (SHA-256 of `git diff -- benches/facade_delivery.rs` at measurement time). Branch: `codex/eb-cancellation-admission-20261106`.

Run time: 2026-11-06 UTC. Host: Linux `mas-server`, kernel `6.8.0-147-generic`, x86_64; Intel Xeon E5-2680 v4 @ 2.40GHz, 56 logical CPUs (2 sockets × 14 cores × 2 threads). Rust toolchain: `rustc 1.94.0 (4a4ef493e 2026-03-02)`, LLVM 21.1.8; Cargo 1.94.0. Profile: Cargo's optimized `bench` profile (release optimization, no custom profile flags).

Command: `cargo bench --locked --bench facade_delivery`

Final exact exit code: **0**. The process emitted one original no-subscriber case and all 24 requested matrix outputs. No Rust compiler warnings or benchmark errors were emitted. `size_of::<PublishReceipt>()` was 112 bytes.

### Measurement design and boundaries

The matrix includes synchronous `EventBus` ordinary and checked publish, asynchronous `AsyncEventBus` ordinary and checked publish, and `NotificationPublisher`, each at 0, 1, 16, and 128 destinations. Each matrix case runs two warmup batches followed by seven reported raw samples. Each batch contains exactly 1,000 messages. The retained original no-subscriber async facade case also uses two warmups and seven raw samples, each containing 10,000 messages.

Fixture construction, subscriptions, topic/request construction, notification publisher creation, and cleanup are outside facade publish timing. Sync and async facade samples time the 1,000-call publication loop including local facade/SPI admission; they do not include handler completion. The async fixture holds subscriptions without starting their handlers, keeping the receiver queues as the fixed destinations. The sync fixture has no-op handlers and raises facade scheduling limits to 128,000 owned deliveries overall and per subscription, 128 running handlers, and 128 subscriptions. Every local SPI mailbox has capacity 1,000 and the provider-wide outstanding limit is 128,000.

Notification enqueue samples time only the 1,000 `try_publish` calls. Drain-inclusive samples report enqueue duration plus the duration of `close`, which closes admission and waits for worker drain. The notification publisher queue capacity is 1,000. A fresh local sync fixture is created per notification sample. After drain, assertions require 1,000 enqueued and published notifications, the expected admission category (all `no_destinations` at fanout 0, all `accepted` otherwise), and zero publication/request errors, queue-full, queue-closed, observer-panicked, or worker-panicked counts.

For ordinary facade publication, every receipt is checked to report exactly the configured accepted destination count. Checked publication uses `AtLeastOneAccepted`; 0 destinations must produce `CheckedPublishError::Admission` with `AdmissionCheckError::NoAcceptedDestination`, and nonzero fanout must return a receipt with all destinations accepted. The 0-destination outcome is an expected checked failure after ordinary publication; it is not a provider publish failure.

All runs use the in-process local SPI. These results do not measure provider/network delivery latency or subscriber handler completion. No message is intentionally rejected to create load.

### Raw baseline results

Each value is elapsed nanoseconds for the indicated whole batch. The output is copied verbatim from the successful final benchmark run; sample order is run order and has not been sorted.

```text
publish_receipt_size_bytes=112
case=original_no_subscribers raw_sample_ns=[11243618, 11163740, 11334392, 11315575, 11306207, 11404007, 11415623] messages=10000
case=sync_publish destinations=0 raw_sample_ns=[1339068, 1349329, 1333654, 1344617, 1332781, 1409412, 1341268] messages=1000 boundary=facade_publish_loop_including_local_admission
case=sync_checked destinations=0 raw_sample_ns=[1419503, 1410914, 1423515, 1409531, 1416853, 1413496, 1422631] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_publish destinations=0 raw_sample_ns=[1492919, 1470987, 1335544, 1332712, 1338241, 1332440, 1337196] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_checked destinations=0 raw_sample_ns=[1390878, 1382732, 1392963, 1393007, 1389285, 1390078, 1390873] messages=1000 boundary=facade_publish_loop_including_local_admission
case=notification_enqueue destinations=0 raw_sample_ns=[39002, 38864, 39449, 39060, 39186, 38939, 39089] messages=1000 boundary=try_publish_loop_only
case=notification_drain_inclusive destinations=0 raw_sample_ns=[2338914, 2333860, 2374058, 2321214, 2394879, 2330675, 2330628] messages=1000 boundary=enqueue_loop_through_close_and_worker_drain
case=sync_publish destinations=1 raw_sample_ns=[3746990, 3178048, 3763600, 3216432, 3677227, 3937767, 3734097] messages=1000 boundary=facade_publish_loop_including_local_admission
case=sync_checked destinations=1 raw_sample_ns=[3852361, 3949927, 3350348, 3856800, 3359799, 3889930, 3473698] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_publish destinations=1 raw_sample_ns=[1642895, 1646284, 1706292, 1664789, 1646313, 1712203, 1653761] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_checked destinations=1 raw_sample_ns=[1691565, 1711370, 1725740, 1701963, 1680242, 1670762, 1691540] messages=1000 boundary=facade_publish_loop_including_local_admission
case=notification_enqueue destinations=1 raw_sample_ns=[39135, 39149, 39675, 48887, 39075, 41440, 38964] messages=1000 boundary=try_publish_loop_only
case=notification_drain_inclusive destinations=1 raw_sample_ns=[4627984, 4890419, 5800143, 4744424, 4701375, 4626923, 5859895] messages=1000 boundary=enqueue_loop_through_close_and_worker_drain
case=sync_publish destinations=16 raw_sample_ns=[24570853, 30502022, 28933144, 29174278, 31962024, 23832250, 33078717] messages=1000 boundary=facade_publish_loop_including_local_admission
case=sync_checked destinations=16 raw_sample_ns=[31327754, 25093696, 25454304, 26923405, 42373141, 28896750, 38580355] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_publish destinations=16 raw_sample_ns=[7173066, 7437337, 7283968, 7101923, 7517644, 7233914, 7221497] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_checked destinations=16 raw_sample_ns=[7602146, 7361795, 7266026, 7223263, 7321477, 7425564, 7166214] messages=1000 boundary=facade_publish_loop_including_local_admission
case=notification_enqueue destinations=16 raw_sample_ns=[38499, 65345, 38450, 38599, 52797, 49266, 48994] messages=1000 boundary=try_publish_loop_only
case=notification_drain_inclusive destinations=16 raw_sample_ns=[36497351, 26061746, 50220705, 39272037, 43230346, 39760646, 40214441] messages=1000 boundary=enqueue_loop_through_close_and_worker_drain
case=sync_publish destinations=128 raw_sample_ns=[439588798, 442812951, 355623126, 488270031, 434773847, 407928946, 479172170] messages=1000 boundary=facade_publish_loop_including_local_admission
case=sync_checked destinations=128 raw_sample_ns=[479729624, 422532463, 449789777, 481279681, 460440677, 489200492, 355145799] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_publish destinations=128 raw_sample_ns=[59213831, 60232553, 57515217, 62996952, 61546504, 66426094, 63177107] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_checked destinations=128 raw_sample_ns=[66048158, 64019596, 65187281, 61568944, 60350797, 61398033, 65715495] messages=1000 boundary=facade_publish_loop_including_local_admission
case=notification_enqueue destinations=128 raw_sample_ns=[52641, 38499, 93046, 58356, 82192, 38716, 69330] messages=1000 boundary=try_publish_loop_only
case=notification_drain_inclusive destinations=128 raw_sample_ns=[813466631, 509840528, 713808866, 870511048, 894034088, 1110753705, 635013913] messages=1000 boundary=enqueue_loop_through_close_and_worker_drain
```

These samples are a single-host pre-cache baseline, not a portable performance claim. Later candidate measurements should use the same benchmark parameters and boundaries, and compare interleaved runs on the same host to account for host noise.

### Cache candidate comparison

The run order was baseline (the raw baseline above), candidate 1, baseline 2, candidate 2. All runs used the same host, Rust 1.94.0, optimized bench profile, and unchanged benchmark parameters. Each command was `cargo bench --locked --bench facade_delivery` and exited 0, emitting 25 case lines with no compiler warnings or benchmark errors. The temporary candidate patch had SHA-256 `773ccf42d05772d125ea66bb3b8f889cb64c69aabe80b965abbf44e5a1599089` (SHA-256 of `git diff -- src/model/publish_receipt.rs`). Its receipt size was 144 bytes versus 112 bytes without the cache, an increase of 32 bytes (28.6%). The candidate code was removed after measurement.

#### Candidate 1 (after documented baseline)

Command: `cargo bench --locked --bench facade_delivery`; exit code: 0. Raw elapsed nanoseconds per batch, in run order:

```text
publish_receipt_size_bytes=144
case=original_no_subscribers raw_sample_ns=[11390405, 11507303, 11568731, 11655312, 11463671, 11504616, 11529279] messages=10000
case=sync_publish destinations=0 raw_sample_ns=[1370815, 1375260, 1387157, 1364147, 1368805, 1371873, 1380900] messages=1000 boundary=facade_publish_loop_including_local_admission
case=sync_checked destinations=0 raw_sample_ns=[1408024, 1405526, 1417618, 1414942, 1412729, 1405370, 1400514] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_publish destinations=0 raw_sample_ns=[1386050, 1365180, 1372940, 1370973, 1374244, 1368115, 1387020] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_checked destinations=0 raw_sample_ns=[1452097, 1457534, 1443581, 1443422, 1455417, 1449563, 1452619] messages=1000 boundary=facade_publish_loop_including_local_admission
case=notification_enqueue destinations=0 raw_sample_ns=[37667, 37937, 37706, 37812, 37834, 38061, 37607] messages=1000 boundary=try_publish_loop_only
case=notification_drain_inclusive destinations=0 raw_sample_ns=[3623346, 3120382, 3065861, 3185755, 3118733, 3039283, 3723515] messages=1000 boundary=enqueue_loop_through_close_and_worker_drain
case=sync_publish destinations=1 raw_sample_ns=[3227243, 3109207, 3046187, 3143777, 3155697, 3187089, 3086883] messages=1000 boundary=facade_publish_loop_including_local_admission
case=sync_checked destinations=1 raw_sample_ns=[3097763, 3203968, 3110023, 3123010, 3475243, 3125910, 3134878] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_publish destinations=1 raw_sample_ns=[1707780, 1729324, 1684992, 1683798, 1680667, 1670286, 1668715] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_checked destinations=1 raw_sample_ns=[1723708, 1718604, 1728226, 1720150, 1707619, 1717535, 1732368] messages=1000 boundary=facade_publish_loop_including_local_admission
case=notification_enqueue destinations=1 raw_sample_ns=[38433, 38317, 37942, 37922, 38072, 37942, 38177] messages=1000 boundary=try_publish_loop_only
case=notification_drain_inclusive destinations=1 raw_sample_ns=[4722172, 4591739, 5147091, 4676289, 4732580, 4780115, 4610795] messages=1000 boundary=enqueue_loop_through_close_and_worker_drain
case=sync_publish destinations=16 raw_sample_ns=[26007599, 35565696, 24507865, 24055484, 28056952, 27814596, 31744685] messages=1000 boundary=facade_publish_loop_including_local_admission
case=sync_checked destinations=16 raw_sample_ns=[27023557, 32638418, 39309832, 42143692, 24297781, 21717417, 22680709] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_publish destinations=16 raw_sample_ns=[7483138, 7480111, 7493310, 7430398, 7402792, 7421366, 7417452] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_checked destinations=16 raw_sample_ns=[7451920, 7556483, 7521448, 7420085, 7616571, 7402071, 7422117] messages=1000 boundary=facade_publish_loop_including_local_admission
case=notification_enqueue destinations=16 raw_sample_ns=[51987, 38221, 37941, 61050, 37946, 37914, 46929] messages=1000 boundary=try_publish_loop_only
case=notification_drain_inclusive destinations=16 raw_sample_ns=[30258378, 38256041, 24753460, 27612652, 50998146, 25253205, 48105932] messages=1000 boundary=enqueue_loop_through_close_and_worker_drain
case=sync_publish destinations=128 raw_sample_ns=[738427315, 727072358, 528659657, 625220515, 724370219, 2968671532, 654633988] messages=1000 boundary=facade_publish_loop_including_local_admission
case=sync_checked destinations=128 raw_sample_ns=[3924249911, 1222105455, 684033749, 629970618, 476975020, 416766159, 419202232] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_publish destinations=128 raw_sample_ns=[64559473, 64656507, 65923722, 65944019, 65453768, 67829965, 66241781] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_checked destinations=128 raw_sample_ns=[64876346, 63436863, 64728716, 66286893, 72502647, 66921872, 64985123] messages=1000 boundary=facade_publish_loop_including_local_admission
case=notification_enqueue destinations=128 raw_sample_ns=[48177, 61888, 58651, 48171, 46578, 52923, 38221] messages=1000 boundary=try_publish_loop_only
case=notification_drain_inclusive destinations=128 raw_sample_ns=[689776656, 586871935, 765957128, 803868615, 587283816, 652706287, 718402937] messages=1000 boundary=enqueue_loop_through_close_and_worker_drain
```

#### Baseline 2 (after candidate 1)

Command: `cargo bench --locked --bench facade_delivery`; exit code: 0. Raw elapsed nanoseconds per batch, in run order:

```text
publish_receipt_size_bytes=112
case=original_no_subscribers raw_sample_ns=[11269029, 11264511, 11303739, 11300487, 11266418, 11225036, 11282899] messages=10000
case=sync_publish destinations=0 raw_sample_ns=[1341897, 1330748, 1328681, 1413325, 1337444, 1339701, 1340275] messages=1000 boundary=facade_publish_loop_including_local_admission
case=sync_checked destinations=0 raw_sample_ns=[1363623, 1387329, 1368159, 1362324, 1365935, 1577282, 1371160] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_publish destinations=0 raw_sample_ns=[1338086, 1362918, 1340261, 1347457, 1334714, 1446921, 1536035] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_checked destinations=0 raw_sample_ns=[1405551, 1430214, 1408779, 1414834, 1409013, 1446832, 1486488] messages=1000 boundary=facade_publish_loop_including_local_admission
case=notification_enqueue destinations=0 raw_sample_ns=[42130, 41624, 41596, 41454, 41630, 47573, 51214] messages=1000 boundary=try_publish_loop_only
case=notification_drain_inclusive destinations=0 raw_sample_ns=[2365254, 2372689, 2373925, 2773815, 2377606, 2435045, 2482688] messages=1000 boundary=enqueue_loop_through_close_and_worker_drain
case=sync_publish destinations=1 raw_sample_ns=[3752419, 3174606, 3357637, 3215503, 3354007, 3249966, 3361258] messages=1000 boundary=facade_publish_loop_including_local_admission
case=sync_checked destinations=1 raw_sample_ns=[3347450, 3726888, 3667368, 3306828, 5184760, 3754628, 3741931] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_publish destinations=1 raw_sample_ns=[1904242, 1900198, 2051340, 2375918, 1891694, 1889193, 1893637] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_checked destinations=1 raw_sample_ns=[1955229, 2277795, 2209088, 2262235, 2278846, 2312926, 2073996] messages=1000 boundary=facade_publish_loop_including_local_admission
case=notification_enqueue destinations=1 raw_sample_ns=[38275, 38275, 41306, 50728, 39509, 60633, 38711] messages=1000 boundary=try_publish_loop_only
case=notification_drain_inclusive destinations=1 raw_sample_ns=[5283791, 5122310, 5273725, 5154938, 5182160, 5812399, 5021761] messages=1000 boundary=enqueue_loop_through_close_and_worker_drain
case=sync_publish destinations=16 raw_sample_ns=[25914987, 35615515, 22506389, 25401222, 24041265, 27462476, 38710386] messages=1000 boundary=facade_publish_loop_including_local_admission
case=sync_checked destinations=16 raw_sample_ns=[33583516, 26662787, 22770649, 42296691, 35621956, 22926956, 27849256] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_publish destinations=16 raw_sample_ns=[7372431, 7389845, 7361661, 7441996, 7367637, 7392632, 7332630] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_checked destinations=16 raw_sample_ns=[7469496, 7414834, 7436227, 7484750, 7458927, 7484155, 7414455] messages=1000 boundary=facade_publish_loop_including_local_admission
case=notification_enqueue destinations=16 raw_sample_ns=[38437, 50349, 73882, 38150, 91936, 92060, 38028] messages=1000 boundary=try_publish_loop_only
case=notification_drain_inclusive destinations=16 raw_sample_ns=[52210112, 38988126, 28443106, 53534503, 48116889, 25011952, 56953409] messages=1000 boundary=enqueue_loop_through_close_and_worker_drain
case=sync_publish destinations=128 raw_sample_ns=[891225989, 788210313, 710546310, 575670776, 969968905, 1176724360, 471390013] messages=1000 boundary=facade_publish_loop_including_local_admission
case=sync_checked destinations=128 raw_sample_ns=[524160789, 552007032, 494140967, 446631245, 436492219, 438655663, 412242040] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_publish destinations=128 raw_sample_ns=[67056100, 70089376, 64712036, 67076565, 68076425, 64538142, 66418189] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_checked destinations=128 raw_sample_ns=[63953612, 66012250, 66105186, 65614749, 63957650, 65990228, 67098236] messages=1000 boundary=facade_publish_loop_including_local_admission
case=notification_enqueue destinations=128 raw_sample_ns=[65564, 49258, 50253, 38452, 38628, 38839, 85589] messages=1000 boundary=try_publish_loop_only
case=notification_drain_inclusive destinations=128 raw_sample_ns=[3799401553, 1903154669, 1832323797, 633071938, 1889894731, 1131543703, 560764896] messages=1000 boundary=enqueue_loop_through_close_and_worker_drain
```

#### Candidate 2 (after baseline 2)

Command: `cargo bench --locked --bench facade_delivery`; exit code: 0. Raw elapsed nanoseconds per batch, in run order:

```text
publish_receipt_size_bytes=144
case=original_no_subscribers raw_sample_ns=[20868672, 11730929, 13008814, 11610395, 11541627, 11591979, 11725043] messages=10000
case=sync_publish destinations=0 raw_sample_ns=[1322861, 1342502, 1323072, 1351218, 1326073, 1332232, 1334102] messages=1000 boundary=facade_publish_loop_including_local_admission
case=sync_checked destinations=0 raw_sample_ns=[1377413, 1392509, 1392027, 1393895, 1403614, 1389274, 1389020] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_publish destinations=0 raw_sample_ns=[1375583, 1380952, 1384631, 1379021, 1388513, 1372997, 1386551] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_checked destinations=0 raw_sample_ns=[1449946, 1447853, 1450265, 1448049, 1444123, 1455824, 1449749] messages=1000 boundary=facade_publish_loop_including_local_admission
case=notification_enqueue destinations=0 raw_sample_ns=[38532, 39030, 38518, 38888, 38958, 47464, 54550] messages=1000 boundary=try_publish_loop_only
case=notification_drain_inclusive destinations=0 raw_sample_ns=[2442267, 2433388, 2357904, 2370711, 2360197, 2396467, 2357067] messages=1000 boundary=enqueue_loop_through_close_and_worker_drain
case=sync_publish destinations=1 raw_sample_ns=[3252260, 3235417, 3190109, 3205879, 4253763, 3366371, 3660987] messages=1000 boundary=facade_publish_loop_including_local_admission
case=sync_checked destinations=1 raw_sample_ns=[5393307, 5360028, 5461919, 5183740, 3435790, 3832499, 3621199] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_publish destinations=1 raw_sample_ns=[1712914, 1723218, 1792597, 1793169, 1704898, 1714445, 1721759] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_checked destinations=1 raw_sample_ns=[1753488, 1930614, 1749476, 1832640, 1887869, 1800429, 1740699] messages=1000 boundary=facade_publish_loop_including_local_admission
case=notification_enqueue destinations=1 raw_sample_ns=[38348, 38441, 38061, 38122, 38112, 38162, 38369] messages=1000 boundary=try_publish_loop_only
case=notification_drain_inclusive destinations=1 raw_sample_ns=[5011840, 4816107, 4842845, 4694543, 4934148, 4973444, 4754772] messages=1000 boundary=enqueue_loop_through_close_and_worker_drain
case=sync_publish destinations=16 raw_sample_ns=[34152056, 41626156, 41383906, 33296671, 41336176, 39701878, 38665090] messages=1000 boundary=facade_publish_loop_including_local_admission
case=sync_checked destinations=16 raw_sample_ns=[33049317, 26510415, 27383623, 32638160, 44732088, 28745764, 42478566] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_publish destinations=16 raw_sample_ns=[7232853, 7184430, 7197624, 7188034, 7183567, 7351773, 7178669] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_checked destinations=16 raw_sample_ns=[7232882, 7285396, 7268031, 7299201, 7240148, 7251490, 7295425] messages=1000 boundary=facade_publish_loop_including_local_admission
case=notification_enqueue destinations=16 raw_sample_ns=[49031, 58630, 38649, 38248, 52025, 48394, 49418] messages=1000 boundary=try_publish_loop_only
case=notification_drain_inclusive destinations=16 raw_sample_ns=[46351035, 25969037, 40376459, 52096516, 42436980, 35381879, 50624547] messages=1000 boundary=enqueue_loop_through_close_and_worker_drain
case=sync_publish destinations=128 raw_sample_ns=[574479180, 2662835845, 632683661, 520960397, 492362651, 493198899, 454747454] messages=1000 boundary=facade_publish_loop_including_local_admission
case=sync_checked destinations=128 raw_sample_ns=[564171765, 500929740, 458584820, 412248038, 431456124, 431426388, 427253341] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_publish destinations=128 raw_sample_ns=[71163039, 68470065, 80914030, 65679402, 63018795, 67724270, 62992884] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_checked destinations=128 raw_sample_ns=[84576105, 89503179, 81023538, 80068828, 102985649, 86057693, 63247766] messages=1000 boundary=facade_publish_loop_including_local_admission
case=notification_enqueue destinations=128 raw_sample_ns=[38307, 64907, 38248, 92683, 74883, 38184, 96473] messages=1000 boundary=try_publish_loop_only
case=notification_drain_inclusive destinations=128 raw_sample_ns=[768812546, 802459698, 685776383, 606644997, 1157618527, 1209024536, 949301246] messages=1000 boundary=enqueue_loop_through_close_and_worker_drain
```

#### Decision

The cache candidate did not show a repeatable gain at both 16 and 128 destinations, and low-fanout samples do not establish absence of a material regression. Async 16-destination samples were near the baseline with a small run-to-run spread; async 128-destination candidate samples varied from slightly faster to slower and included scheduler outliers. Sync high-fanout samples were substantially noisier than the candidate effect. Together with the 32-byte receipt growth, this evidence does not meet the design retention criterion. The cache is therefore not retained. `PublishReceipt` keeps computing its classification from the immutable acknowledgement; the benchmark and public-behavior regression tests remain to guide a future measured decision. No performance improvement is claimed. These local-SPI measurements do not describe remote providers or handler completion.

### Final correctness-assertion pair

The async checked-publish zero-destination assertion was strengthened to require `CheckedPublishError::Admission` with `AdmissionCheckError::NoAcceptedDestination`; this pair reran after that benchmark-only correctness refinement. The benchmark source patch digest at these runs was SHA-256 `cec31af9da9eee7c61bd5a6ec5a462e138cf103933178cf2e74d09d79673fee4`. The run order was baseline 3 followed by candidate 3. Both used `cargo bench --locked --bench facade_delivery`, exited 0, emitted 25 case lines, and had no compiler warnings or benchmark errors. The baseline again measured 112-byte receipts; the temporary candidate measured 144-byte receipts.

#### Baseline 3 (exact 0-destination checked-error assertion)

Command: `cargo bench --locked --bench facade_delivery`; exit code: 0. Raw elapsed nanoseconds per batch, in run order:

```text
publish_receipt_size_bytes=112
case=original_no_subscribers raw_sample_ns=[11396608, 11407800, 11337922, 11336575, 11313092, 11298025, 11313820] messages=10000
case=sync_publish destinations=0 raw_sample_ns=[1343703, 1338330, 1338063, 1334426, 1341460, 1336916, 1342518] messages=1000 boundary=facade_publish_loop_including_local_admission
case=sync_checked destinations=0 raw_sample_ns=[1383037, 1398266, 1383753, 1381135, 1386367, 1390204, 1387139] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_publish destinations=0 raw_sample_ns=[1313606, 1318988, 1312667, 1303148, 1304597, 1308140, 1309657] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_checked destinations=0 raw_sample_ns=[1392345, 1396170, 1400394, 1387367, 1385571, 1402882, 1387430] messages=1000 boundary=facade_publish_loop_including_local_admission
case=notification_enqueue destinations=0 raw_sample_ns=[37540, 38097, 37884, 37862, 37748, 37961, 37765] messages=1000 boundary=try_publish_loop_only
case=notification_drain_inclusive destinations=0 raw_sample_ns=[3695213, 2998114, 2964817, 2984035, 3191721, 3047596, 3089078] messages=1000 boundary=enqueue_loop_through_close_and_worker_drain
case=sync_publish destinations=1 raw_sample_ns=[3949234, 3863431, 3096782, 3145447, 3355050, 3061750, 3593575] messages=1000 boundary=facade_publish_loop_including_local_admission
case=sync_checked destinations=1 raw_sample_ns=[3113911, 3190757, 3224014, 3112256, 3198626, 3166663, 3306179] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_publish destinations=1 raw_sample_ns=[1712843, 1710317, 1698918, 1798926, 1748538, 1618445, 1681733] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_checked destinations=1 raw_sample_ns=[1717146, 1767353, 1688953, 1747902, 1800587, 1802855, 1754300] messages=1000 boundary=facade_publish_loop_including_local_admission
case=notification_enqueue destinations=1 raw_sample_ns=[38065, 41602, 38140, 49112, 38345, 37873, 38182] messages=1000 boundary=try_publish_loop_only
case=notification_drain_inclusive destinations=1 raw_sample_ns=[4925383, 4580682, 4664226, 4745590, 4714941, 4581790, 4755410] messages=1000 boundary=enqueue_loop_through_close_and_worker_drain
case=sync_publish destinations=16 raw_sample_ns=[33305970, 27509675, 25430896, 25913633, 38886678, 26240358, 29973065] messages=1000 boundary=facade_publish_loop_including_local_admission
case=sync_checked destinations=16 raw_sample_ns=[29487318, 33684678, 35196232, 25483668, 35260247, 45525868, 34201677] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_publish destinations=16 raw_sample_ns=[8750534, 10202815, 8766265, 9688054, 8081953, 9129660, 7479199] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_checked destinations=16 raw_sample_ns=[7778646, 7753432, 7740912, 9666699, 9532069, 10064339, 10028081] messages=1000 boundary=facade_publish_loop_including_local_admission
case=notification_enqueue destinations=16 raw_sample_ns=[38389, 38320, 38644, 38438, 38595, 38484, 38623] messages=1000 boundary=try_publish_loop_only
case=notification_drain_inclusive destinations=16 raw_sample_ns=[49327658, 27450261, 56919681, 34158435, 43446094, 36515201, 31338301] messages=1000 boundary=enqueue_loop_through_close_and_worker_drain
case=sync_publish destinations=128 raw_sample_ns=[2225730729, 491290965, 743186227, 399198593, 676095442, 869329612, 394084792] messages=1000 boundary=facade_publish_loop_including_local_admission
case=sync_checked destinations=128 raw_sample_ns=[497214081, 490608750, 408552371, 456828292, 611199027, 495878566, 463013072] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_publish destinations=128 raw_sample_ns=[68266497, 70722325, 69446560, 84232845, 71996861, 70466982, 70794574] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_checked destinations=128 raw_sample_ns=[66380952, 63593100, 62144945, 63928544, 63576369, 63038625, 81119453] messages=1000 boundary=facade_publish_loop_including_local_admission
case=notification_enqueue destinations=128 raw_sample_ns=[92227, 65482, 74155, 38617, 39226, 47256, 86000] messages=1000 boundary=try_publish_loop_only
case=notification_drain_inclusive destinations=128 raw_sample_ns=[504297059, 1306797044, 710647514, 996398244, 847091839, 613127782, 1245116116] messages=1000 boundary=enqueue_loop_through_close_and_worker_drain
```

#### Candidate 3 (interleaved cache candidate)

Command: `cargo bench --locked --bench facade_delivery`; exit code: 0. Raw elapsed nanoseconds per batch, in run order:

```text
publish_receipt_size_bytes=144
case=original_no_subscribers raw_sample_ns=[11632418, 11900016, 11803178, 11621702, 11716908, 11535438, 11754739] messages=10000
case=sync_publish destinations=0 raw_sample_ns=[1484351, 1499236, 1498051, 1485232, 1494565, 1484184, 1498457] messages=1000 boundary=facade_publish_loop_including_local_admission
case=sync_checked destinations=0 raw_sample_ns=[1557675, 1542952, 1561443, 1555572, 1545524, 1560123, 1556167] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_publish destinations=0 raw_sample_ns=[1369912, 1360919, 1369967, 1358363, 1362151, 1372390, 1361842] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_checked destinations=0 raw_sample_ns=[1421407, 1431293, 1423698, 1424411, 1463075, 1425302, 1431484] messages=1000 boundary=facade_publish_loop_including_local_admission
case=notification_enqueue destinations=0 raw_sample_ns=[37736, 37741, 91809, 37751, 37906, 37973, 38069] messages=1000 boundary=try_publish_loop_only
case=notification_drain_inclusive destinations=0 raw_sample_ns=[3504063, 3515978, 4421023, 3530932, 3270521, 3436948, 3277301] messages=1000 boundary=enqueue_loop_through_close_and_worker_drain
case=sync_publish destinations=1 raw_sample_ns=[3363942, 3563434, 3328155, 3310713, 3569561, 3365218, 3512736] messages=1000 boundary=facade_publish_loop_including_local_admission
case=sync_checked destinations=1 raw_sample_ns=[3419325, 3413885, 3503048, 3600676, 3392210, 3552311, 3484600] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_publish destinations=1 raw_sample_ns=[1718039, 1768166, 1735125, 1776289, 1846265, 1791784, 1752617] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_checked destinations=1 raw_sample_ns=[1772922, 1755488, 1759687, 1775324, 1774032, 1853865, 1770052] messages=1000 boundary=facade_publish_loop_including_local_admission
case=notification_enqueue destinations=1 raw_sample_ns=[52537, 38563, 57423, 48506, 38384, 76024, 38399] messages=1000 boundary=try_publish_loop_only
case=notification_drain_inclusive destinations=1 raw_sample_ns=[6343531, 5366636, 5294444, 5119309, 5505696, 5248793, 5101926] messages=1000 boundary=enqueue_loop_through_close_and_worker_drain
case=sync_publish destinations=16 raw_sample_ns=[28171083, 23999877, 29935540, 26755896, 23088484, 40991683, 50395490] messages=1000 boundary=facade_publish_loop_including_local_admission
case=sync_checked destinations=16 raw_sample_ns=[26994173, 25005371, 28839810, 85176537, 127364719, 87724612, 130203884] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_publish destinations=16 raw_sample_ns=[15699256, 15717331, 15421660, 14748040, 15597976, 15287720, 12874206] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_checked destinations=16 raw_sample_ns=[13740423, 9406463, 8528852, 7608027, 7576274, 7423968, 7280026] messages=1000 boundary=facade_publish_loop_including_local_admission
case=notification_enqueue destinations=16 raw_sample_ns=[38584, 92120, 38372, 38319, 38349, 52276, 47615] messages=1000 boundary=try_publish_loop_only
case=notification_drain_inclusive destinations=16 raw_sample_ns=[33551267, 32378694, 34232631, 29488057, 49056850, 130413808, 174383408] messages=1000 boundary=enqueue_loop_through_close_and_worker_drain
case=sync_publish destinations=128 raw_sample_ns=[485343600, 465796000, 454782778, 388377158, 446327525, 459078891, 492625242] messages=1000 boundary=facade_publish_loop_including_local_admission
case=sync_checked destinations=128 raw_sample_ns=[390852646, 508111807, 608118285, 436555083, 416588378, 463152150, 479024153] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_publish destinations=128 raw_sample_ns=[72206153, 74040454, 72744446, 69769262, 71422270, 70771763, 71762210] messages=1000 boundary=facade_publish_loop_including_local_admission
case=async_checked destinations=128 raw_sample_ns=[68733751, 74110697, 69139701, 73332664, 75431392, 77427182, 75006591] messages=1000 boundary=facade_publish_loop_including_local_admission
case=notification_enqueue destinations=128 raw_sample_ns=[51592, 38907, 38483, 53479, 45614, 38515, 48692] messages=1000 boundary=try_publish_loop_only
case=notification_drain_inclusive destinations=128 raw_sample_ns=[1177029812, 1357950272, 1240777335, 2168637458, 877035796, 892549636, 1047449713] messages=1000 boundary=enqueue_loop_through_close_and_worker_drain
```


## Facade signal registration

### Method

The benchmark loads the production `AsyncSignal` module through a benchmark-only sibling source file. No signal algorithm is copied, and no public API was added. The baseline binary was built after restoring `register_waiter` to the implementation at HEAD `99cf32ab1b4ee517758e5b13e5f41df19573bbc8`; the SHA-256 of the HEAD signal source is `9c79d6c799d09e5f5ac485c86bf7111a82c3c7636fa29f287d002e397e2388eb`. Its benchmark binary SHA-256 is `28ffbe700af5d3f29a25a473a50e6e29aed2bb744926e1a7cd96e7ea04359498`.

The candidate binary used the temporary `will_wake` early-return implementation. Its signal source SHA-256 was `1ce1e6d1fa866f4ab18c55994c81f9c4393cb80642c69606f85f94b6bc9c1802`; its benchmark binary SHA-256 is `1293a8eadff7e3e4bc294c7c63cd2887d7c7c03a466c96bd3707b8b8b52ad77c`. The candidate source was removed after applying the decision rule below. Both binaries used the same benchmark source, `Cargo.lock`, optimized bench profile, and dependency graph.

Environment: Linux x86_64, kernel `6.8.0-147-generic`, 56 reported CPUs, Rust `rustc 1.94.0 (4a4ef493e 2026-03-02)`, Cargo `1.94.0 (85eff7c80 2026-01-15)`. Other workspace Cargo activity was visible on the host while measuring, so absolute timings are local evidence rather than a product performance guarantee.

At build time, SHA-256 identities were `f8ca4fc190bebc2e44c28d05107a8686c976568d7f9f315afe42a2c8e826e841` for `benches/facade_signal_registration.rs` and `983ce213fad5a3e461eb373daba0bbf25a1b4e610a667d6bd062d23f2c9048c3` for its path-module support source. The benchmark binary hashes above identify the exact compiled artifacts.

Each process performed two warmups and then recorded seven sample means for each scenario. Seven process pairs alternated order: baseline/candidate, candidate/baseline, baseline/candidate, candidate/baseline, baseline/candidate, candidate/baseline, baseline/candidate. Thus each version has 49 recorded sample means per scenario. Samples are nanoseconds per operation. The end-to-end scenario used the public local facade to subscribe, caller-poll its runner, perform four repeated idle polls with the same task waker before each publish, receive each publication, acknowledge it in the handler, and poll through settlement back to receive wait. It measured 250 publishes per sample. The direct scenarios registered one signal waiter 100,000 times with either one stable waker or alternating wakers.

Commands used to build each binary:

```text
cargo +1.94.0 bench --locked --bench facade_signal_registration --no-run
```

The 14 measurement processes invoked `/tmp/rs-event-bus-t2/baseline both` and `/tmp/rs-event-bus-t2/candidate both`. All process invocations exited 0. Complete process output was retained at `/tmp/rs-event-bus-t2/raw_process_samples.txt` during this implementation; all seven-sample vectors are reproduced below so the record does not depend on that temporary file.

Before applying the candidate, the same-waker clone-count regression was run against the baseline with `cargo +1.94.0 test --locked --lib facade::async_event_bus::internal::async_signal::tests::test_same_waker_registration_does_not_clone_again -- --exact --nocapture`. One test ran and failed as expected: two clones were observed where the candidate assertion expected one. After the performance decision, that optimization-specific assertion was replaced by behavior checks that pass on the retained implementation.

### Results

| Scenario | Baseline median | Baseline MAD | Candidate median | Candidate MAD | Candidate change |
| --- | ---: | ---: | ---: | ---: | ---: |
| Same waker | 68 ns/op | 1 ns | 33 ns/op | 0 ns | -35 ns (-51.47%) |
| Alternating wakers | 68 ns/op | 0 ns | 91 ns/op | 0 ns | +23 ns (+33.82%) |
| Public end-to-end | 16,823 ns/op | 332 ns | 16,239 ns/op | 269 ns | -584 ns (-3.47%) |

MAD is the median absolute deviation from the median, computed over all 49 sample means for that version and scenario. The same-waker improvement exceeds both 5% and the larger MAD (1 ns). The alternating-waker regression exceeds 5% and both MADs (0 ns). The end-to-end change is an improvement smaller than its larger MAD (332 ns), so it does not show a clear end-to-end gain or regression.

The candidate fails the guardrail because alternating wakers became slower by 33.82%, beyond the measured spread. The production optimization is therefore not retained. This is consistent with the candidate taking an initial lookup lock and then a second insertion lock for every alternating waker, compared with the baseline's single insertion lock.

Callback counts (clone, drop, wake) were identical across the seven samples in each process:

| Scenario | Baseline | Candidate |
| --- | --- | --- |
| Same waker | (100001, 100000, 1) | (1, 0, 1) |
| Alternating wakers | (100001, 100000, 1) | (100000, 99999, 1) |
| Public end-to-end, 250 publishes | (2989, 2488, 516) | (1504, 1003, 516) |

### Raw sample means

The values in each vector are the concatenation of the seven samples from each of the seven processes for that version, in process-pair order. Every process used two warmups, omitted from these vectors.

```text
same baseline:
[68,68,67,68,68,68,67, 67,68,67,67,67,68,67, 67,67,67,67,68,67,68, 67,68,67,67,67,67,67, 86,71,68,67,67,68,68, 68,68,68,68,69,68,68, 68,68,67,67,68,68,70]
same candidate:
[33,33,33,33,33,33,33, 33,33,33,33,33,33,33, 33,33,33,33,33,33,33, 33,33,33,33,33,33,33, 33,33,33,33,33,33,33, 33,33,34,33,33,34,33, 33,33,33,33,33,33,33]
alternating baseline:
[68,68,68,68,68,68,68, 69,69,69,69,69,69,69, 68,68,68,68,68,68,68, 68,68,68,68,68,68,68, 68,68,68,68,68,68,68, 68,68,68,68,68,68,68, 68,69,69,68,68,68,68]
alternating candidate:
[91,91,91,91,91,91,91, 91,91,91,91,91,91,91, 91,91,91,91,91,91,91, 91,91,91,91,91,91,91, 91,91,91,91,91,91,91, 91,92,91,92,91,92,92, 91,92,92,92,92,92,96]
end-to-end baseline:
[19276,18884,18816,18744,18481,18666,18577, 16874,16743,16755,16680,16740,17354,16547, 16815,16706,16623,16695,16631,16714,17385, 17227,16823,16769,16578,16617,16635,16734, 16593,16601,16345,16365,16474,16491,16491, 17289,16839,17115,16647,17380,19243,17085, 18121,18289,18693,18490,18055,17947,17778]
end-to-end candidate:
[17175,16219,16120,16044,16120,16180,16215, 17139,16668,16745,16831,16614,16644,16762, 16268,16110,16239,16089,16155,16107,16201, 16436,16386,16505,16113,16216,16260,16387, 16683,16264,15951,15970,15960,15902,15892, 16476,15763,16119,15817,15926,15886,16034, 17784,17640,17588,17576,17504,17617,17229]
```

### Regression coverage retained

The performance optimization was withdrawn, but the behavior regressions remain useful: repeated same-waker registration followed by notification wakes once and can be registered again; an alternating waker replaces the previous one; notification from inside a waker clone callback does not prevent insertion; and existing tests verify clone/drop/wake callbacks run outside the registry lock. Tests use the repository's existing `RawWaker` ownership discipline.


## Ordering

Measured on 2026-09-29. **Keep the current production Weak registry.** The
allocation-identity Drop cleanup candidate improves the high-key registry stress
cases by 82.12–97.97%, but fails the maximum 5% default-case regression gate.
With four live keys and a four-ticket window, churn regresses from 292 to 444
ns/op (+52.05%; MAD 1 and 2 ns). The actual public facade at default admission
also regresses for same-key delivery from 3886 to 4238 ns/op (+9.06%; MAD 73 and
21 ns). This rejects this candidate, rather than establishing that every possible
cleanup optimization must regress.

### Decision conditions

The implementation design requires all three conditions: registry retain scans
use at least 10% of enqueue-path CPU; high-key median elapsed cost improves at
least 15%; and the default four-key case regresses by no more than 5%.
The elapsed comparisons below come from uninstrumented stable release builds.
The separate CPU profile is used for CPU attribution, never as an elapsed-time
comparison or a wall-clock surrogate. No production ordering files were changed.

### CPU profile and gate result

The retain CPU condition **passes**, the high-key elapsed improvement condition
**passes**, and the default-regression condition **fails**. The three conditions
are conjunctive, so the candidate is rejected.

A matching gprof histogram and debug-bearing optimized binary attribute real
CPU samples to the original source; no production instrumentation or copied
registry state machine is involved. Only the following unmistakable retain
locations are counted, excluding other likely scan-loop/atomic samples:

| CPU sample location | Samples / 284 total | Fraction of all sampled CPU |
|---|---:|---:|
| hashbrown `HashMap::retain`, map.rs:904 | 99 | 34.86% |
| original retain predicate, async_ordering_lanes.rs:55 | 99 | 34.86% |
| hashbrown `HashMap::retain`, map.rs:905 | 25 | 8.80% |
| Conservative identified scan total | 223 | 78.52% |

The complete enqueue function accounts for 97.54% of sampled CPU (277 of 284
samples). Identified scan samples therefore account for approximately 80.50% of
that self CPU; using the larger whole-process denominator still gives a
conservative 78.52% lower bound, well above the required 10%. These percentages
are sampled CPU work, not mcount call proportions or measured wall time.
The 266,240 recorded enqueue calls equal 262,144 measured operations plus 4,096
initial lane creations. The asserted live-key count implies 1,073,741,824 retain
predicates during the steady workload, plus 8,386,560 during initial creation.
No individual scan predicate counter was added to production.

The fresh profiling process exited 0 in 2.980107 wall seconds,
using 2.979420 CPU seconds, with 15
involuntary switches. Other authors had resumed tools, and external processes
were allowed to continue; this was not an exclusive machine window. The CPU
profile is one separate attribution run with 284 CPU histogram samples,
not a substitute for the seven elapsed samples in every matrix configuration.
Profiling retains normal optimization/inlining and explicitly disables Cargo's
release debug stripping so gprof can resolve inline source locations.

```sh
cargo +nightly-2026-06-05 rustc --locked --release --bench ordering_keys \
  --manifest-path /tmp/ordering-t8-baseline/Cargo.toml \
  --target-dir /tmp/ordering-t8-profile -- \
  -Zinstrument-mcount=yes -Cdebuginfo=2 -Cstrip=none \
  -Cforce-frame-pointers=yes -Clink-arg=-pg
ORDERING_PROFILE=1 ORDERING_PROFILE_OPERATIONS=262144 taskset -c 4 \
  /tmp/ordering-t8-profile/release/deps/ordering_keys-0e8857df5b94497b
gprof /tmp/ordering-t8-profile/release/deps/ordering_keys-0e8857df5b94497b gmon.out -p -q
gprof -l /tmp/ordering-t8-profile/release/deps/ordering_keys-0e8857df5b94497b gmon.out -p
```

Profile binary SHA-256: `68ce9e69e9d8c8989a9046da9c8138978ee9180b6a7266ccda793bfc2c201603`.
Matching gmon SHA-256: `5cdb6c104a0c4e193ad2e13bfa2bca8fb0a1225e80ec84f1787c57e4f327d6c6`.
Full flat/callgraph and inline-line reports: `baseline-gprof.txt` and
`baseline-gprof-lines.txt`, with process metadata in `profile-run.json`.
The earlier stripped binary's profile established enqueue CPU concentration but
could not establish scan attribution; it is preserved separately as
`initial-stripped-gmon.out` / `initial-stripped-gprof.txt` and is excluded from final scan attribution. Only the fresh matching
histogram and symbol file listed above support the scan gate. `perf` was denied by
`perf_event_paranoid=4`; the validated mcount/gprof route required no sysctl change.


### Machine and builds

- Intel Core i5-9600K at nominal 3.70 GHz, six physical cores, no SMT; 9 MiB L3.
- x86_64 Linux 7.0.0-34-generic; CPU governor `powersave`, dynamic turbo enabled.
- Performance builds: rustc 1.94.0 (`4a4ef493e`, 2026-03-02), Cargo 1.94.0;
  Cargo bench/release defaults, optimized level 3, no LTO, default codegen units,
  locked dependencies. No RUSTFLAGS or Cargo profile overrides were set.
- CPU profiling build: rustc 1.98.0-nightly (`e7815e522`, 2026-06-04), toolchain
  `nightly-2026-06-05`; release optimization with mcount instrumentation, debug
  information and forced frame pointers. GNU gprof 2.42.
- Both comparison binaries were pinned to CPU 4. The team paused heavy tools,
  but unrelated builds and desktop activity continued. This was not machine-wide
  isolation. Each timed sample records wall time, thread CPU and process CPU.
  Registry stress accepts thread CPU / wall >=98%; the real facade accepts
  process CPU / wall >=98% so legitimate process-owned timer work is included.
  There is no intended timer, network or condition-variable wait in these timed
  fixtures. All 672 accepted samples satisfy that rule (minimum ratio
  0.980390). 748 rejected scheduling-contaminated attempts are retained.
  This screening does not remove shared-cache, frequency or thermal variation;
  the raw samples and MAD expose the remaining dispersion.

Baseline and candidate were built from frozen source copies of crate version
0.16.0 during the boundary migration, with identical non-ordering source,
benchmark input and lockfile. The final target also typechecks against the
integrated 0.17.0 API; these measurements concern ordering, not release-wide
end-to-end performance. Only `async_lane.rs` and
`async_ordering_lanes.rs` differed. Comparison source copies and binaries are
under `/tmp/ordering-t8-baseline`, `/tmp/ordering-t8-candidate`,
`/tmp/ordering-t8-target` and `/tmp/ordering-t8-candidate-target`. Raw evidence is
under `/tmp/ordering-t8-evidence`. These temporary paths identify session provenance.
The exact original sample/rejection text, parsed samples with MAD association,
profile outputs, source/binary SHA manifests and complete candidate patch are
permanently stored in [ordering_performance_artifacts](ordering_performance_artifacts/README.md),
and in the [fixed evidence snapshot](https://github.com/qubit-ltd/rs-event-bus/tree/e8a1ed724954756d48c3d5216cbd13ac39a0cb81/doc/ordering_performance_artifacts).
That artifact set and the tables below preserve this decision independently of `/tmp`.

Frozen baseline SHA-256 values:

- `benches/ordering_keys.rs`: `2997d68798675de5e3701ed92e414483e22a43808e78171e01872d223d39bd96`
- `src/pipeline/ordering_lane/internal/async_lane.rs`: `e346cc1bd9dd4b736f8cc6bff623daddcd26ba6e3115d7efd0b9183d0525b4d3`
- `src/pipeline/ordering_lane/internal/async_ordering_lanes.rs`: `3d0aa6de9992b09d37d004568e2821d44e34125284443d05cf8a5c5bdd926b12`

The final benchmark makes a metadata-only naming correction: the frozen facade
log label `input_keys` was a case-size parameter. The final target prints
`case_keys` and `distinct_input_keys` explicitly. Measured operations and timing
logic are unchanged. Its Linux CPU-clock bindings are also gated so other hosts
can compile the target; CPU-validated execution requires 64-bit Linux.
The final target binds the six production async ordering source files directly
and reexports only the three types it uses, instead of importing the internal
module root and its unrelated test-only synchronous reexports. This binding-only
change removes an unused-import suppression; all workload functions and the
frozen measured source snapshots remain unchanged.
A final Clippy correction passes the formatted ordering key by value rather than
borrowing it; the builder stores the same key and request preparation remains
outside the timed region. The final benchmark source SHA-256 is
`abc6714a926005e5c6bede00dfc5c69ff640892cc2151d62c4d916485fb28da8`; the measured baseline SHA above and all measurement artifacts
retain their original bytes. No performance sample was rerun for these changes.

### Workload boundaries

Registry stress imports the **actual production internal module** using `#[path]`,
without expanding library visibility or copying its state machine. It keeps
exactly 4, 64, 1024 or 4096 guards alive, then queues up to 4 or 64 new
tickets. Balanced batches contain min(live keys, window) distinct keys;
same-key batches fill the whole window. Balanced work rotates distinct keys; same-key work really polls pending
tickets before FIFO handoff; churn drops and rebuilds a lane with fresh prebuilt
keys. The guard count is asserted after every sample. Each sample completes
32,768 operations. All keys, payload values, vectors and initial lanes are
prepared before timing. Future allocation, enqueue, poll, guard handoff and
release remain part of the measured operation. Wait timestamps introduce the
same observation overhead in both versions. Results and completion counts are
passed through `black_box`.

Retained guards are deliberately outside the new-ticket window in registry
stress. This is a high-cardinality internal stress boundary, **not** a claim
that default facade admission 4 permits 4096 simultaneous lanes. The separate
public-facade matrix applies real admission 4 and max(case keys, 64), and observes
actual gated first-wave handlers. Balanced elevated cases observe 4, 64, 1024
and 4096 simultaneous distinct-key handlers; default admission observes at most
four. Same-key cases contain one distinct key and observe one handler. Churn
contains one new input key per delivery, and observes the configured initial
window. Public-facade samples complete 2 * max(case keys, 1024) deliveries.

Publish requests are built and all publications complete before timing. The
runner is initially polled with blocked handlers, then the timed interval
releases the gate and drains deliveries. Same-key initial queue depth is fixed
at 64; remaining messages are published before timing without polling the gated
runner. This avoids a cubic, unmeasured setup caused by repeatedly polling
thousands of pending same-key tickets, while retaining real same-key queuing,
the complete message count and every admission configuration.

Every side/configuration has two warmups and seven accepted independent samples.
The wrapper alternates baseline/candidate order between adjacent cases; each
case compares the same parameters close together. Seven samples support this
decision, not a tail-latency service-level claim. Registry wait p95 is computed
within each sample; facade reports drain cost and first-wave concurrency instead
of pretending to measure per-message waiting time.

The original full-matrix pilot was discarded because only whole-process CPU
time had been recorded, including setup, which could not identify timed-region
preemption. Its incomplete final same-key setup was terminated. None of those
pilot values enter the tables or the decision.

### Registry results

Candidate change = (candidate median / baseline median - 1) * 100%; negative
means faster. MAD is the median absolute deviation of the seven elapsed
ns/op samples. Mops/s = 1000 / median ns/op.

| Case keys | Window / admission | Workload | Baseline seven ns/op samples | Candidate seven ns/op samples | Median B / C | MAD B / C | Candidate change | Mops/s B / C | Rejected B / C |
|---:|---:|---|---|---|---:|---:|---:|---:|---:|
| 4 | 4 | balanced | 244, 247, 245, 246, 244, 254, 248 | 239, 239, 239, 244, 242, 238, 241 | 246 / 239 | 2 / 1 | -2.85% | 4.0650 / 4.1841 | 12 / 4 |
| 4 | 4 | same-key | 245, 254, 262, 261, 265, 256, 255 | 239, 248, 253, 251, 255, 249, 246 | 256 / 249 | 5 / 3 | -2.73% | 3.9062 / 4.0161 | 4 / 38 |
| 4 | 4 | churn | 285, 291, 293, 292, 291, 293, 293 | 447, 445, 438, 444, 439, 446, 443 | 292 / 444 | 1 / 2 | +52.05% | 3.4247 / 2.2523 | 7 / 7 |
| 4 | 64 | balanced | 249, 258, 256, 253, 254, 253, 252 | 245, 243, 242, 244, 243, 236, 241 | 253 / 243 | 1 / 1 | -3.95% | 3.9526 / 4.1152 | 4 / 6 |
| 4 | 64 | same-key | 257, 249, 259, 260, 250, 253, 261 | 253, 253, 253, 251, 254, 250, 255 | 257 / 253 | 4 / 1 | -1.56% | 3.8911 / 3.9526 | 2 / 11 |
| 4 | 64 | churn | 299, 296, 287, 297, 295, 298, 301 | 450, 451, 446, 440, 451, 444, 441 | 297 / 446 | 2 / 5 | +50.17% | 3.3670 / 2.2422 | 16 / 8 |
| 64 | 4 | balanced | 318, 327, 325, 322, 320, 323, 319 | 259, 244, 236, 236, 239, 259, 257 | 322 / 244 | 3 / 8 | -24.22% | 3.1056 / 4.0984 | 39 / 47 |
| 64 | 4 | same-key | 317, 314, 317, 317, 320, 322, 322 | 245, 242, 240, 243, 245, 252, 244 | 317 / 244 | 3 / 1 | -23.03% | 3.1546 / 4.0984 | 22 / 32 |
| 64 | 4 | churn | 460, 461, 465, 468, 467, 461, 471 | 473, 465, 454, 472, 467, 461, 472 | 465 / 467 | 4 / 5 | +0.43% | 2.1505 / 2.1413 | 20 / 6 |
| 64 | 64 | balanced | 306, 319, 315, 318, 314, 312, 317 | 246, 249, 251, 250, 249, 248, 252 | 315 / 249 | 3 / 1 | -20.95% | 3.1746 / 4.0161 | 2 / 7 |
| 64 | 64 | same-key | 310, 321, 310, 305, 319, 316, 312 | 250, 249, 250, 260, 252, 247, 244 | 312 / 250 | 4 / 2 | -19.87% | 3.2051 / 4.0000 | 12 / 5 |
| 64 | 64 | churn | 458, 452, 457, 454, 463, 461, 450 | 472, 466, 469, 476, 479, 465, 473 | 457 / 472 | 4 / 4 | +3.28% | 2.1882 / 2.1186 | 25 / 8 |
| 1024 | 4 | balanced | 2228, 2040, 2283, 2264, 2366, 2247, 2141 | 239, 241, 246, 251, 240, 243, 239 | 2247 / 241 | 36 / 2 | -89.27% | 0.4450 / 4.1494 | 0 / 0 |
| 1024 | 4 | same-key | 2249, 2254, 2194, 2245, 2133, 2399, 2554 | 230, 234, 225, 237, 239, 243, 248 | 2249 / 237 | 55 / 6 | -89.46% | 0.4446 / 4.2194 | 2 / 0 |
| 1024 | 4 | churn | 3104, 2974, 3128, 3101, 3038, 3052, 3161 | 529, 518, 528, 493, 519, 519, 518 | 3101 / 519 | 49 / 1 | -83.26% | 0.3225 / 1.9268 | 67 / 4 |
| 1024 | 64 | balanced | 2668, 2624, 2513, 2532, 2645, 2612, 2661 | 268, 276, 278, 275, 278, 271, 267 | 2624 / 275 | 37 / 3 | -89.52% | 0.3811 / 3.6364 | 27 / 10 |
| 1024 | 64 | same-key | 2669, 2583, 2609, 2567, 2504, 2465, 2524 | 238, 244, 233, 233, 235, 237, 237 | 2567 / 237 | 43 / 2 | -90.77% | 0.3896 / 4.2194 | 2 / 0 |
| 1024 | 64 | churn | 2571, 2573, 2549, 2544, 2606, 2777, 2611 | 465, 446, 460, 459, 480, 464, 446 | 2573 / 460 | 29 / 5 | -82.12% | 0.3887 / 2.1739 | 0 / 0 |
| 4096 | 4 | balanced | 10830, 10484, 10806, 10911, 10714, 10572, 10551 | 247, 252, 257, 256, 252, 243, 247 | 10714 / 252 | 142 / 5 | -97.65% | 0.0933 / 3.9683 | 0 / 0 |
| 4096 | 4 | same-key | 10375, 10853, 11148, 10814, 10896, 10936, 11367 | 218, 222, 222, 221, 223, 218, 219 | 10896 / 221 | 82 / 2 | -97.97% | 0.0918 / 4.5249 | 8 / 0 |
| 4096 | 4 | churn | 11091, 10749, 10721, 10827, 10797, 10640, 10760 | 530, 530, 535, 534, 538, 539, 553 | 10760 / 535 | 39 / 4 | -95.03% | 0.0929 / 1.8692 | 0 / 0 |
| 4096 | 64 | balanced | 10911, 10662, 10551, 10726, 10933, 10688, 10545 | 299, 305, 319, 320, 308, 323, 311 | 10688 / 311 | 137 / 8 | -97.09% | 0.0936 / 3.2154 | 1 / 1 |
| 4096 | 64 | same-key | 10829, 10553, 10696, 10918, 10812, 10713, 10735 | 232, 235, 232, 231, 232, 237, 236 | 10735 / 232 | 77 / 1 | -97.84% | 0.0932 / 4.3103 | 0 / 1 |
| 4096 | 64 | churn | 10725, 10869, 11089, 11103, 11266, 10912, 10831 | 488, 496, 488, 492, 488, 479, 470 | 10912 / 488 | 177 / 4 | -95.53% | 0.0916 / 2.0492 | 0 / 0 |

The baseline retain predicate visits every registry entry on every enqueue.
With the asserted live guard count, the measured 32,768 enqueues therefore imply
131,072 / 2,097,152 / 33,554,432 / 134,217,728 Weak predicates per sample for
4 / 64 / 1024 / 4096 live keys, respectively, excluding setup. Churn additionally
removes one expired entry and inserts its replacement per measured operation.
These are code-derived deterministic operation counts, not hardware counters.
The candidate performs no full-table enqueue cleanup and does one identity-
checked registry deletion at final lane release; steady balanced/same-key
handoffs keep the original allocation alive, while churn pays deletion plus
new-lane bookkeeping and key ownership. That tradeoff is visible in default
churn's regression.

### Public facade results

Case keys denotes the matrix parameter. Actual distinct input keys are that
number for balanced, one for same-key, and 2 * max(case keys, 1024) for churn.
Every accepted sample's gated handler count matches the asserted first-wave
cardinality described above.

| Case keys | Window / admission | Workload | Baseline seven ns/op samples | Candidate seven ns/op samples | Median B / C | MAD B / C | Candidate change | Mops/s B / C | Rejected B / C |
|---:|---:|---|---|---|---:|---:|---:|---:|---:|
| 4 | 4 | balanced | 4242, 4112, 4049, 4271, 4345, 4256, 4192 | 4190, 4270, 4396, 4358, 4210, 4216, 4259 | 4242 / 4259 | 50 / 49 | +0.40% | 0.2357 / 0.2348 | 11 / 7 |
| 4 | 4 | same-key | 3886, 3871, 3802, 4091, 3959, 3927, 3801 | 4183, 4204, 4238, 4246, 4247, 4210, 4259 | 3886 / 4238 | 73 / 21 | +9.06% | 0.2573 / 0.2360 | 7 / 3 |
| 4 | 4 | churn | 4030, 4006, 4066, 4451, 4302, 4308, 4276 | 4529, 4552, 4730, 4645, 4401, 4433, 4495 | 4276 / 4529 | 175 / 96 | +5.92% | 0.2339 / 0.2208 | 9 / 34 |
| 4 | 64 | balanced | 4063, 4009, 3879, 4200, 4196, 4219, 4278 | 4342, 4290, 4322, 4361, 4139, 4203, 4286 | 4196 / 4290 | 82 / 52 | +2.24% | 0.2383 / 0.2331 | 31 / 18 |
| 4 | 64 | same-key | 4093, 3982, 4089, 3930, 4156, 4177, 4001 | 4537, 4526, 4416, 4324, 4311, 4391, 4386 | 4089 / 4391 | 88 / 67 | +7.39% | 0.2446 / 0.2277 | 39 / 10 |
| 4 | 64 | churn | 4233, 4397, 4346, 4302, 4415, 4407, 4490 | 4329, 4387, 4556, 4568, 4628, 4639, 4481 | 4397 / 4556 | 51 / 75 | +3.62% | 0.2274 / 0.2195 | 2 / 10 |
| 64 | 4 | balanced | 3799, 4191, 4112, 4075, 3892, 4073, 4031 | 4222, 4347, 4281, 4262, 4328, 4248, 4186 | 4073 / 4262 | 42 / 40 | +4.64% | 0.2455 / 0.2346 | 21 / 13 |
| 64 | 4 | same-key | 3871, 3912, 3932, 3761, 3858, 3696, 3876 | 4094, 3954, 4059, 4033, 4053, 4056, 4065 | 3871 / 4056 | 41 / 9 | +4.78% | 0.2583 / 0.2465 | 12 / 8 |
| 64 | 4 | churn | 3995, 4104, 4249, 4134, 4207, 4165, 4256 | 4318, 4509, 4437, 4591, 4441, 4374, 4401 | 4165 / 4437 | 61 / 63 | +6.53% | 0.2401 / 0.2254 | 5 / 1 |
| 64 | 64 | balanced | 3983, 3929, 4013, 3867, 3806, 3727, 3671 | 4373, 4421, 4463, 4476, 4445, 4320, 4329 | 3867 / 4421 | 116 / 48 | +14.33% | 0.2586 / 0.2262 | 4 / 6 |
| 64 | 64 | same-key | 3609, 3609, 3596, 3591, 3587, 3799, 3474 | 3668, 3746, 3864, 3691, 3916, 3711, 3809 | 3596 / 3746 | 13 / 63 | +4.17% | 0.2781 / 0.2670 | 1 / 0 |
| 64 | 64 | churn | 3546, 3508, 3574, 3716, 3978, 3621, 3590 | 4145, 3903, 3835, 3970, 3846, 3875, 3859 | 3590 / 3875 | 44 / 29 | +7.94% | 0.2786 / 0.2581 | 1 / 0 |
| 1024 | 4 | balanced | 3934, 3930, 4064, 4038, 4040, 4226, 4126 | 4289, 4120, 4154, 4247, 4289, 4234, 4321 | 4040 / 4247 | 86 / 42 | +5.12% | 0.2475 / 0.2355 | 0 / 0 |
| 1024 | 4 | same-key | 3624, 3437, 3459, 3631, 3561, 3474, 3592 | 3847, 3613, 3669, 3626, 3739, 3673, 3767 | 3561 / 3673 | 70 / 60 | +3.15% | 0.2808 / 0.2723 | 0 / 0 |
| 1024 | 4 | churn | 3635, 3708, 3622, 3657, 3628, 3516, 3620 | 3886, 3767, 3726, 3659, 3708, 3659, 3718 | 3628 / 3718 | 8 / 49 | +2.48% | 0.2756 / 0.2690 | 0 / 0 |
| 1024 | 1024 | balanced | 3846, 3835, 4155, 4143, 3869, 3711, 3937 | 4231, 4358, 4122, 4265, 4399, 4268, 4325 | 3869 / 4268 | 68 / 57 | +10.31% | 0.2585 / 0.2343 | 3 / 11 |
| 1024 | 1024 | same-key | 4059, 3972, 4088, 4085, 4136, 4081, 4057 | 4639, 4571, 4623, 4476, 4598, 4642, 4617 | 4081 / 4617 | 22 / 22 | +13.13% | 0.2450 / 0.2166 | 0 / 5 |
| 1024 | 1024 | churn | 3508, 3669, 4005, 3613, 3844, 4098, 4090 | 3879, 3932, 3820, 3635, 3902, 3817, 3990 | 3844 / 3879 | 231 / 59 | +0.91% | 0.2601 / 0.2578 | 1 / 3 |
| 4096 | 4 | balanced | 3686, 3674, 3742, 3681, 3666, 3613, 3620 | 3923, 4173, 4172, 4058, 4159, 4171, 4252 | 3674 / 4171 | 12 / 12 | +13.53% | 0.2722 / 0.2398 | 0 / 1 |
| 4096 | 4 | same-key | 3329, 3378, 3367, 3540, 3395, 3466, 3637 | 3733, 3971, 3697, 3648, 3715, 3698, 3698 | 3395 / 3698 | 66 / 17 | +8.92% | 0.2946 / 0.2704 | 0 / 0 |
| 4096 | 4 | churn | 3767, 3715, 3900, 3912, 3833, 3963, 3868 | 3974, 3886, 3897, 3998, 4116, 4176, 4155 | 3868 / 3998 | 44 / 112 | +3.36% | 0.2585 / 0.2501 | 0 / 0 |
| 4096 | 4096 | balanced | 3493, 3589, 3624, 3333, 3855, 3536, 3597 | 3851, 3817, 3574, 4390, 4097, 3657, 3687 | 3589 / 3817 | 53 / 160 | +6.35% | 0.2786 / 0.2620 | 1 / 0 |
| 4096 | 4096 | same-key | 3598, 3601, 3554, 3526, 3488, 3484, 3737 | 3628, 3632, 3684, 3696, 3665, 3618, 3648 | 3554 / 3648 | 47 / 20 | +2.64% | 0.2814 / 0.2741 | 0 / 0 |
| 4096 | 4096 | churn | 4070, 3703, 3439, 3451, 4136, 3466, 3436 | 3617, 3789, 3638, 4112, 3631, 3706, 3665 | 3466 / 3665 | 30 / 41 | +5.74% | 0.2885 / 0.2729 | 1 / 2 |

### Registry wait observations

Each table entry is the median across seven sample-level queue-wait medians and
sample-level p95 values. Balanced and same-key intervals run from just before
enqueue to ready guard acquisition; churn intervals run from release/rebuild
start to acquisition. These local handoff intervals include deliberate gating
and observation overhead; they are not application end-to-end latency.

| Live registry keys | Ticket window | Workload | Baseline median wait / p95 (ns) | Candidate median wait / p95 (ns) |
|---:|---:|---|---:|---:|
| 4 | 4 | balanced | 568 / 708 | 556 / 697 |
| 4 | 4 | same-key | 596 / 736 | 573 / 723 |
| 4 | 4 | churn | 255 / 331 | 397 / 492 |
| 4 | 64 | balanced | 600 / 736 | 575 / 706 |
| 4 | 64 | same-key | 8451 / 10435 | 8262 / 10141 |
| 4 | 64 | churn | 257 / 339 | 396 / 509 |
| 64 | 4 | balanced | 763 / 1015 | 565 / 717 |
| 64 | 4 | same-key | 747 / 1006 | 559 / 704 |
| 64 | 4 | churn | 411 / 524 | 409 / 534 |
| 64 | 64 | balanced | 10166 / 13783 | 7998 / 9996 |
| 64 | 64 | same-key | 10124 / 13866 | 8208 / 10114 |
| 64 | 64 | churn | 411 / 509 | 417 / 545 |
| 1024 | 4 | balanced | 6114 / 8707 | 559 / 690 |
| 1024 | 4 | same-key | 6288 / 8787 | 535 / 624 |
| 1024 | 4 | churn | 2921 / 3623 | 450 / 682 |
| 1024 | 64 | balanced | 84660 / 154849 | 8644 / 11459 |
| 1024 | 64 | same-key | 83482 / 152302 | 7798 / 9463 |
| 1024 | 64 | churn | 2555 / 2808 | 402 / 545 |
| 4096 | 4 | balanced | 30680 / 42752 | 580 / 786 |
| 4096 | 4 | same-key | 30982 / 42907 | 531 / 605 |
| 4096 | 4 | churn | 10530 / 10973 | 461 / 730 |
| 4096 | 64 | balanced | 346559 / 648788 | 9961 / 14262 |
| 4096 | 64 | same-key | 349272 / 651563 | 7690 / 9397 |
| 4096 | 64 | churn | 10669 / 11102 | 433 / 619 |

### Reproduction and checks

The public benchmark target is dependency-free beyond existing dependencies:

```sh
cargo bench --locked --bench ordering_keys
ORDERING_SMOKE=1 cargo bench --locked --bench ordering_keys
```

`ORDERING_FILTER` selects one exact output case label for paired execution;
`ORDERING_PROFILE=1` selects the high-key workload through the current public
asynchronous facade and
`ORDERING_PROFILE_OPERATIONS` controls its operation count. A full run without
filter covers every configuration. CPU-clock validity filtering lives entirely
in the standalone benchmark, never in production code or ordinary unit tests.

The registry-only measurements above are historical results for the retired
workload; they remain unchanged and are not rewritten as facade measurements.
The current profile exercises the public facade, while the smoke mode checks
workload correctness and admission only. A correctness smoke run is not
performance evidence and is not compared with those historical registry values.

The measured comparison commands were `cargo bench --locked --bench
ordering_keys --no-run` for both frozen manifests followed by the same benchmark
executables through `taskset -c 4`; the exact pairing wrapper and full CPU/wall
records are in `run_pairs.py`, `paired-samples.log` and `paired-results.json`.
Both versions passed workload-invariant smoke runs. The temporary candidate
first failed the immediate-registry-reclaim regression against baseline
(one stale entry rather than zero), then passed after implementation. Its
allocation-identity old-lane Drop/recreated-key regression also passed (2 tests,
0 failures). These experiments did not add an assumed software defect or a
timing assertion to production tests.

Formatting used `align-ci.sh --dry-run` to confirm the project's formatter and
then the authorized one-file rustfmt invocation with `nightly-2026-06-05`,
`.infra/style/rustfmt.toml` and `skip_children=true`, avoiding concurrent edits
to other authors' files. Full repository style/CI is the integration task's
responsibility; this performance decision does not claim those checks passed.

Final one-file rustfmt check passed, and `cargo check --locked --bench
ordering_keys` against the integrated 0.17.0 crate exited 0 without warnings.
The cached `cargo bench --locked --bench ordering_keys` entrypoint also exited
0 with a filtered complete seven-sample case; the 48-case decision dataset
remains the separately frozen paired matrix.

### Rejected experimental candidate

The following patch is retained as experiment evidence and was **not applied
to production**. Final lane Drop runs after all strong owners and lane-state
locks release, acquires the registry lock, and deletes only a matching data
allocation. Enqueue upgrades and replacement remain under the registry lock.

```diff
--- a/src/pipeline/ordering_lane/internal/async_lane.rs
+++ b/src/pipeline/ordering_lane/internal/async_lane.rs
@@ -8,8 +8,11 @@
 //! Mutable state and synchronization for one asynchronous ordering lane.
 
 use std::collections::VecDeque;
+use std::collections::HashMap;
 use std::sync::Mutex;
+use std::sync::Weak;
 
+use super::OrderingLaneKey;
 use super::async_lane_state::AsyncLaneState;
 
 /// One asynchronous FIFO lane and its waiter state.
@@ -18,15 +21,34 @@
 pub(super) struct AsyncLane<T> {
     /// Active turn and queued values with their registered wakers.
     pub(super) state: Mutex<AsyncLaneState<T>>,
+    /// Registry identity used only after the last strong lane owner releases.
+    registry: Weak<Mutex<HashMap<OrderingLaneKey, Weak<AsyncLane<T>>>>>,
+    /// Stable key paired with this allocation.
+    key: OrderingLaneKey,
 }
-impl<T> Default for AsyncLane<T> {
+impl<T> AsyncLane<T> {
     /// Creates an idle lane with an empty FIFO and no registered waiters.
-    fn default() -> Self {
+    pub(super) fn new(key: OrderingLaneKey, registry: Weak<Mutex<HashMap<OrderingLaneKey, Weak<AsyncLane<T>>>>>) -> Self {
         Self {
             state: Mutex::new(AsyncLaneState {
                 active: false,
                 queue: VecDeque::new(),
             }),
+            registry,
+            key,
         }
     }
 }
+
+impl<T> Drop for AsyncLane<T> {
+    /// Removes only this allocation, after all lane-state locks are released.
+    fn drop(&mut self) {
+        if let Some(registry) = self.registry.upgrade() {
+            let mut lanes = registry.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
+            let allocation = self as *const Self;
+            if lanes.get(&self.key).is_some_and(|lane| lane.as_ptr() == allocation) {
+                lanes.remove(&self.key);
+            }
+        }
+    }
+}
--- a/src/pipeline/ordering_lane/internal/async_ordering_lanes.rs
+++ b/src/pipeline/ordering_lane/internal/async_ordering_lanes.rs
@@ -25,7 +25,7 @@
 #[must_use]
 pub(crate) struct AsyncOrderingLanes<T> {
     /// Weak lane registry so inactive ordering keys can be reclaimed.
-    lanes: Mutex<HashMap<OrderingLaneKey, Weak<AsyncLane<T>>>>,
+    lanes: Arc<Mutex<HashMap<OrderingLaneKey, Weak<AsyncLane<T>>>>>,
     /// Generates FIFO ticket identities.
     next_ticket: AtomicU64,
 }
@@ -36,7 +36,7 @@
     /// A collection with no live keys and ticket numbering starting at one.
     pub(crate) fn new() -> Self {
         Self {
-            lanes: Mutex::new(HashMap::new()),
+            lanes: Arc::new(Mutex::new(HashMap::new())),
             next_ticket: AtomicU64::new(1),
         }
     }
@@ -52,9 +52,8 @@
     pub(crate) fn enqueue(&self, key: OrderingLaneKey, value: T) -> AsyncOrderingTurn<T> {
         let lane = {
             let mut lanes = self.lanes.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
-            lanes.retain(|_, lane| lane.strong_count() != 0);
             lanes.get(&key).and_then(Weak::upgrade).unwrap_or_else(|| {
-                let lane = Arc::new(AsyncLane::default());
+                let lane = Arc::new(AsyncLane::new(key.clone(), Arc::downgrade(&self.lanes)));
                 lanes.insert(key, Arc::downgrade(&lane));
                 lane
             })
@@ -78,3 +77,66 @@
         Self::new()
     }
 }
+
+#[cfg(test)]
+mod tests {
+    use std::future::Future;
+    use std::task::Context;
+    use std::task::Poll;
+    use std::task::Waker;
+    use std::sync::Arc;
+
+    use qubit_id::Id;
+
+    use super::AsyncOrderingLanes;
+    use super::OrderingLaneKey;
+    use super::AsyncLane;
+
+    /// Verifies final release reclaims the entry without a later enqueue scan.
+    #[test]
+    fn test_final_guard_release_reclaims_registry_entry() {
+        let lanes = AsyncOrderingLanes::new();
+        let key = OrderingLaneKey::new("bench", Some("same-key"), Id::new(1));
+        let guard = match Box::pin(lanes.enqueue(key, ()))
+            .as_mut()
+            .poll(&mut Context::from_waker(Waker::noop()))
+        {
+            Poll::Ready(Some(guard)) => guard,
+            _ => panic!("first turn must acquire its lane"),
+        };
+        assert_eq!(lanes.lanes.lock().unwrap().len(), 1);
+        drop(guard);
+        assert_eq!(lanes.lanes.lock().unwrap().len(), 0);
+    }
+
+    /// Forces final Drop to wait while the same key receives a new allocation.
+    #[test]
+    fn test_old_lane_drop_preserves_recreated_key_allocation() {
+        let lanes = AsyncOrderingLanes::new();
+        let key = OrderingLaneKey::new("bench", Some("same-key"), Id::new(1));
+        let guard = match Box::pin(lanes.enqueue(key.clone(), ()))
+            .as_mut()
+            .poll(&mut Context::from_waker(Waker::noop()))
+        {
+            Poll::Ready(Some(guard)) => guard,
+            _ => panic!("first turn must acquire its lane"),
+        };
+        let mut registry = lanes.lanes.lock().unwrap();
+        let old_weak = registry.get(&key).unwrap().clone();
+        let old = old_weak.upgrade().unwrap();
+        drop(guard);
+        let dropping = std::thread::spawn(move || drop(old));
+        while old_weak.strong_count() != 0 {
+            std::thread::yield_now();
+        }
+        let replacement = Arc::new(AsyncLane::new(key.clone(), Arc::downgrade(&lanes.lanes)));
+        registry.insert(key.clone(), Arc::downgrade(&replacement));
+        drop(registry);
+        dropping.join().unwrap();
+        let registry = lanes.lanes.lock().unwrap();
+        assert_eq!(registry.get(&key).unwrap().as_ptr(), Arc::as_ptr(&replacement));
+        drop(registry);
+        drop(replacement);
+        assert!(lanes.lanes.lock().unwrap().is_empty());
+    }
+}
```


## Rejection diagnostics

### Scope and method

This is the EB-R01 T1 optimization-before measurement. Production code remains unchanged. The benchmark uses fixed synchronous and asynchronous SPI acknowledgements with `PublishVisibility::DestinationAdmissions`; it measures facade publication and receipt validation, excluding fixture construction and cleanup. It covers 1, 16, and 128 destinations, all-rejected and alternating accepted/rejected outcomes (the one-destination partial case is omitted), zero and one observer, sync and async. Each case checks admission outcome and observer call count. Each sample contains 1,000 publishes after two warmups; seven raw samples are emitted for every case.

### Environment and execution

- Worktree: `/data/working/qubit/rust-common/.worktrees/rs-event-bus/codex-eb-r01-r02-20261106`
- Source revision: `39bee718ed6396a6d8b0e633a854958832ecac89` (production code unchanged)
- OS / architecture: Linux 6.8.0-147-generic, x86_64 (`x86_64-unknown-linux-gnu`)
- CPU: Intel(R) Xeon(R) CPU E5-2680 v4 @ 2.40GHz; benchmark processes were not pinned to a CPU.
- Rust toolchain: `rustc 1.94.0 (4a4ef493e 2026-03-02)`, LLVM 21.1.8
- Fixtures inspected: discovery provider and documentation consumer both use edition 2024; discovery fixture enables `discovery` and `qubit-spi/inventory`; documentation fixture uses `tokio` 1.53. Discovery fixture lockfile is format version 4.
- Cargo version: `cargo 1.94.0 (85eff7c80 2026-01-15)`
- Command: `cargo bench --locked --bench rejection_diagnostics`
- First attempt: `cargo bench --locked --bench rejection_diagnostics` failed with exit code 101 during compilation. The compiler reported that `SpiError` must be imported from the crate root and that `PublishRequest<u64>` is not `Clone` (the benchmark borrowed and cloned a request).
- Correction: import `SpiError` from `qubit_event_bus`; preconstruct one independently owned request batch per warmup/sample and consume each batch during timing. Request construction remains outside the measured region.
- Initial corrected run: completed with exit code 0, but emitted two `unused_must_use` warnings for benchmark receipt outcome handling. Both were corrected to `let _ = black_box(...)`, and the benchmark was rerun afterward.
- Warning-clean run: `cargo bench --locked --bench rejection_diagnostics` exited 0 with no warnings and all 20 cases. The bench subscription stubs were subsequently changed from panic to explicit unsupported SPI errors as required by the plan, then candidate and seven paired benchmark runs were executed against that final bench source.

### Raw results

The initial corrected run before warning cleanup emitted the following 20 lines (each line is raw executable output):

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1344160, 1363090, 1347987, 1351763, 1350252, 1348744, 1344702] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1173371, 1171871, 1172840, 1169973, 1181565, 1177701, 1176561] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1417907, 1422765, 1419641, 1424004, 1421370, 1420902, 1415706] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1252976, 1254699, 1260472, 1253501, 1255161, 1274255, 1260377] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3523421, 3463194, 3450049, 3452508, 3451202, 3463267, 3457202] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3256258, 3274786, 3249195, 3262237, 3260550, 3276275, 3258577] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3665645, 3684107, 3682930, 3666978, 3675441, 3681809, 3681601] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3452945, 3453896, 3461257, 3464161, 3486600, 3471330, 3498575] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2594843, 2614480, 2601208, 2613846, 2604283, 2592993, 2611169] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2408339, 2403679, 2397478, 2416051, 2407250, 2452749, 2428802] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2750502, 2732812, 2751244, 2749556, 2733779, 2750448, 2753591] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2546365, 2558257, 2539127, 2603805, 2540265, 2551115, 2536751] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[24680130, 24605655, 25061057, 25687706, 24618423, 24811007, 24868929] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[23866903, 23630939, 23586041, 23650402, 23642502, 23652403, 23648923] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24903517, 25599880, 25173068, 24897298, 25363576, 25329985, 25672849] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25022806, 24802426, 24780220, 24767862, 24752785, 25272796, 24847814] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15879511, 15939672, 17074845, 16442436, 17496870, 16658763, 16439815] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[17362906, 19828416, 16391822, 16456916, 15704957, 16973065, 15848122] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[21438683, 21569464, 22749172, 23906414, 23867963, 22863991, 17316448] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16209987, 16132239, 16088352, 16118396, 16104704, 16099772, 16205513] iterations=1000 boundary=facade_publish_and_receipt_validation
```

### Interpretation

### Interleaved baseline/candidate comparison

Seven process-level pairs were run in baseline→candidate order. Each command was `cargo bench --locked --bench rejection_diagnostics` and exited 0; each process performed two warmups and emitted seven raw batch samples for all 20 cases. The table reports the median of each process's seven batch samples, then the median across seven paired process runs; ranges are the min–max of those seven process medians. Units are nanoseconds per 1,000 facade publishes. The first run of each worktree was a compilation/warm-up run and is not included in the paired set. Baseline production source is `39bee718ed6396a6d8b0e633a854958832ecac89`; candidate adds only the `observers.is_empty()` early return in `PublisherPipeline::emit_rejections`. The final bench source was identical in both worktrees.

| Case | Baseline median (range) | Candidate median (range) |
| --- | ---: | ---: |
| async / 1 destinations / all rejected / 0 observer(s) | 1296292 (1256149–1395329) | 1144728 (1128959–1157230) |
| async / 1 destinations / all rejected / 1 observer(s) | 1368039 (1330513–1398178) | 1335984 (1309981–1353470) |
| async / 128 destinations / all rejected / 0 observer(s) | 24174142 (24018811–24325537) | 13158246 (13112057–13232117) |
| async / 128 destinations / all rejected / 1 observer(s) | 24810381 (24682462–25253785) | 25143529 (25044801–25476667) |
| async / 128 destinations / partial / 0 observer(s) | 15997945 (15889658–16115532) | 10472130 (10420575–10542621) |
| async / 128 destinations / partial / 1 observer(s) | 16390234 (16136322–16669708) | 16492274 (16406248–16709814) |
| async / 16 destinations / all rejected / 0 observer(s) | 3420614 (3385987–3473626) | 1978420 (1948220–1993034) |
| async / 16 destinations / all rejected / 1 observer(s) | 3565280 (3528399–3599424) | 3587079 (3566542–3625605) |
| async / 16 destinations / partial / 0 observer(s) | 2541578 (2516225–2562414) | 1788735 (1757306–1810593) |
| async / 16 destinations / partial / 1 observer(s) | 2658060 (2651600–2716189) | 2656321 (2629547–2674535) |
| sync / 1 destinations / all rejected / 0 observer(s) | 1371019 (1330845–2853216) | 1231304 (1214674–1294556) |
| sync / 1 destinations / all rejected / 1 observer(s) | 1414716 (1383320–1717851) | 1401018 (1380753–1426010) |
| sync / 128 destinations / all rejected / 0 observer(s) | 24149821 (24057395–38582561) | 13072453 (13025731–13309106) |
| sync / 128 destinations / all rejected / 1 observer(s) | 24755658 (24615339–25086528) | 25152431 (25068385–25366101) |
| sync / 128 destinations / partial / 0 observer(s) | 16103804 (15990639–20424298) | 10369547 (10247240–10434703) |
| sync / 128 destinations / partial / 1 observer(s) | 16504657 (16334322–17333588) | 16607320 (16527408–16750331) |
| sync / 16 destinations / all rejected / 0 observer(s) | 3504919 (3485704–3619358) | 2084986 (2063868–2100346) |
| sync / 16 destinations / all rejected / 1 observer(s) | 3662552 (3641037–3965650) | 3664231 (3638309–3801247) |
| sync / 16 destinations / partial / 0 observer(s) | 2641051 (2609606–2685245) | 1893054 (1873483–1918043) |
| sync / 16 destinations / partial / 1 observer(s) | 2752062 (2718785–2794018) | 2762287 (2733381–2789938) |

#### Retained raw samples

##### Pair 1 — baseline

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1357263, 1456659, 1363973, 1361289, 1366011, 1367500, 1361136] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1299179, 1292708, 1295001, 1296292, 1312529, 1307785, 1295752] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1419362, 1414012, 1432349, 1414164, 1421568, 1458348, 1509668] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1439027, 1367152, 1368039, 1402045, 1358224, 1449755, 1367186] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3515186, 3487271, 3513344, 3612884, 3498456, 3476826, 3504919] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3437987, 3436717, 3430657, 3434892, 3438852, 3422514, 3426408] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3705960, 3676581, 3641037, 3637062, 3943925, 3624738, 3636551] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3753347, 3879761, 3580539, 3585179, 3574530, 3588996, 3573462] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2593435, 2729383, 2629313, 2598813, 3252175, 2612368, 2621750] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2541563, 2538711, 2553069, 2529092, 2540891, 2599646, 2532400] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2718785, 2817545, 2714576, 2721330, 2706203, 2717759, 2724674] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2640468, 2651600, 2659869, 2643184, 2659531, 2652358, 2648656] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[24149131, 24066832, 24314339, 24181843, 24322651, 24063151, 24265834] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[24143439, 24223472, 24066197, 24076271, 24151568, 24241208, 24163244] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25086528, 25090268, 24953150, 25402073, 25094294, 25079657, 25005830] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24735252, 24765100, 25105642, 24888832, 24896590, 24954027, 25098080] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[16062585, 16077731, 16103804, 16122224, 16144350, 16083274, 16569767] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[16064695, 16138916, 16025707, 16013310, 16034834, 16063455, 16109165] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16431412, 16649807, 16641396, 16543274, 16709883, 16424018, 16503074] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16949970, 17576431, 16657083, 16767853, 16669708, 16582999, 16535579] iterations=1000 boundary=facade_publish_and_receipt_validation
```

##### Pair 1 — candidate

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1274083, 1239684, 1318472, 1290339, 1253568, 1254357, 1243595] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1156645, 1156882, 1161344, 1165270, 1151898, 1150808, 1160668] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1476199, 1402018, 1409777, 1422100, 1399678, 1414181, 1405423] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1327794, 1344634, 1327747, 1334104, 1348607, 1338383, 1345999] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[2117509, 2153852, 2139425, 2100346, 2094538, 2084244, 2090321] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[1989949, 1984955, 1993034, 1989089, 1993678, 1994262, 2002720] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3664101, 3715590, 3677112, 3664231, 3649111, 3663110, 3680315] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3602597, 3582048, 3600461, 3598419, 3607005, 3609374, 3670134] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1915338, 1903271, 1911017, 1904961, 1922765, 1899185, 1905434] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1810593, 1802689, 1818105, 1813552, 1817891, 1810386, 1803718] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2762287, 2754422, 2775807, 2869021, 2790748, 2761086, 2761515] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2677284, 2662227, 2666925, 2666364, 2676372, 2721971, 2670419] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13127613, 13093188, 13027109, 13115604, 13039373, 13058124, 13072453] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13112057, 13064954, 13079022, 13077777, 13126923, 13265245, 13385387] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25116928, 25169025, 25152431, 25062018, 25072139, 25199160, 25166647] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25373822, 25185346, 25115819, 25093647, 25476974, 25375942, 26374341] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10683182, 10637744, 10373961, 10489338, 10434703, 10382978, 10352913] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10587874, 10613255, 10535998, 10461530, 10556985, 10527074, 10542621] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16561154, 16671934, 16552656, 16669736, 16778577, 16647787, 16552328] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16427572, 16632123, 16492274, 16539038, 16508747, 16477633, 16474473] iterations=1000 boundary=facade_publish_and_receipt_validation
```

##### Pair 2 — baseline

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1329374, 1329759, 1345540, 1338889, 1342034, 1360533, 1346603] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1279097, 1274785, 1282687, 1267897, 1269017, 1281431, 1269416] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1401593, 1421718, 1401113, 1390034, 1409534, 1393256, 1412736] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1342250, 1346534, 1353877, 1355892, 1344589, 1366503, 1347925] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3485244, 3474807, 3516294, 3512734, 3512491, 3489320, 3510184] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3441409, 3545636, 3520341, 3552316, 3472282, 3473626, 3469952] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3644203, 3646402, 3647583, 3659031, 3657079, 3675353, 3688837] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3541756, 3571064, 3557493, 3580162, 3556175, 3551360, 3565385] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2617870, 2608710, 2618850, 2633688, 2631423, 2647326, 2639580] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2541578, 2513420, 2548599, 2533014, 2570591, 2526361, 2585925] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2711304, 2743301, 2758766, 2756477, 2750491, 2752664, 2745373] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2671791, 2645298, 2648844, 2660288, 2648682, 2667396, 2658060] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[24076560, 24104040, 24137290, 24121279, 24095569, 24110772, 24375730] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[24149829, 23998105, 24125546, 24329123, 24655295, 24334228, 24287291] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24654041, 24577418, 24601042, 24727557, 24615339, 24587634, 24766263] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24779104, 25067251, 24742453, 24863734, 24713946, 24807478, 24788245] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15980204, 15981973, 16126670, 16056924, 15975311, 16086048, 15990639] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15997945, 16025888, 16028417, 16135714, 15958231, 15916198, 15975829] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16473468, 16444987, 16648932, 16462447, 16466732, 16439747, 16369224] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16463756, 16544439, 16479289, 16460130, 16545889, 16484391, 16453436] iterations=1000 boundary=facade_publish_and_receipt_validation
```

##### Pair 2 — candidate

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1220963, 1215127, 1215441, 1216195, 1220635, 1214457, 1211665] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1135863, 1132422, 1124572, 1126635, 1132554, 1128959, 1127788] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1383626, 1381635, 1370250, 1385737, 1389349, 1383756, 1377435] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1312301, 1305908, 1321255, 1314419, 1309981, 1304982, 1302745] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[2083576, 2072625, 2071040, 2068758, 2074279, 2069171, 2071622] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[1939459, 1935519, 1948220, 2032573, 2080447, 1945159, 1957829] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3656195, 3654121, 3661675, 3651787, 3648543, 3657159, 3674534] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3554535, 3551628, 3563600, 3573007, 3567967, 3566542, 3577092] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1878078, 1943543, 1882238, 1880924, 1881073, 1892771, 1892825] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1757123, 1751264, 1758366, 1748594, 1757306, 1762577, 1758502] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2722889, 2714768, 2718985, 2836421, 2808766, 2755571, 2767305] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2644845, 2635602, 2636472, 2640210, 2633858, 2626179, 2717451] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13053551, 13076358, 13007894, 13022278, 13018914, 13072782, 13025731] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13230915, 13184714, 13220183, 13206304, 13259456, 13247938, 13227403] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25000395, 25032791, 25018252, 25137168, 25110099, 25095122, 25068385] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25061291, 25067440, 25113244, 25063689, 25106064, 25103436, 25180393] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10351854, 10368558, 10333162, 10361207, 10469482, 10400558, 10397091] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10446751, 10463946, 10440133, 10491252, 10564874, 10456020, 10500871] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16428705, 16492027, 16561949, 16527408, 16532658, 16524985, 16579630] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16515150, 16554533, 16519265, 16535412, 16520488, 16587288, 16589590] iterations=1000 boundary=facade_publish_and_receipt_validation
```

##### Pair 3 — baseline

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[2853216, 2507281, 2912999, 2901646, 2888850, 2695940, 2358935] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1289275, 1283341, 1283347, 1307694, 1291679, 1287507, 1294024] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1404915, 1400791, 1390888, 1403315, 1400341, 1383662, 1399052] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1359285, 1359853, 1360942, 1372682, 1367973, 1389751, 1370689] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3462236, 3478173, 3494895, 3497342, 3484556, 3485704, 3502442] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3420614, 3405379, 3411281, 3421737, 3448390, 3406547, 3458692] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3666663, 3645619, 3651093, 3673078, 3657409, 3683089, 3662552] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3557503, 3555165, 3567003, 3546165, 3558574, 3586371, 3568478] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2653697, 2650189, 2652050, 2643901, 2661412, 2655587, 2641670] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2525927, 2529858, 2563862, 2562243, 2526787, 2554603, 2551003] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2757122, 2750478, 2767632, 2742243, 2749069, 2752062, 2778045] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2655322, 2761410, 2664395, 2665840, 2671320, 2671126, 2667472] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[24018442, 24013227, 24077108, 23974558, 24057395, 24122687, 24122235] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[24357037, 24210726, 24243906, 24208232, 24230761, 24234898, 24211296] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24655182, 24648033, 24916872, 25011095, 24705893, 24780258, 24755658] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24742699, 24727331, 24750694, 24690859, 26142286, 25322078, 24927538] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[16010346, 16038360, 15977678, 15958308, 16019879, 16026600, 15985692] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15954920, 15953299, 15930040, 15921809, 15938333, 15944868, 15955817] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16495321, 16403356, 16381602, 16334322, 16327976, 16315532, 16334018] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16272447, 16369251, 16254110, 16270109, 16246861, 16273000, 16243770] iterations=1000 boundary=facade_publish_and_receipt_validation
```

##### Pair 3 — candidate

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1233622, 1231304, 1229050, 1237605, 1233135, 1229167, 1225716] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1151543, 1145804, 1144728, 1145190, 1141578, 1142362, 1142018] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1401018, 1395876, 1405351, 1400118, 1391031, 1428391, 1407308] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1320484, 1327433, 1317002, 1319514, 1322758, 1319013, 1318826] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[2081958, 2072172, 2070557, 2066186, 2071695, 2073485, 2068905] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[1961640, 1959363, 1972519, 1959938, 1961169, 1959237, 1966421] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3647392, 3720791, 3636833, 3650610, 3635112, 3650515, 3701400] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3598458, 3579960, 3575249, 3578286, 3575996, 3584375, 3563803] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1892161, 1888696, 1891225, 1887479, 1893049, 1894141, 1892959] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1769398, 1771674, 1777163, 1770702, 1809217, 1805654, 1796406] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2746336, 2750922, 2752976, 2744282, 2755345, 2751393, 2761127] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2655077, 2646786, 2665815, 2653439, 2647512, 2654832, 2656193] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13085473, 13111949, 13134236, 13145889, 13074584, 13109081, 13146075] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13166586, 13139476, 13157757, 13079118, 13123483, 13176357, 13166764] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25200567, 25188925, 25198760, 25218171, 25200967, 25264238, 25272102] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25068725, 25092565, 25080896, 25044801, 25031656, 25004356, 25037355] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10234504, 10247240, 10233026, 10243623, 10344163, 10474525, 10401572] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10401623, 10439633, 10410685, 10426254, 10415482, 10420575, 10430469] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16493366, 16561040, 16620053, 16607320, 16586924, 16673826, 22362876] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16420720, 16422136, 16514035, 16545619, 16471874, 16443592, 16441963] iterations=1000 boundary=facade_publish_and_receipt_validation
```

##### Pair 4 — baseline

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1375701, 1366090, 1386448, 1381079, 1382417, 1410422, 1354703] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1314637, 1347464, 1329306, 1342930, 1315525, 1336351, 1314040] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1414735, 1403691, 1428980, 1446143, 1404316, 1473485, 1424108] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1413193, 1390211, 1391497, 1398178, 1386972, 1411956, 1405516] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3494052, 3486478, 3546888, 3490446, 3547114, 3494771, 3541659] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3409227, 3412236, 3432987, 3442367, 3411355, 3408073, 3409211] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3965650, 3861447, 4067386, 4034135, 3961567, 3939074, 4098155] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3567646, 3716363, 3653616, 3596945, 3594304, 3590317, 3585451] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2650465, 2629825, 2640204, 2626482, 2641051, 2833373, 2696488] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2539208, 2539358, 2543625, 2557592, 2541627, 2555597, 2541813] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2751734, 2761706, 2775944, 2744719, 2754164, 2759402, 2756488] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[3042776, 3045598, 2686861, 2673007, 2672004, 2691190, 2674186] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[24113773, 24112270, 24326386, 24474919, 24056459, 24212556, 24149821] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[24329457, 24126847, 24187145, 24205669, 24165411, 24057812, 24042614] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24676507, 24580331, 24680424, 24649962, 24603342, 24664205, 24660709] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24802974, 24810381, 24789913, 25159154, 24779008, 24988730, 24839719] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[16038210, 16060609, 16095707, 16014296, 15999289, 16019823, 16023796] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15895064, 15907676, 15895294, 15882351, 15879256, 15889658, 15880492] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16448157, 16478423, 16485423, 16503070, 16506661, 16476121, 16440464] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16291677, 16193565, 16174549, 16226248, 16263873, 16202846, 16178820] iterations=1000 boundary=facade_publish_and_receipt_validation
```

##### Pair 4 — candidate

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1217045, 1219936, 1218795, 1222163, 1218531, 1215332, 1224425] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1141734, 1138198, 1133177, 1137822, 1134145, 1137625, 1140203] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1394481, 1394145, 1388624, 1395046, 1380898, 1392832, 1378332] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1318759, 1315609, 1317425, 1315876, 1313774, 1322420, 1313214] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[2068678, 2079673, 2167084, 2309975, 2155758, 2092952, 2092810] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[1984092, 1980990, 1989839, 1998252, 1995646, 1988949, 1995529] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3917590, 3935492, 3795526, 3672664, 3701336, 3801247, 3829432] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3707736, 3647516, 3697228, 3577736, 3585975, 3571102, 3580769] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1897436, 1902528, 1900797, 1904898, 1907038, 1903112, 1994922] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1837156, 1777434, 1791816, 1782855, 1945898, 1798958, 1791391] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2769463, 3090937, 2867740, 2897840, 2755636, 2745095, 2743095] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2627351, 2638299, 2761252, 2629547, 2622985, 2615928, 2631354] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13018348, 13007613, 13221040, 13407328, 13034132, 13033355, 13039578] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13175753, 13158143, 13169576, 13163331, 13211232, 13260790, 13206465] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[27714959, 25317994, 25185812, 25195498, 25291698, 25263121, 25249003] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25060230, 25064373, 25063318, 25054485, 25079274, 25046733, 25092775] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10338878, 10338155, 10340009, 10353130, 10357758, 10379361, 10388393] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10412764, 10429105, 10432559, 10473008, 10435719, 10423607, 10421118] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16540017, 16533651, 16496714, 16507462, 16560433, 16554561, 16524372] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16398629, 16406248, 16426751, 16393308, 16405906, 16418925, 16412464] iterations=1000 boundary=facade_publish_and_receipt_validation
```

##### Pair 5 — baseline

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1328388, 1331087, 1315896, 1341437, 1329500, 1330845, 1348355] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1256149, 1260282, 1259891, 1254860, 1254683, 1262320, 1245757] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1383320, 1392613, 1383800, 1376237, 1385036, 1376715, 1381199] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1325487, 1331762, 1322887, 1325845, 1330513, 1331031, 1355372] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3472944, 3475588, 3488886, 3503890, 3651267, 3501929, 3482223] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3363906, 3382512, 3385987, 3391477, 3396016, 3383220, 3404711] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3631788, 3639605, 3635813, 3660638, 3657889, 3666316, 3673937] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3528399, 3515045, 3515759, 3521636, 3535348, 3536669, 3555187] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2612200, 2609606, 2613087, 2607698, 2609329, 2624149, 2605487] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2511913, 2516225, 2510613, 2540242, 2501679, 2521696, 2529029] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2729242, 2709979, 2737391, 2900509, 2824870, 2726055, 2760174] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2642120, 2662486, 2687148, 2665430, 2653380, 2645607, 2631456] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[24105182, 24037730, 24120753, 23978663, 24169013, 24168840, 24163189] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[24042609, 23942323, 23972519, 24021095, 24018811, 24014386, 24020166] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24588892, 24557208, 24618172, 24603432, 24622254, 24766854, 24706875] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24699279, 24804131, 24712529, 24663083, 24609244, 24682307, 24682462] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[16032625, 16199433, 16168257, 15975523, 15999539, 16768130, 16238730] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15899140, 15911459, 15888411, 15921195, 15916810, 15949602, 16609428] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16395810, 16419423, 16418484, 18447356, 16618898, 17284608, 16504657] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16196619, 16127142, 16121800, 16136322, 16142297, 16128301, 16140725] iterations=1000 boundary=facade_publish_and_receipt_validation
```

##### Pair 5 — candidate

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1216815, 1208720, 1214674, 1223121, 1212801, 1216903, 1213023] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1155806, 1154350, 1153173, 1155094, 1181169, 1153465, 1152258] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1380753, 1376070, 1383573, 1383196, 1390548, 1380205, 1374348] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1345981, 1335745, 1335984, 1340750, 1334186, 1333074, 1340180] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[2065000, 2068264, 2064028, 2063868, 2063813, 2056718, 2060737] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[1994274, 1989964, 1994196, 1984582, 1994292, 1987516, 1992141] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3630290, 3638309, 3672405, 3661107, 3651762, 3637610, 3635849] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3628365, 3616774, 3618745, 3619182, 3615114, 3610840, 3615675] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1877430, 1873483, 1871869, 1871413, 1877468, 1872438, 1881554] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1777221, 1790243, 1793448, 1785446, 1788735, 1780478, 1794022] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2755101, 2740362, 2735075, 2719502, 2724215, 2726123, 2733381] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2672044, 2674535, 2674030, 2658125, 2707527, 2684143, 2676283] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13063228, 13135179, 13128512, 17581037, 13349451, 13230779, 13085656] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13110050, 13612817, 13211267, 13121501, 13158246, 13138740, 13169156] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24978214, 25343041, 25203089, 25107133, 25095481, 25052552, 25051704] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25389001, 25429356, 25437028, 25476667, 25479102, 25567067, 25493263] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10369547, 10345909, 10340044, 10363388, 10383835, 10395147, 10395272] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10525311, 10530554, 10506430, 10522159, 10486633, 10482527, 10522115] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16797181, 16592237, 16664206, 16668742, 16653737, 16616061, 16620181] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16633824, 16650685, 16674850, 16649472, 16723938, 17180460, 17273941] iterations=1000 boundary=facade_publish_and_receipt_validation
```

##### Pair 6 — baseline

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1552003, 1645134, 1579094, 1509399, 2388317, 2099967, 1644308] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1439505, 1395329, 1301892, 1619852, 1442590, 1328331, 1345632] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[2026871, 1594566, 1646630, 1632238, 1717851, 2296594, 2641261] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1439812, 1379056, 1388251, 1347045, 1371713, 1384893, 1421951] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3567576, 3542453, 3619358, 3643640, 3580769, 3706419, 3646570] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3407009, 3506925, 3398198, 3466273, 3395281, 3403453, 3459080] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3710446, 3699556, 3705117, 3698899, 3771043, 3770613, 3780351] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3593934, 3561405, 3565280, 3572260, 3630585, 3561486, 3554810] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2673365, 2728517, 2676163, 2677764, 2685245, 2694066, 2728484] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2538443, 2550365, 2536799, 2534846, 2523393, 2532567, 2562782] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2797191, 2785599, 2804257, 2774372, 2775574, 2778993, 2781218] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2640184, 2678090, 2745605, 2643673, 2659186, 2652848, 2656902] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[27038083, 38582561, 42983707, 43862470, 42895580, 30389147, 28945388] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[25187074, 24549638, 24139361, 24077670, 24171771, 24174142, 24419226] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24967178, 25046934, 25049318, 24833732, 25056416, 25290020, 24953580] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25215321, 25359306, 25245929, 25114851, 26320893, 25446125, 25253785] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[16241189, 16400701, 16392634, 16314643, 16139413, 16513403, 17422010] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[16025755, 16052527, 16098668, 16563324, 16608984, 16115532, 16169207] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16519283, 16831749, 17147847, 17691205, 16763943, 16654289, 16720270] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[17540445, 16309292, 16386544, 16602800, 16438556, 16412161, 16258141] iterations=1000 boundary=facade_publish_and_receipt_validation
```

##### Pair 6 — candidate

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1562445, 1336947, 1290009, 1291433, 1361756, 1294556, 1260644] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1134508, 1133391, 1170921, 1151846, 1127536, 1149294, 1131508] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1443972, 1424483, 1423683, 1423288, 1400620, 1410506, 1396941] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1367319, 1353470, 1354864, 1341841, 1324868, 1326271, 1372285] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[2146043, 2078521, 2079769, 2087197, 2084986, 2081927, 2087960] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[1960317, 1957343, 1954180, 1945358, 2023181, 1955646, 1968297] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3777177, 3696083, 3734445, 3759868, 3693166, 3722195, 3674571] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3627112, 3587079, 3621085, 3576424, 3566707, 3568956, 3636031] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1887025, 1873945, 1895123, 1893015, 1893277, 1893054, 1893592] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1759418, 1758092, 1764266, 1768318, 2618473, 3891017, 3695606] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2789938, 2810198, 2823134, 2834316, 2785856, 2777329, 2767173] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2645746, 2656321, 2701449, 2716261, 2686141, 2642929, 2647062] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13612913, 13292860, 13398466, 13333511, 13148929, 13095144, 13309106] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13232633, 13185533, 13109547, 13102736, 13268402, 13232117, 13232523] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25304050, 25433986, 25424911, 25434852, 25361436, 25242500, 25366101] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25699336, 25414378, 25278666, 25333970, 25246194, 25165260, 25267687] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10307261, 10395097, 10399133, 10344019, 10383480, 10359514, 10414911] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10524690, 10563844, 10504689, 10498022, 10502841, 10605240, 10482445] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16653144, 16767456, 16662104, 16949299, 16943664, 16750331, 16719826] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16859284, 16694343, 16760201, 16635172, 16722213, 16604146, 16709814] iterations=1000 boundary=facade_publish_and_receipt_validation
```

##### Pair 7 — baseline

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1409868, 1368230, 1383276, 1356356, 1371019, 1391601, 1360566] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1301595, 1305572, 1284058, 1294338, 1292615, 1317502, 1313543] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1412585, 1399904, 1410309, 1415124, 1456929, 1467129, 1414716] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1379296, 1464534, 1393330, 1388297, 1405879, 1399831, 1355333] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3533649, 3533939, 3503467, 3555411, 3536033, 3548669, 3534855] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3441592, 3438617, 3449626, 3483159, 3458517, 3499404, 3437386] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3678885, 3684216, 3724326, 3693631, 3708506, 3709626, 3712102] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3625653, 3610381, 3598420, 3575495, 3614122, 3599424, 3584632] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2673659, 2671733, 2666736, 2651375, 2662017, 2686915, 2719516] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2567464, 2544478, 2562414, 2556366, 2547995, 2652560, 2568805] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2793327, 2763877, 3109992, 2794018, 2760436, 2864071, 2798041] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2697713, 2678069, 2716189, 2695072, 2718998, 3020123, 3174516] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[24628307, 24537940, 24416755, 24480773, 24474555, 24524257, 24716904] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[25354990, 24549550, 24144705, 24257019, 24325537, 24372470, 24312510] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25073372, 24980422, 24953091, 24903500, 25187042, 25152776, 25042699] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25130909, 24996754, 25081628, 24905479, 24851732, 25053472, 25068368] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[31723375, 28487021, 20424298, 21496797, 16560132, 17938221, 18012337] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[19647945, 17174977, 16623705, 16060224, 15965491, 16056711, 16044999] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[18233020, 18569764, 16794900, 16860760, 17053409, 17333588, 17852118] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[17206472, 16521329, 16376652, 16390234, 16382828, 16369847, 16407147] iterations=1000 boundary=facade_publish_and_receipt_validation
```

##### Pair 7 — candidate

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1264321, 1259975, 1258512, 1268065, 1263037, 1291057, 1286309] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1160715, 1157230, 1155387, 1169600, 1153945, 1155062, 1160296] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1444301, 1425892, 1428350, 1426010, 1428618, 1425817, 1415848] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1367926, 1345002, 1338201, 1373196, 1349091, 1348559, 1336112] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[2112552, 2093734, 2105554, 2116741, 2099678, 2097136, 2100272] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[1981265, 1973021, 1981445, 1977071, 1978420, 1978217, 1981573] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3698114, 3743882, 3688985, 3687888, 3689137, 3698750, 3682572] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3630800, 3609860, 3632997, 3625605, 3611209, 3633484, 3610139] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1923451, 1920358, 1939905, 1909044, 1912161, 1896667, 1918043] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1796053, 1785629, 1793490, 1783576, 1811139, 1795104, 1807828] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2775604, 2760510, 2788070, 2779932, 2774508, 2772869, 2776766] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2670223, 2663782, 2673665, 2685151, 2662954, 2677776, 2668126] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13039671, 13048351, 13072214, 13035935, 13051514, 13065289, 13188551] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13143415, 13102430, 13100478, 13098203, 13148930, 13144023, 13147413] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25086833, 25085159, 25077294, 25110975, 25103981, 25090441, 25089726] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25123226, 25108155, 25152476, 25159105, 25145891, 25114736, 25143529] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10379872, 10363403, 10376019, 10384537, 10405108, 10376291, 10406341] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10472130, 10464502, 10477698, 10534890, 10472152, 10458378, 10450314] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16486363, 16607335, 16575068, 16595846, 16589023, 16663779, 16629252] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16494374, 16463204, 16464848, 16446788, 16435799, 16498371, 16412293] iterations=1000 boundary=facade_publish_and_receipt_validation
```

#### Interpretation

For zero observers, the candidate removes per-rejection diagnostic construction. The paired medians show lower measured time in the 16- and 128-destination groups; one-destination differences are smaller. Observer-present cases are the control and remain in the same general range, though some samples show scheduler noise. This is benchmark evidence for this host and setup, not a portable performance guarantee; no timing assertion or CI threshold was added.

### Final-source interleaved comparison (authoritative)

The earlier interleaved section is retained as preliminary data from before the Clippy-driven benchmark source cleanup. The following final-source measurement supersedes it. Baseline production source is `39bee718ed6396a6d8b0e633a854958832ecac89`; candidate source adds only the empty-observer guard. Both sides used byte-identical final bench code (`benches/rejection_diagnostics.rs` SHA-256 `78eeac85c1ba24f6955153e348f490874eb56bc697129969b66c979b4269f05a`); candidate pipeline source SHA-256 was `4df7cfc3c64f8dd4da704c26440c95b944bf290a8873b57e20018bb449057b92`. Commands alternated baseline then candidate for seven pairs. All 14 commands exited 0 and produced all 20 cases. Each case contains seven timed batches of 1,000 publishes after two warmups. Table values are the median of seven samples within each process, then the median across seven process-level pair values; ranges are min–max of those seven process medians. Values are nanoseconds per 1,000 publishes.

| Case | Baseline median (range) | Candidate median (range) |
| --- | ---: | ---: |
| async / 1 destinations / all rejected / 0 observer(s) | 1238510 (1222706–1404215) | 1138031 (1113888–1183436) |
| async / 1 destinations / all rejected / 1 observer(s) | 1322257 (1306947–1495959) | 1322846 (1308870–1355239) |
| async / 128 destinations / all rejected / 0 observer(s) | 23905843 (23844376–25805543) | 12996169 (12928619–13024253) |
| async / 128 destinations / all rejected / 1 observer(s) | 24968053 (24920134–25138870) | 24774681 (24558344–24931509) |
| async / 128 destinations / partial / 0 observer(s) | 15726341 (15616202–15910141) | 10305549 (10245738–11769964) |
| async / 128 destinations / partial / 1 observer(s) | 16234333 (16145627–16470000) | 16190380 (16113131–16450716) |
| async / 16 destinations / all rejected / 0 observer(s) | 3344707 (3327471–3472184) | 1962264 (1932300–2005036) |
| async / 16 destinations / all rejected / 1 observer(s) | 3560545 (3539872–3737197) | 3551781 (3508396–3580835) |
| async / 16 destinations / partial / 0 observer(s) | 2493077 (2468691–2637734) | 1776701 (1749218–1823013) |
| async / 16 destinations / partial / 1 observer(s) | 2628388 (2608193–2765842) | 2628115 (2600508–2684276) |
| sync / 1 destinations / all rejected / 0 observer(s) | 1337171 (1315281–1359807) | 1229136 (1210722–1283695) |
| sync / 1 destinations / all rejected / 1 observer(s) | 1408366 (1393609–1431372) | 1410533 (1393590–1461510) |
| sync / 128 destinations / all rejected / 0 observer(s) | 23920907 (23728747–23973648) | 12968592 (12929732–13036382) |
| sync / 128 destinations / all rejected / 1 observer(s) | 24787715 (24761687–25003648) | 24821972 (24681442–27553877) |
| sync / 128 destinations / partial / 0 observer(s) | 15856188 (15780058–15991302) | 10216845 (10172560–10323766) |
| sync / 128 destinations / partial / 1 observer(s) | 16423984 (16337512–16501513) | 16260110 (16156679–17294987) |
| sync / 16 destinations / all rejected / 0 observer(s) | 3473318 (3432508–3503439) | 2101254 (2067065–2130279) |
| sync / 16 destinations / all rejected / 1 observer(s) | 3640622 (3619207–3702424) | 3663915 (3621877–3692501) |
| sync / 16 destinations / partial / 0 observer(s) | 2617337 (2578970–2630562) | 1885417 (1872841–1931123) |
| sync / 16 destinations / partial / 1 observer(s) | 2736555 (2701406–2754505) | 2745932 (2706875–2816782) |

#### Final-source raw samples

##### Pair 1 — baseline

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1328988, 1344978, 1341348, 1333384, 1334868, 1330188, 1330937] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1237892, 1236435, 1236703, 1247092, 1242232, 1233693, 1249110] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1405733, 1395314, 1393258, 1409239, 1394720, 1636599, 1402899] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1354082, 1337684, 1320758, 1321053, 1333391, 1322257, 1317828] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3450650, 3440519, 3445728, 3434982, 3457068, 3444568, 3463779] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3344707, 3326300, 3323413, 3414889, 3619155, 3416343, 3339941] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3613300, 3630950, 3625529, 3617243, 3645169, 3631868, 3646070] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3544950, 3542230, 3533230, 3550004, 3538226, 3548018, 3541016] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2582956, 2594298, 2579608, 2588135, 2586031, 2587548, 2597672] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2560709, 2484125, 2483023, 2496401, 2493077, 2499071, 2486165] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2720473, 2712761, 2729577, 2720839, 2725114, 2717367, 2735406] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2611657, 2626208, 2644868, 2644944, 2630988, 2617324, 2625783] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[23750847, 23775287, 23993435, 23726919, 23807508, 23850681, 23932952] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[24144755, 23796094, 23813099, 23864467, 23899342, 23846916, 23992386] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24806262, 24738828, 24812790, 24827003, 24775658, 24775510, 24786913] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24915630, 25047428, 24968053, 24942956, 25009217, 25111752, 24922146] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15889905, 15907411, 16427087, 15974165, 15857184, 15786241, 15784996] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15642664, 15636453, 15626187, 15699386, 15768082, 15705481, 15702684] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16240296, 16351679, 16446199, 16337512, 16363246, 16327366, 16319373] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16144162, 16196251, 16320918, 16106406, 16183719, 16270871, 16132954] iterations=1000 boundary=facade_publish_and_receipt_validation
```

##### Pair 1 — candidate

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1229538, 1235572, 1220370, 1222682, 1215585, 1211910, 1223865] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1113888, 1113633, 1113285, 1125189, 1112872, 1130592, 1124683] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1431297, 1410384, 1425742, 1408173, 1403868, 1426720, 1410533] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1316889, 1326318, 1310188, 1314158, 1320076, 1313882, 1306283] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[2084142, 2090518, 2096172, 2111179, 2087149, 2090753, 2090117] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[1939624, 1957714, 1945908, 1932834, 1938549, 1935371, 1937252] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3627172, 3637991, 3621877, 3626811, 3616171, 3618203, 3610036] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3505763, 3498801, 3517070, 3503523, 3518013, 3508396, 3512050] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1853852, 1859845, 1861749, 1879282, 1889347, 1888944, 1958589] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1755215, 1746244, 1757160, 1749626, 1750557, 1748218, 1753977] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2725094, 2823910, 2816782, 2819644, 2817561, 2799155, 2725029] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2601345, 2589724, 2615332, 2576230, 2600508, 2577290, 2616548] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[12941880, 12943407, 12946125, 12955782, 12989623, 13047313, 13045603] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[12962699, 12982319, 12961375, 12949706, 12970248, 12984996, 12980747] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24712003, 24930850, 24735091, 25482825, 24891853, 24806728, 24835774] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[26032862, 24701283, 24672332, 24633948, 24673686, 24700898, 24663617] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10172560, 10163571, 10164884, 10170932, 10210722, 10206250, 10229621] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10263990, 10262523, 10296293, 10269890, 10239289, 10215177, 10338290] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16183543, 16204754, 16215807, 16217708, 16331937, 16205115, 16239541] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16065892, 16079582, 16105307, 16117781, 16129794, 16139032, 16138150] iterations=1000 boundary=facade_publish_and_receipt_validation
```

##### Pair 2 — baseline

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1334672, 1347955, 1335697, 1343798, 1342547, 1334803, 1337171] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1222703, 1223955, 1219354, 1236490, 1221142, 1223515, 1222706] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1403872, 1419333, 1403573, 1410177, 1399012, 1397663, 1413460] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1312451, 1306947, 1309226, 1306662, 1301072, 1301551, 1309354] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3481662, 3473303, 3466393, 3473254, 3468887, 3468404, 3463560] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3325323, 3324046, 3332716, 3327471, 3328074, 3331963, 3322800] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3636225, 3640622, 3628982, 3641045, 3679199, 3650669, 3635429] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3634869, 3539872, 3534890, 3528723, 3542041, 3536616, 3685687] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2634846, 2632386, 2635496, 2625709, 2621941, 2630562, 2628893] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2473564, 2473330, 2471909, 2490164, 2474119, 2472927, 2476090] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2738357, 2741073, 2736555, 2723017, 2734533, 2731738, 2737763] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2614483, 2606596, 2621092, 2608853, 2621951, 2625837, 2620140] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[23893110, 23920907, 23892285, 23875026, 23960272, 23969811, 23982287] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[23905843, 23769487, 23911177, 23853158, 23874709, 23990830, 23984956] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24925081, 24782765, 24829792, 24891831, 24815084, 24984991, 25009199] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24914665, 24893889, 24920134, 24881289, 26487272, 25048154, 24938199] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15874038, 15912386, 15902445, 15893246, 16030432, 15896627, 15898720] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15597936, 15615759, 15641516, 15812416, 15591079, 15616202, 15629500] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16452185, 16501513, 16501862, 16634459, 16468270, 16503742, 16449057] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16146322, 16145627, 16281706, 16116548, 16156839, 16119417, 16137364] iterations=1000 boundary=facade_publish_and_receipt_validation
```

##### Pair 2 — candidate

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1222810, 1210172, 1210929, 1214812, 1209017, 1208281, 1210722] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1183436, 1187956, 1176082, 1184363, 1180116, 1180291, 1188475] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1407064, 1392776, 1415128, 1397693, 1393115, 1404213, 1389962] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1353197, 1362658, 1350120, 1355239, 1358333, 1355611, 1351966] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[2067065, 2072383, 2073951, 2061481, 2065083, 2069522, 2064608] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[2003811, 2007703, 2005036, 2000120, 2000697, 2005332, 2007709] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3617416, 3643602, 3624340, 3617979, 3630911, 3616265, 3623977] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3576664, 3581227, 3567861, 3580835, 3600327, 3611893, 3577484] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1885723, 1872767, 1877598, 1989858, 1897614, 1885417, 1878781] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1812571, 1820223, 1834589, 1824908, 1828055, 1819834, 1823013] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2715685, 2704289, 2726582, 2702886, 2699364, 2706875, 2713756] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2687757, 2684276, 2678976, 2729907, 2676371, 2675437, 2697102] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[12926839, 12894596, 12901860, 12931441, 13216090, 13136362, 13043729] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13024253, 13023798, 13054935, 13003125, 13056970, 13037998, 13024216] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24747818, 24704739, 24698347, 24802272, 24850967, 24752099, 24801193] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24820293, 24864436, 25014116, 24895881, 24886684, 24924484, 25076914] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10171097, 10159092, 10196258, 10169989, 10265380, 10211713, 10268218] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10305549, 10297168, 10276747, 10281034, 10356244, 10416687, 10617466] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16099660, 16142701, 16200961, 16178663, 16164137, 16178024, 16198598] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16231888, 16329029, 16170886, 16182040, 16135962, 16216058, 16205159] iterations=1000 boundary=facade_publish_and_receipt_validation
```

##### Pair 3 — baseline

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1309848, 1309356, 1326285, 1315281, 1311851, 1315748, 1323783] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1404215, 1390468, 1407983, 1405599, 1399476, 1415538, 1396708] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1410162, 1409539, 1414352, 1417318, 1408719, 1403480, 1415924] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1480627, 1485399, 1495959, 1496392, 1496387, 1515484, 1480811] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3481380, 3473318, 3461020, 3454782, 3515327, 3506922, 3464057] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3472627, 3472184, 3470130, 3521724, 3478174, 3472180, 3464167] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3619207, 3624731, 3603044, 3617397, 3623582, 3613474, 3751984] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3737197, 3740068, 3769682, 3745581, 3714078, 3715139, 3711582] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2574995, 2578970, 2573156, 2581183, 2582974, 2592733, 2574977] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2641932, 2633133, 2639013, 2640954, 2637734, 2637513, 2633057] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2701807, 2703952, 2701406, 2712710, 2697178, 2692831, 2700306] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2754979, 2765842, 2770277, 2764495, 2759543, 2765887, 2768231] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[23880966, 23903472, 23979707, 23864023, 23997883, 23949586, 25402706] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[24306712, 24019934, 24032530, 24029824, 24083003, 24075345, 24092051] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24762320, 25813435, 24757912, 24781142, 24787715, 24812964, 24860506] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25088030, 25138870, 25303849, 25243681, 25143142, 25132414, 25133179] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15788565, 15813822, 15860667, 15802673, 15917053, 15834416, 15836747] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15876524, 15910141, 15911909, 15906864, 15895534, 15970227, 16022268] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16322875, 16386847, 16459518, 16436618, 16393690, 16365563, 16351352] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16592538, 16499069, 16470000, 16468617, 16441408, 16501020, 16454062] iterations=1000 boundary=facade_publish_and_receipt_validation
```

##### Pair 3 — candidate

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1229149, 1227253, 1229136, 1227512, 1238709, 1229751, 1225536] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1121305, 1119783, 1118922, 1120783, 1127237, 1118743, 1118867] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1415005, 1415816, 1421711, 1409763, 1422973, 1416722, 1413653] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1306450, 1310037, 1300933, 1308870, 1310948, 1308942, 1298991] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[2073847, 2084088, 2070028, 2069607, 2066850, 2066069, 2069035] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[1935012, 1967154, 1935632, 1932275, 1932300, 1924243, 1931801] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3627375, 3631516, 3626040, 3619183, 3625151, 3611825, 3623385] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3517422, 3513908, 3545964, 3514109, 3541142, 3535882, 3524116] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1866216, 1868320, 1873803, 1872841, 1874751, 1871356, 1875251] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1747837, 1749475, 1744073, 1747400, 1749218, 1753419, 1750629] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2729976, 2717443, 2722356, 2733212, 2722488, 2720339, 2751311] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2636226, 2628115, 2617850, 2639098, 2643047, 2618152, 2623439] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13063870, 12890288, 12897405, 12929732, 12959340, 12887137, 12970881] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[12933906, 12911183, 12928619, 12878704, 12918769, 12947392, 12934833] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24681442, 24647029, 24642623, 24657643, 24724578, 24695253, 24744122] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24558344, 24552632, 24538330, 24563302, 24577465, 24557290, 24582313] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10205284, 10202513, 10205815, 10214596, 10253261, 10240272, 10236848] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10244277, 10259052, 10244346, 10266213, 10245738, 10239361, 10251731] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16101197, 16136800, 16138932, 16185156, 16159182, 16156679, 16178694] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16118049, 16113131, 16095170, 16340551, 16135429, 16106084, 16057867] iterations=1000 boundary=facade_publish_and_receipt_validation
```

##### Pair 4 — baseline

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1330761, 1322428, 1321133, 1322681, 1343546, 1324090, 1322028] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1229706, 1220739, 1224968, 1232214, 1236666, 1229873, 1231954] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1393609, 1399793, 1395906, 1393172, 1386145, 1389913, 1397074] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1311217, 1311697, 1305616, 1314477, 1306023, 1309214, 1308457] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3432508, 3433021, 3431755, 3426425, 3519890, 3426214, 3531380] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3325306, 3340837, 3345965, 3328027, 3331508, 3331332, 3351495] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3607590, 3651673, 3660808, 3617487, 3596657, 3620025, 3654165] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3554370, 3539932, 3568164, 3584621, 3548464, 3568219, 3559219] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2607452, 2653984, 2588282, 2576228, 2584513, 2582216, 2573084] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2491897, 2461909, 2468691, 2470030, 2471982, 2460577, 2461305] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2708110, 2699975, 2710043, 2706044, 2695708, 2704680, 2702754] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2610321, 2600070, 2608193, 2613058, 2599804, 2612135, 2602570] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[23689193, 23689865, 23728747, 23649759, 23802235, 24834079, 24503223] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[37828141, 27722891, 25381042, 25805543, 26592944, 24240382, 24734060] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24857243, 24978092, 25398871, 25062718, 25043667, 24890413, 25003648] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25036714, 24972746, 24985398, 25061512, 25004365, 25125874, 25135441] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[16029198, 15991302, 15948723, 16025041, 15995675, 15863439, 15908454] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15725173, 15709650, 15891712, 15666657, 15694733, 15994836, 15652942] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16469741, 16437534, 16465842, 16516747, 16524505, 16441953, 16409323] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16378174, 16215315, 16299862, 16524990, 16760034, 16537385, 16294469] iterations=1000 boundary=facade_publish_and_receipt_validation
```

##### Pair 4 — candidate

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1264078, 1253008, 1253152, 1257005, 1263838, 1254720, 1265204] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1158125, 1164649, 1163503, 1169984, 1162161, 1162152, 1159696] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1428864, 1462335, 1456270, 1427490, 1425758, 1432879, 1423443] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1394328, 1351474, 1331037, 1394552, 1377848, 1335489, 1333016] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[2101254, 2097450, 2111004, 2123158, 2146437, 2089303, 2092313] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[1977942, 1970024, 1988780, 1975353, 1984283, 1976143, 1979019] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3676318, 3782734, 3681206, 3655709, 3671264, 3711629, 3734674] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3553264, 3547949, 3610309, 3566278, 3579098, 3569469, 3560738] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1897805, 1888369, 1928800, 1898391, 1890386, 1881978, 1895553] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1781739, 1776173, 1780876, 1782869, 1788702, 1801822, 1781334] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2748410, 2733299, 2723149, 2731701, 2730748, 2735307, 2745555] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2626467, 2628840, 2633455, 2623085, 2626610, 2628436, 2635114] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[12976960, 13010245, 13036382, 13033072, 13093942, 13202143, 13140570] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[12992220, 13107874, 13003992, 13064932, 13019878, 12983666, 13045360] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24817373, 24791343, 24859076, 24863242, 24977168, 24876779, 24874863] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24740206, 24722462, 24834924, 25084891, 24993477, 24931509, 25075251] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10256433, 10252114, 10298780, 10236399, 10331723, 10380052, 10462904] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10293073, 10316616, 10334274, 10308193, 10296724, 10307914, 10331264] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16321941, 16300016, 16261566, 16264105, 16503482, 16299239, 16332832] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16449540, 16428528, 16472371, 16537942, 16572518, 16450716, 16416741] iterations=1000 boundary=facade_publish_and_receipt_validation
```

##### Pair 5 — baseline

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1347909, 1350567, 1370472, 1351533, 1351127, 1337508, 1341622] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1278480, 1271459, 1290000, 1265256, 1269334, 1281801, 1269631] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1408366, 1414124, 1407552, 1408678, 1407359, 1405231, 1422033] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1383105, 1419412, 1356046, 1365240, 1347632, 1345920, 1357761] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3499761, 3471534, 3470915, 3470582, 3528857, 3566985, 3552036] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3401381, 3441672, 3423443, 3587191, 3445273, 3398551, 3436940] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3664136, 3684770, 3702529, 3729070, 3727243, 3682615, 3668327] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3585959, 3588283, 3581003, 3594572, 3573614, 3588394, 3595475] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2621857, 2611531, 2624648, 2625644, 2609078, 2619498, 2614084] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2583879, 2526310, 2540494, 2526606, 2543353, 2526037, 2540068] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2748699, 2755269, 2757764, 2749213, 2745489, 2754687, 2754505] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2676812, 2678173, 2695350, 2664153, 2663614, 2698501, 2686119] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[23978726, 23995384, 24132768, 23924084, 23895500, 23973648, 23884501] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[23963742, 23943744, 24738197, 24989942, 24026066, 23933457, 23932861] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24811879, 24748194, 24762500, 24697097, 24761687, 24724900, 24761946] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24985887, 25054260, 25037927, 25074890, 25050181, 25036158, 25070235] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15829373, 15863456, 15922446, 15822491, 15876608, 15826454, 15856188] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15795959, 15739293, 15746732, 15722188, 15740935, 15728279, 15740826] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16345236, 16427805, 16503273, 16424495, 16447301, 16395896, 16356513] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16310897, 16353634, 16339517, 16321976, 16277286, 16295438, 16319532] iterations=1000 boundary=facade_publish_and_receipt_validation
```

##### Pair 5 — candidate

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1290613, 1283695, 1295742, 1277769, 1274974, 1290172, 1282051] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1148816, 1148802, 1159648, 1149655, 1143999, 1146613, 1147815] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1467703, 1460434, 1472681, 1441112, 1464141, 1458091, 1461510] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1332408, 1335911, 1335417, 1336224, 1335948, 1337773, 1339452] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[2196750, 2127594, 2129555, 2124293, 2130279, 2130734, 2138182] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[1950320, 1952795, 1968956, 1958334, 1964136, 1963552, 1962264] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3675836, 3692523, 3684463, 3676030, 3692501, 3771215, 3736498] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3555933, 3535975, 3543997, 3551200, 3551781, 3552235, 3630682] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1922453, 1930980, 1931123, 1926405, 2004854, 2011851, 1937021] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1769329, 1773227, 1779507, 1770455, 1767341, 1777779, 1780772] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2772880, 2764880, 2790691, 2777592, 2787625, 2766624, 2763180] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2618375, 2617298, 2640328, 2602587, 2618268, 2609854, 2637460] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13010917, 13027903, 13289123, 13071586, 13032632, 13019370, 13043758] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[12977090, 12961193, 12980302, 12997868, 13011605, 13000716, 12996169] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24875067, 24794812, 24800184, 24780501, 24828505, 24828332, 24821972] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24783570, 24803121, 24825968, 24777256, 24787771, 24778743, 24831124] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10248664, 10265532, 10254810, 10259248, 10289574, 10290426, 10296974] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10338125, 10325390, 10310459, 10395909, 10374581, 10372678, 10374491] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16182930, 16236325, 16280626, 16295360, 16392443, 16359624, 16328039] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16156507, 16195870, 16214174, 16198244, 16185840, 16190380, 16179484] iterations=1000 boundary=facade_publish_and_receipt_validation
```

##### Pair 6 — baseline

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1368272, 1352204, 1358565, 1359807, 1344767, 1363901, 1376828] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1328039, 1238510, 1235660, 1246096, 1233895, 1239139, 1236247] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1434086, 1402404, 1417153, 1398470, 1411389, 1410006, 1411417] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1310876, 1326697, 1309967, 1315295, 1322995, 1312968, 1318730] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3499231, 3509848, 3497762, 3501124, 3514469, 3505500, 3503439] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3358482, 3352967, 3360844, 3363728, 3349796, 3405766, 3359924] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3709291, 3702424, 3696541, 3709714, 3710608, 3701538, 3690654] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3554430, 3556849, 3560545, 3561888, 3551833, 3562345, 3560781] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2626351, 2617337, 2621035, 2614636, 2611732, 2619196, 2612048] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2490606, 2490070, 2513090, 2496521, 2512899, 2507787, 2518787] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2742790, 2726980, 2749828, 2732228, 2724435, 2747266, 2739050] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2651094, 2640455, 2635246, 2652923, 2637499, 2645897, 2656105] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[23935158, 23906779, 23902544, 23836608, 23956952, 23927884, 23946103] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[23940822, 23849660, 23860439, 23823908, 23821475, 23957852, 23909770] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[25052294, 24818235, 24897795, 25237473, 24804977, 24775120, 24820997] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24947978, 24968318, 24977827, 24945877, 24932777, 24959479, 24939394] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15811299, 15855318, 15848963, 15858362, 15818579, 15859988, 15868807] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15914327, 15877271, 15866396, 15863266, 15858604, 15823250, 15829834] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16354984, 16414176, 16464349, 16413101, 16405413, 20808865, 24055614] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16232612, 16220303, 16204997, 16204682, 16207695, 16212363, 16205581] iterations=1000 boundary=facade_publish_and_receipt_validation
```

##### Pair 6 — candidate

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1240787, 1247015, 1238175, 1248135, 1241121, 1241081, 1240878] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1138031, 1132793, 1132766, 1142186, 1132219, 1147222, 1227395] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1397886, 1394290, 1403744, 1412844, 1409582, 1407900, 1399518] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1324239, 1313065, 1303409, 1320204, 1313133, 1312764, 1326716] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[2097407, 2114805, 2107377, 2103291, 2102461, 2093523, 2133569] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[1959790, 1955568, 1964864, 1959628, 1972073, 1958552, 1964140] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3660519, 3661703, 3647084, 3663915, 3711549, 3681803, 3690250] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3546747, 3531241, 3526437, 3536945, 3538468, 3537133, 3534368] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1881706, 1872251, 1883408, 1874710, 1952627, 1872590, 1890616] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1758652, 1776701, 1777399, 1774860, 1769141, 1780915, 1785531] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2776120, 2751890, 2756396, 2752239, 2764417, 2755399, 2757054] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2621842, 2628398, 2628906, 2610916, 2629847, 2606471, 2632908] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[12938918, 12930728, 12936092, 12968592, 13043338, 13076560, 13001661] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[13014849, 13006386, 12994240, 13163182, 13005734, 13025173, 13089600] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[27293576, 27471079, 27553877, 27730130, 27800057, 27628644, 27383212] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24738711, 25028009, 24805779, 24967177, 24738960, 24769260, 24686901] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10242088, 10237626, 10191488, 10219238, 10216845, 10213984, 10216332] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10294060, 10286239, 10268561, 10325034, 10274358, 10286556, 10311530] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16185934, 16267367, 16260110, 16267528, 16247479, 16259478, 16262496] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16122994, 16134470, 16140646, 16139779, 16211366, 16133820, 16132470] iterations=1000 boundary=facade_publish_and_receipt_validation
```

##### Pair 7 — baseline

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1346122, 1343447, 1361564, 1355313, 1350139, 1353108, 1347430] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1251283, 1239845, 1240655, 1246057, 1246158, 1236319, 1238306] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1429591, 1439996, 1431372, 1432247, 1426412, 1433138, 1424904] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1329460, 1330983, 1326994, 1329002, 1338011, 1322586, 1321419] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3479475, 3465577, 3488296, 3479931, 3493991, 3479507, 3499913] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[3325210, 3329200, 3335147, 3323220, 3325286, 3328031, 3329034] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3655361, 3688873, 3676670, 3669345, 3675733, 3697046, 3683201] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3599544, 3630930, 3635166, 3589380, 3587643, 3546305, 3573441] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2622156, 2642957, 2628473, 2616130, 2619971, 2620254, 2623407] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[2486591, 2484544, 2486041, 2500583, 2480097, 2494595, 2489797] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2742114, 2742409, 2746219, 2742619, 2738720, 2734046, 2735004] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2628388, 2629421, 2629879, 2615631, 2625288, 2622613, 2645286] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[23704205, 23743955, 23732823, 23744637, 23752152, 24199315, 23853469] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[24161766, 23780146, 23763951, 23844376, 23825235, 23919359, 23879563] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24758665, 24889890, 24750924, 24703684, 24776681, 24791518, 24829231] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24985984, 24901545, 24930900, 24881836, 25056629, 24926250, 24963887] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15759622, 15800330, 16045204, 15780058, 15773326, 15771487, 15782475] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[15748840, 15720881, 15726341, 15792544, 15716761, 15720601, 15749678] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16343230, 16429829, 16489511, 16423984, 16451969, 16410016, 16374810] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16320725, 16240595, 16277417, 16234333, 16225214, 16196930, 16196021] iterations=1000 boundary=facade_publish_and_receipt_validation
```

##### Pair 7 — candidate

```text
case=sync destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1216284, 1216751, 1223513, 1224405, 1222650, 1216383, 1218514] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=0 rejected=1 raw_sample_ns=[1129102, 1138577, 1137290, 1133222, 1126598, 1157565, 1144975] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1401539, 1392272, 1396523, 1391511, 1400729, 1393590, 1389407] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=1 partial=false observers=1 rejected=1 raw_sample_ns=[1324995, 1323440, 1308057, 1321380, 1322846, 1313537, 1328293] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[2105801, 2097894, 2121954, 2098016, 2113946, 2109130, 2097959] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=0 rejected=16 raw_sample_ns=[1968026, 1966375, 1976881, 1983393, 1986093, 1972996, 1982215] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3675546, 3665493, 3672984, 3672792, 3661853, 3670585, 3659350] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=false observers=1 rejected=16 raw_sample_ns=[3557783, 3544833, 3564465, 3656162, 3549566, 3554343, 3536388] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1888828, 1886053, 1883386, 1886093, 1888643, 1871855, 1909856] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=0 rejected=8 raw_sample_ns=[1778715, 1783856, 1791088, 1768175, 1784321, 1774521, 1776971] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2748383, 2736208, 2745932, 2727461, 2746827, 2720612, 3014228] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=16 partial=true observers=1 rejected=8 raw_sample_ns=[2626536, 2614738, 2616023, 2612715, 2608055, 2640834, 2627839] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[12941451, 12966535, 12969854, 13099498, 13035263, 13055145, 13053438] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=0 rejected=128 raw_sample_ns=[12977643, 12992478, 13003108, 12968480, 12990292, 12996331, 13008646] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24736509, 24746774, 24721127, 24724364, 25421822, 25455313, 25473279] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=false observers=1 rejected=128 raw_sample_ns=[24764029, 24774681, 24785475, 24733525, 24775906, 24766203, 24927657] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[10303869, 10402650, 10275384, 10289113, 10323766, 10659136, 11055190] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=0 rejected=64 raw_sample_ns=[12891217, 10520318, 11207437, 11579767, 12283784, 11769964, 12603607] iterations=1000 boundary=facade_publish_and_receipt_validation
case=sync destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[16317482, 17743865, 16509487, 16422674, 17294987, 18599266, 17890788] iterations=1000 boundary=facade_publish_and_receipt_validation
case=async destinations=128 partial=true observers=1 rejected=64 raw_sample_ns=[20094778, 20106890, 17192492, 16442884, 16254963, 16307544, 16271632] iterations=1000 boundary=facade_publish_and_receipt_validation
```

#### Final-source interpretation

Zero-observer measurements are consistently lower for the 16- and 128-destination cases, in both all-rejected and partial-rejection shapes and in sync/async facades. The one-destination differences are small. One-observer controls remain near their baseline values; outliers show shared-host scheduling noise. This is evidence for the measured Linux host and toolchain, not a general performance promise. No timing assertion was added to CI.

### T5 integrated validation

- `./.infra/bin/align-ci.sh`: first run exited 1 on missing full license headers in three new Rust files; after fixing them, a second run exited 1 on five Clippy findings in the bench and test probe. Both were corrected without lint suppressions. The final alignment run exited 0.
- `./.infra/bin/ci-check.sh`: exited 0, including workspace tests, default/discovery/conformance/all-feature matrix, Rustdoc/doctests, package verification, coverage, and audit. Coverage: 95.21% functions, 92.08% lines, 93.31% regions. `coverage-cfg-clippy` was explicitly unconfigured and skipped by repository CI.


## Synchronous facade capacity

These local measurements compare the pre-change baseline with the 2026-10-07
retry-scheduler run. They describe resource shape on one machine, not
performance guarantees or portable capacity thresholds.

### Evidence status

The exact temporary paths cited below were checked and are unavailable:
complete raw logs and the exact source baseline are missing, so the historical
excerpts cannot be fully reverified. The dates, commands, environment details,
and measurements below are retained as recorded; the tables are not a
replacement for the original output or a reproducible source snapshot. The
current-version measurements below are separate observations and do not
recreate or revise the historical evidence.

For future measurements, archive the following together:

- Every command and its exit code, plus complete stdout and stderr logs.
- UTC run time, Rust/tool versions, OS, CPU, and relevant concurrency/load
  conditions.
- Source HEAD and relevant dirty diff, plus SHA-256 hashes for the manifest,
  lockfile, and benchmark source.
- Warmup and measured sample parameters, and the exact summary method (for
  example, which samples feed a median and how failures are treated).
- A SHA-256 checksum for each archived artifact.

Remove or redact sensitive environment values before archiving; retain enough
environment detail to explain the measurement conditions.

### Pre-change baseline: environment and commands

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

### Pre-change baseline: subscription creation and shutdown

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

### Pre-change baseline: retry saturation

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

### 2026-10-07 run: environment and commands

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

#### Retry saturation after the scheduler change

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

#### Subscription threads and lifecycle after the change

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

### Current-version measurements: 2026-11-04 review

These measurements were collected from the current implementation for this
review. They are kept separate from the historical tables above and do not
support a historical before/after performance claim. The exact source state,
environment, commands, complete stdout/stderr, and checksums are archived in
the [fixed evidence snapshot](https://github.com/qubit-ltd/rs-event-bus/tree/e8a1ed724954756d48c3d5216cbd13ac39a0cb81/doc/capacity_performance_artifacts/20261104-review).

- UTC run interval: see the start and finish timestamp files in the archive.
- Source HEAD: see the archive manifest; the task-local source patch and
  benchmark source are included to reconstruct the measured working tree.
- Rust: `rustc 1.94.0 (4a4ef493e 2026-03-02)`; Cargo: `cargo 1.94.0`.
- Host: Linux x86_64, Intel Xeon E5-2680 v4, 56 logical CPUs visible. The
  captured load average was high (about 21) during later artifact collection;
  use the raw samples when interpreting timing spread.
- Commands, run serially:

  ```sh
  cargo bench --locked --bench local_threads
  cargo bench --locked --bench retry_saturation
  ```

`local_threads` completed all 42 measured lifecycle samples. Each of the six
mode/count groups had seven successful samples after two warmups. Medians below
are recomputed from the archived raw nanoseconds; peak threads were constant
within each group. Linux `/proc/self/task` supplied thread counts.

| Mode | Subscriptions | Create median (ms) | Cancel and shutdown median (ms) | Peak threads |
| --- | ---: | ---: | ---: | ---: |
| Sync | 16 | 2.016 | 461.154 | 21 |
| Sync | 64 | 6.849 | 1,958.770 | 69 |
| Sync | 256 | 39.840 | 6,008.587 | 261 |
| Async | 16 | 0.287 | 0.090 | 1 |
| Async | 64 | 0.920 | 0.347 | 1 |
| Async | 256 | 2.636 | 1.270 | 1 |

`retry_saturation` completed all seven measured samples after two warmups. The
median independent-handler wait was **0.138479 ms**. The individual values and
complete process output are in the archive; no sample was removed.

These measurements describe this host and these benchmark workloads only. The
raw output retains all samples so the summaries can be independently
recomputed; no values are written for unsupported metrics.


## EB-001 / EB-002 implementation measurements

### Environment and method

- Project source: `qubit-event-bus` at baseline commit `31effda34cf16bc6f8a43af9010854992d061062`.
- Host: x86_64 Linux 6.8.0-147-generic, dual Intel Xeon E5-2680 v4, 56 logical CPUs, 2.40 GHz nominal.
- Toolchain: rustc/cargo 1.94.0; locked dependencies; release profile for benchmarks.
- Timed matrix pinned to CPU 2 with `taskset`; 2 warmups and 7 independent accepted samples for each of 24 configurations. The benchmark rejected samples below its 98% timed CPU/wall gate; this run rejected 0 samples.
- `ordering_keys` exercised balanced, same-key and churn workloads at case sizes 4, 64, 1024 and 4096, with running/owned limits 4 and `max(case_size, 64)`. It reports observed concurrent handlers, so the high-key/limit-4 cases distinguish input key count from active admission. The public facade does not expose an internal locked-lane counter; that boundary is not directly measured.
- Middleware baseline and post-change runs used 500 deliveries per sample at 0, 1, 8 and 32 async/sync middleware layers, both pinned to CPU 2. Concurrent CI work on the host caused visible scheduling variance, so use these results as contextual comparison only.

### Commands and results

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

### EB-002 decision

The profile completed 262,144 public-facade deliveries using one balanced high-key / limit-64 workload. It was not repeated across the full matrix or across subscription-count/locked-lane dimensions. The 0.01-second gprof sampling bins are coarse; this is attribution evidence, not a precise CPU percentage comparison. The flat profile's largest entries were formatting (5.05%), hashing (4.04%) and scheduler notifications (3.54%). The identified scheduler scans were small in this sample: `refresh_ready` 1.01%, the `eligible_lane` predicate 1.01%, and ready-queue selection 1.52%. The full elapsed matrix exercised balanced, same-key and churn workloads at high key counts under admission limit 4 and larger; it does not expose the exact internal locked-lane count. This evidence does not show a reproducible dominant scan hotspot or establish a candidate improvement above baseline variation. The matrix also has broad high-key dispersion relative to default cases.

**Decision: do not modify the scheduling core.** Complexity exists, but this evidence does not justify an index or membership-set change. The public-facade workload exercises backlogs from many distinct keys under admission limit 4, but cannot report the exact internal locked-lane count. No fairness or resource-release behavior was changed.

### EB-001 allocation probe

The focused unit probe creates the input `SpiFuture` before enabling a thread-local allocation counter, then measures only construction of the panic wrapper. At the baseline, it failed as expected with 2 allocations. After passing the existing pinned future directly into `CatchUnwindFuture`, it passed with exactly 1 allocation. Thread-local counting prevents allocations on concurrently running test threads from contaminating the measurement.

The first staged `git diff --cached --check` flagged trailing spaces in the gprof report's column headings. Those spaces were removed from the retained text reports; the repeated check passed. The binary gmon profile is unchanged.
