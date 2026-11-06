# Admission summary performance baseline

## Baseline identity

This is the **pre-cache baseline** for EB-002. It measures the implementation at source commit `1a0d47cfbbb5e23c840af78416a26a6cb997a907`; no `PublishReceipt` admission-outcome cache was present. The benchmark file was an uncommitted working-tree change with SHA-256 patch digest `a51d7fdb09525944f2da6d259e9cae90f7cd153d2de7fde00f972dc9040f34bc` (SHA-256 of `git diff -- benches/facade_delivery.rs` at measurement time). Branch: `codex/eb-cancellation-admission-20261106`.

Run time: 2026-11-06 UTC. Host: Linux `mas-server`, kernel `6.8.0-147-generic`, x86_64; Intel Xeon E5-2680 v4 @ 2.40GHz, 56 logical CPUs (2 sockets × 14 cores × 2 threads). Rust toolchain: `rustc 1.94.0 (4a4ef493e 2026-03-02)`, LLVM 21.1.8; Cargo 1.94.0. Profile: Cargo's optimized `bench` profile (release optimization, no custom profile flags).

Command: `cargo bench --locked --bench facade_delivery`

Final exact exit code: **0**. The process emitted one original no-subscriber case and all 24 requested matrix outputs. No Rust compiler warnings or benchmark errors were emitted. `size_of::<PublishReceipt>()` was 112 bytes.

## Measurement design and boundaries

The matrix includes synchronous `EventBus` ordinary and checked publish, asynchronous `AsyncEventBus` ordinary and checked publish, and `NotificationPublisher`, each at 0, 1, 16, and 128 destinations. Each matrix case runs two warmup batches followed by seven reported raw samples. Each batch contains exactly 1,000 messages. The retained original no-subscriber async facade case also uses two warmups and seven raw samples, each containing 10,000 messages.

Fixture construction, subscriptions, topic/request construction, notification publisher creation, and cleanup are outside facade publish timing. Sync and async facade samples time the 1,000-call publication loop including local facade/SPI admission; they do not include handler completion. The async fixture holds subscriptions without starting their handlers, keeping the receiver queues as the fixed destinations. The sync fixture has no-op handlers and raises facade scheduling limits to 128,000 owned deliveries overall and per subscription, 128 running handlers, and 128 subscriptions. Every local SPI mailbox has capacity 1,000 and the provider-wide outstanding limit is 128,000.

Notification enqueue samples time only the 1,000 `try_publish` calls. Drain-inclusive samples report enqueue duration plus the duration of `close`, which closes admission and waits for worker drain. The notification publisher queue capacity is 1,000. A fresh local sync fixture is created per notification sample. After drain, assertions require 1,000 enqueued and published notifications, the expected admission category (all `no_destinations` at fanout 0, all `accepted` otherwise), and zero publication/request errors, queue-full, queue-closed, observer-panicked, or worker-panicked counts.

For ordinary facade publication, every receipt is checked to report exactly the configured accepted destination count. Checked publication uses `AtLeastOneAccepted`; 0 destinations must produce `CheckedPublishError::Admission` with `AdmissionCheckError::NoAcceptedDestination`, and nonzero fanout must return a receipt with all destinations accepted. The 0-destination outcome is an expected checked failure after ordinary publication; it is not a provider publish failure.

All runs use the in-process local SPI. These results do not measure provider/network delivery latency or subscriber handler completion. No message is intentionally rejected to create load.

## Raw baseline results

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

## Cache candidate comparison

The run order was baseline (the raw baseline above), candidate 1, baseline 2, candidate 2. All runs used the same host, Rust 1.94.0, optimized bench profile, and unchanged benchmark parameters. Each command was `cargo bench --locked --bench facade_delivery` and exited 0, emitting 25 case lines with no compiler warnings or benchmark errors. The temporary candidate patch had SHA-256 `773ccf42d05772d125ea66bb3b8f889cb64c69aabe80b965abbf44e5a1599089` (SHA-256 of `git diff -- src/model/publish_receipt.rs`). Its receipt size was 144 bytes versus 112 bytes without the cache, an increase of 32 bytes (28.6%). The candidate code was removed after measurement.

### Candidate 1 (after documented baseline)

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

### Baseline 2 (after candidate 1)

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

### Candidate 2 (after baseline 2)

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

### Decision

The cache candidate did not show a repeatable gain at both 16 and 128 destinations, and low-fanout samples do not establish absence of a material regression. Async 16-destination samples were near the baseline with a small run-to-run spread; async 128-destination candidate samples varied from slightly faster to slower and included scheduler outliers. Sync high-fanout samples were substantially noisier than the candidate effect. Together with the 32-byte receipt growth, this evidence does not meet the design retention criterion. The cache is therefore not retained. `PublishReceipt` keeps computing its classification from the immutable acknowledgement; the benchmark and public-behavior regression tests remain to guide a future measured decision. No performance improvement is claimed. These local-SPI measurements do not describe remote providers or handler completion.

## Final correctness-assertion pair

The async checked-publish zero-destination assertion was strengthened to require `CheckedPublishError::Admission` with `AdmissionCheckError::NoAcceptedDestination`; this pair reran after that benchmark-only correctness refinement. The benchmark source patch digest at these runs was SHA-256 `cec31af9da9eee7c61bd5a6ec5a462e138cf103933178cf2e74d09d79673fee4`. The run order was baseline 3 followed by candidate 3. Both used `cargo bench --locked --bench facade_delivery`, exited 0, emitted 25 case lines, and had no compiler warnings or benchmark errors. The baseline again measured 112-byte receipts; the temporary candidate measured 144-byte receipts.

### Baseline 3 (exact 0-destination checked-error assertion)

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

### Candidate 3 (interleaved cache candidate)

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
