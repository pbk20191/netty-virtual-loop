# netty-virtual-loop

A Netty `IoEventLoopGroup` where **every submitted task and every dispatched IO event runs on a
fresh virtual thread carried by the loop's own platform thread**. Blocking code in a handler parks
the virtual thread (the JDK's virtual-thread poller takes over the wait) instead of stalling the
event loop — one selector still multiplexes all channels, Netty's per-channel ordering is preserved,
and the pipeline API is unchanged. Built for brownfield Netty services where blocking calls
(JDBC, sync HTTP clients, `future.sync()`) are endemic and unmappable.

Core mechanism: a virtual thread's Loom scheduler is just an `Executor`; pointing it at
`SingleThreadIoEventLoop.execute` makes the loop thread the carrier. Reaching the private
`ThreadBuilders$VirtualThreadBuilder.scheduler` field requires:

```
--add-opens=java.base/java.lang=ALL-UNNAMED
```

Targets JDK 25+ **GA** (developed on 27-ea) — no `--enable-preview`. Netty 4.2.x.

## How it differs from neighbours

- [Micronaut's loom carrier](https://micronaut.io/2025/06/30/transitioning-to-virtual-threads-using-the-micronaut-loom-carrier/)
  runs framework tasks on loop-carried virtual threads; it does not dispatch Netty IO events on them.
- [franz1981/Netty-VirtualThread-Scheduler](https://github.com/franz1981/Netty-VirtualThread-Scheduler)
  replaces the JDK's default scheduler (Java 27 preview) and gives user virtual threads home-carrier
  IO affinity; pipeline handlers stay on the event loop.
- This project dispatches **the IO events themselves** on virtual threads (serialized per
  registration by a drain virtual thread), makes unmodified `sync()`/`await()` in handlers work
  (`inEventLoop()` dead-lock disguise), rebuilds periodic scheduling to be virtual-thread-correct
  (next round only after TRUE completion, not first park), and gives interruptible
  `cancel(true)` on Netty-compatible task futures.

Two adoption tiers:

1. **Full group replacement** (`VirtualIoEventLoopGroup`): every task and IO event on virtual
   threads; maximum transparency for blocking-everywhere codebases.
2. **Minimal invasion** (`VirtualEventExecutorGroup`): keep the vanilla group and mark only the
   blocking handlers - `pipeline.addLast(virtualExecutorGroup, handler)`. The handler runs on a
   serial drain virtual thread carried by the channel's own loop thread (auto-captured), may
   block and `sync()` freely, and Netty's offload ordering contract is preserved.

## Layout

```
src/main/kotlin/io/github/pbk20191/virtualloop/
├── VirtualIoEventLoopGroup.kt      group: wraps a MultiThreadIoEventLoopGroup "carrier" 1:1;
│                                   ownsCarrier=true (consume) / false (guest) lifecycle modes
├── VirtualIoEventLoop.kt           the loop: VT scheduler, submit/schedule, register, shutdown
├── LoopRuntime.kt                  shared ScopedValues, StackWalker disguise, feature flags
├── LoopTimer.kt                    loop-owned timer (Scheduler + SchedulerLoop driver,
│                                   carrier-confined) + one-shot scheduled future
├── VirtualEventExecutor.kt         minimal-invasion tier: addLast(group, handler) offload lanes
│                                   on loop-carried drain VTs (carrier auto-capture)
├── IoHandleProxy.kt                dynamic-proxy IoHandle wrapper: serial per-registration
│                                   dispatch on a drain VT; registration cancel() hook
├── BlockingGuards.kt               promises/tasks whose dead-lock guard fires only when the
│                                   CARRIER platform thread would block (VTs just park)
├── RunnableNettyTask.kt            FutureTask + Netty Future + CompletionStage in one type;
│                                   cancel(true) interrupts the running virtual thread
├── PromiseLikeCompletableFuture.kt CompletableFuture that is a full Netty ProgressivePromise
├── RunnableNettyFuture.kt          task-future interface
├── VtFutures.kt                    guard-free await/sync helpers for Netty-internal futures
├── PrivateLoomSupport.kt           reflection bridge into Loom internals
├── VirtualLoopStats.kt             scheduler-path counters (benches read these)
└── bench/                          micro-benches (JavaExec tasks)

src/test/kotlin/io/github/pbk20191/virtualloop/   JUnit tests (./gradlew test)
├── SanityTest.kt                   tasks on fresh VTs, one carrier per loop, parking frees it
├── EchoTest.kt                     real socket echo through a 200ms-BLOCKING handler;
│                                   FastThreadLocal bridge + Recycler on the drain VT
├── ReFireTest.kt                   level-triggered re-fire never re-enters a channel
├── OwnershipTest.kt                consume vs guest carrier lifecycle
├── ContractTest.kt                 invokeAll/invokeAny on VTs, in-task sync(), cancel(true)
│                                   interrupt, periodic semantics, shutdown quiet period
├── TlsEchoTest.kt                  TLSv1.3 handshake + echo through a 150ms-BLOCKING handler
│                                   (SslHandler schedules its timeouts through our timer)
├── OffloadExecutorTest.kt          VANILLA group + addLast(offload, blockingHandler): loop-carried
│                                   VT, ordering, in-handler sync(), per-lane timer, shutdown
└── WalkBenchTest.kt                measures the inEventLoop() disguise tiers on a loop VT

Tests run with `-Dio.netty.leakDetection.level=paranoid`; ByteBuf leaks surface as LEAK errors.
```

## Run

| Task | What it does |
|---|---|
| `./gradlew test` | the whole verification suite (see src/test above) |
| `./gradlew runPingPong` / `runPingPongVanilla` | round-trip latency vs stock Netty |
| `./gradlew runHandoff` | same-carrier submission fast path |
| `./gradlew runBench` | inEventLoop() disguise overhead |
| `./gradlew runAlloc` | allocator cost: many VTs on one carrier vs stock FTL loop thread |

## Flags

| Property | Default | Effect |
|---|---|---|
| `virtualloop.disguise` | `true` | `inEventLoop()` answers "false" to Netty's dead-lock guard for loop VTs (plain `sync()` on channel futures works). Costs ~280ns per call via `getCallerClass`; disable and use `VtFutures` for ~1ns. |
| `virtualloop.stats` | `true` | scheduler-path counters (two atomic increments per continuation) |

## Observability (JFR)

Five JFR events under category "Netty Virtual Loop" (all `@Enabled(false)` by default; the
disabled hot-path cost is a single branch — pattern borrowed from Micronaut's loom carrier):

| Event | What it records |
|---|---|
| `…virtualloop.ContinuationScheduled` | every continuation entering the scheduler: dispatch mode (1=intercepted, 2=inline, 3=same-carrier, 4=queued), submitter, hash |
| `…virtualloop.ContinuationRun` | duration of one mounted stretch on the carrier; correlate with Scheduled via hash for schedule→run latency |
| `…virtualloop.IoEventHandled` | duration of one IO event through the wrapped IoHandle (pipeline execution on the drain VT) |
| `…virtualloop.PeriodicRound` | duration + start lateness of each periodic-chain round |
| `…virtualloop.LoopShutdown` | whole graceful-shutdown span; whether it escalated to shutdownNow |
| `…virtualloop.LoopStats` | periodic (default 1s) snapshot of the VirtualLoopStats scheduler counters |

`LoopStats` is a periodic-hook event; due to a JDK limitation (verified on 25.0.3), hooks
registered while a recording is already running never activate for it — start the recording
after the first loop exists.

Enable at runtime, e.g.
`jcmd <pid> JFR.start settings=none +io.github.pbk20191.virtualloop.ContinuationScheduled#enabled=true …`
or programmatically via `Recording.enable(name)`. Verified by `JfrEventsTest`.

## Known trade-offs

- Rides JDK internals (`--add-opens`, the scheduler field, `runContinuation` stability) and Netty
  4.2 internals — version-bump risk items. The JDK 27-preview `jdk.virtualThreadScheduler.implClass`
  hook is the eventual migration path for the reflection layer.
- Carrier-pinning operations (file IO, JNI, DNS lookups) still stall the loop: Loom cannot park
  them and this scheduler has no compensation pool. Socket IO, sleeps, locks and monitors all park.
- CPU-bound handler work hogs the carrier exactly as in stock Netty.
- Verified by the test battery (incl. TLS and paranoid leak detection), not yet
  production-hardened (no soak testing, no io_uring/epoll transport coverage).

## License

[Apache-2.0](LICENSE)
