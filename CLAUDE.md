# netty-virtual-loop — project context

Netty `IoEventLoopGroup` implementations where blocking handler code parks a virtual thread
instead of stalling the event loop. The loop's own platform thread is the Loom CARRIER: a virtual
thread's scheduler is just an `Executor`, and forwarding continuations to
`SingleThreadIoEventLoop.execute()` makes the loop thread carry them. Target: brownfield Netty
services where blocking (JDBC, sync clients, `future.sync()`) is endemic and unmappable.

## Build / run

- JDK 25+ GA (Gradle toolchain 25; developed on 27-ea). Netty 4.2.x. Kotlin.
- Every JVM that touches Loom internals needs `--add-opens=java.base/java.lang=ALL-UNNAMED`
  (already wired into Gradle test/JavaExec tasks).
- `./gradlew test` — the verification battery (real sockets, TLS, offload, JFR, contracts).
  Tests run with `-Dio.netty.leakDetection.level=paranoid`; a `LEAK:` line is a failure signal.
- Benches: `runPingPong` / `runPingPongVanilla` / `runHandoff` / `runBench` / `runAlloc`.
  This machine is noisy across sessions (±10µs): only SAME-SESSION A/B comparisons are meaningful.

## Architecture map (one collaborator per file)

Two adoption tiers:

1. **Full replacement** — `VirtualIoEventLoopGroup` wraps a `MultiThreadIoEventLoopGroup`
   "carrier" 1:1 (`ownsCarrier`: consume vs guest lifecycles). `VirtualIoEventLoop` runs every
   task AND every dispatched IO event on fresh virtual threads; per-channel ordering comes from a
   per-registration serial drain VT (`DelegatedHandle.kt`, dynamic Proxy over the IoHandle).
2. **Minimal invasion** — `VirtualEventExecutorGroup` (`VirtualEventExecutor.kt`): keep a vanilla
   group, mark only blocking handlers via `pipeline.addLast(vtGroup, handler)`. One serial lane +
   drain VT per pipeline; the carrier is AUTO-CAPTURED from `ThreadExecutorMap.currentExecutor()`
   on the first submission (handlerAdded always arrives from the loop).

Shared machinery: `LoopRuntime.kt` (ScopedValues, StackWalker disguise, flags, caller tests),
`LoopTimer.kt` (carrier-confined timer: `Scheduler` + `SchedulerLoop` driver), `BlockingGuards.kt`
(promises/tasks whose dead-lock guard fires only for the real hazard — the carrier platform
thread), `RunnableNettyTask.kt` (java FutureTask + Netty Future + CompletionStage; `cancel(true)`
interrupts the running VT), `PrivateLoomSupport.kt` (reflection into Loom), `LoopEvents.kt` (JFR),
`VirtualLoopStats.kt` (counters the benches read).

## Load-bearing invariants — violate these and things break subtly

- **NEVER `Thread.yield()` inside a VT scheduler Executor**: it can be invoked from Loom's
  switchToCarrierThread window (unpark by a VT), where yielding is illegal. `lazyExecute`
  (non-blocking offer) is the maximum there. Yield in plain VT user code (e.g. periodic catch-up)
  is fine.
- **Same-carrier fast path reasoning**: a VT MOUNTED on the carrier is physically executing on
  the carrier thread — queue mutations can't race carrier-confined state, and the carrier is
  provably awake, so `lazyExecute` (no selector wakeup) is safe. Detection:
  `PrivateLoomSupport.carrierOf(thread)`.
- **Interceptor capture** (`CONTINUATION_INTERCEPTOR` ScopedValue, checked FIRST in schedulers):
  leans on two Loom properties — `runContinuation` is a stable cached object per VT and is
  state-CAS-guarded (stale/duplicate `.run()` is a no-op). In `enqueueAndRun`, clear the holder
  BEFORE the add: only a continuation captured by THIS add may be mounted (the drain may be
  parked in a promise await, not the queue take).
- **Stock periodic scheduling is WRONG for VT bodies**: a continuation returns at the first park,
  so Netty/JUC periodics would measure "time to first park" as execution time and overlap rounds.
  Periodics are multi-step chains of one-shots; the next round arms only after TRUE completion
  (`FutureTask.runAndReset`). Timer cancellation must propagate to the chain
  (`timer.addListener { if cancelled -> cancel }`) or shutdown's `cancelScheduledTasks` kills
  chains silently.
- **`inEventLoop()` answers are caller-sensitive, and that is load-bearing.**
  - Loop VTs: the no-arg variant answers HAZARD, not membership, to `DefaultPromise.checkDeadLock`
    (StackWalker: `getCallerClass` pre-filter ~280ns, full walk only for promise-family callers).
    False for loop VTs (awaiting parks, safe), TRUE for the raw carrier thread (real deadlock).
    Kill-switch: `-Dvirtualloop.disguise=false`.
  - Offload lanes (`VirtualEventExecutor.inEventLoop`): THREE answers. Netty 4.2's
    `ensurePromiseUseCorrectExecutor` replaces any outbound promise whose executor is not "in" on
    the current thread and cascades completion through a listener that `DefaultPromise` dispatches
    via the promise's executor — for a lane promise completed on the loop, the cascade queues
    BEHIND the very drain job awaiting it (bytes flush, promise never completes; stock offload
    executors surface this as BlockingOperationException). So: promise-plumbing callers see the
    captured loop thread as a member; checkDeadLock on the drain gets the disguise; pipeline
    DISPATCH (same class as ensure! discriminated by method-name walk) sees drain identity only.
- **The scheduled-task queue of `AbstractScheduledEventExecutor` is NOT thread-safe**: every
  mutation must be marshalled to the carrier (`Scheduler.execute` routes there); the
  `SchedulerLoop` driver is entirely carrier-confined; mounted-VTs may touch it directly (see
  same-carrier reasoning).
- **Quiet-period shutdown must not observe itself**: the shutdown watcher is raw carrier-timer
  runnables (no VT!) precisely so its wake-ups don't stamp `lastActivityNanos`. Termination watch
  polls `isTerminated` on the carrier timer (`awaitQuiescence`); a loop-carried awaiter VT would
  need factory-direct creation (a tracked task would await itself).
- **FastThreadLocal bridge scope**: `FastThreadLocalThread.runWithFastThreadLocal` wraps ONLY the
  long-lived drain VT (Recycler gets a real pool, PooledByteBufAllocator a real cache). Task VTs
  stay unbridged by design; sharing an InternalThreadLocalMap across VTs would corrupt
  borrow-semantics scratch (e.g. `stringBuilder()`) across park points.

## Known JDK/Netty gotchas (verified, do not re-litigate)

- `Class.getField` sees PUBLIC members only; `VirtualThread.DEFAULT_SCHEDULER` is private static —
  `getDeclaredField` or the whole `PrivateLoomSupport` init poisons to unsupported.
- JFR periodic hooks (`FlightRecorder.addPeriodicEvent`) added while a recording is running never
  activate for it (JDK 25.0.3); and an event type whose instances only exist inside the hook needs
  explicit `FlightRecorder.register`. All events: `@Enabled(false)` + `@StackTrace(false)` +
  static `INSTANCE.isEnabled()` guard → disabled hot-path cost is one branch.
- `ScopedValue.orElse(null)` throws (requireNonNull); use `isBound`.
- Monitor wait/notify on custom-scheduler VTs works on JDK 25 (probed) — JEP 491.
- `ForkJoinPool` asyncMode only changes the OWNER's local dequeue order (poll vs pop); stealing
  happens in both modes. The default VT scheduler is FIFO AND work-stealing (no carrier affinity).
- In tests, TCP coalesces small writes: count BYTES, not reads.
- Netty offload teardown order: shut the IO group FIRST (lifecycle events still flow through live
  lanes), then the offload group; lanes run post-termination stragglers on untracked VTs rather
  than dropping channelInactive.

## Closed dead-ends (tried, rejected — don't retry)

- `ManualIoEventLoop` per IoHandle (owner-thread constraint, handler explosion). NOTE: Micronaut
  and franz1981 both use ManualIoEventLoop owned by ONE virtual thread — viable as an alternative
  backend if the ~+5µs/rtt gap ever matters, but their pipelines still run ON the loop VT.
- Hand-rolled `VirtualIoCarrier` (rejected: use the standard Netty executor model as backend).
- `@CallerSensitive` intrinsic for the disguise (HotSpot ignores it on app-classloader classes).
- Carrier-shared `InternalThreadLocalMap` (park-interleaving corrupts borrow-semantics FTLs).
- CarrierLocal buffer pool: measured (+14ns/alloc under the default adaptive allocator via
  `runAlloc`) and DEFERRED — not worth the machinery at that delta.

## Conventions

- Comments explain WHY and the constraint the code can't show (the codebase is dense with them —
  match that); no change-log style comments.
- Verification is the test battery, not mocks: real sockets, real TLS, real blocking sleeps.
- Benchmarks read `VirtualLoopStats` counters programmatically — keep the counters even though
  JFR events exist.
- Flags live under the `virtualloop.` prefix.
