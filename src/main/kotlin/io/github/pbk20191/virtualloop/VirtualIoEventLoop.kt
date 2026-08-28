package io.github.pbk20191.virtualloop

import io.github.pbk20191.virtualloop.proxy.DelegatedHandle
import io.github.pbk20191.virtualloop.proxy.DelegatedRegistration
import io.github.pbk20191.virtualloop.proxy.InterfaceCache
import io.netty.channel.*
import io.netty.util.concurrent.*
import io.netty.util.concurrent.Future
import io.netty.util.concurrent.ScheduledFuture
import io.netty.util.internal.ThreadExecutorMap
import java.util.Collections
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicReference

/**
 * An [IoEventLoop] that runs every submitted task - and every dispatched IO event - on a fresh
 * virtual thread, while a single backing [SingleThreadIoEventLoop] ([carrier]) provides the NIO IO
 * handling on a normal Netty platform thread (the standard executor model).
 *
 * Architecture map (each collaborator lives in its own file in this package):
 *  - [scheduler] (the virtual threads' Loom scheduler): forwards continuations to
 *    [carrier].execute, so every virtual thread is carried by the loop's platform thread; parking
 *    unmounts and frees the carrier. Same-carrier submissions take a wakeup-free lazyExecute path,
 *    and [CONTINUATION_INTERCEPTOR]/[INLINE_NEXT] (see LoopRuntime.kt) allow captured/inline mounts.
 *  - DelegatedHandle.kt: [io.github.pbk20191.virtualloop.proxy.DelegatedHandle] serializes each registration's IO events onto one
 *    long-lived drain virtual thread (mounted INLINE on the carrier per event);
 *    [io.github.pbk20191.virtualloop.proxy.DelegatedRegistration] hooks cancel() as the drain's terminal signal.
 *  - LoopTimer.kt: the loop-owned timer ([Scheduler] + [SchedulerLoop] driver, carrier-confined)
 *    behind [schedule]/[scheduleAtFixedRate]/[scheduleWithFixedDelay]; periodics are the
 *    multi-step [PeriodicVirtualTask] chain (next round only after TRUE completion).
 *  - BlockingGuards.kt / RunnableNettyTask.kt: task and promise types whose dead-lock guard fires
 *    only for the real hazard (blocking the carrier platform thread) and whose cancel(true)
 *    genuinely interrupts the running virtual thread.
 *
 * Blocking notes: futures created BY this loop are safe to sync()/await() from tasks and handlers.
 * Netty-internal promises (channel write futures) keep their guard unless the inEventLoop()
 * checkDeadLock disguise is active (default; see LoopRuntime.kt flags) - VtFutures.kt has
 * guard-free helpers either way.
 */
// Implements the IoEventLoop INTERFACE directly instead of extending AbstractEventExecutor: the
// abstract base's `newTaskFor` is final and mints PromiseTask futures whose dead-lock guard cannot
// be relaxed, so every inherited AbstractExecutorService entry point was a latent trap (losing a
// submit override once silently regressed in-task invokeAll). Interfaces cannot declare final
// methods, and Netty 4.2's EventExecutor interface provides rich defaults (newPromise overridden
// below, newSucceededFuture/newFailedFuture, inEventLoop()), so nothing inherited can ever create a
// guarded future behind this class's back again.
class VirtualIoEventLoop(
    private val group: IoEventLoopGroup,
    internal val carrier: SingleThreadIoEventLoop
) : AbstractExecutorService(), IoEventLoop {


    private val terminationFuture: Promise<Unit> = DefaultPromise(GlobalEventExecutor.INSTANCE)

    private val nettyScheduler = Scheduler(this, carrier)
    /**
     * Nano timestamp of the most recent virtual-thread continuation scheduled on this loop. Every
     * task start, park-resume and IO drain wake-up passes through [scheduler], so this is the single
     * choke point that observes all activity - it drives the graceful-shutdown quiet period
     * (activity restarts the window, like Netty's own semantics).
     */
    private val lastActivityNanos = java.util.concurrent.atomic.AtomicLong(System.nanoTime())

    private val scheduler = java.util.concurrent.Executor { raw ->
        // Opaque store: this is a heuristic timestamp for the shutdown quiet period - no ordering
        // is needed, and an opaque write skips the volatile store fence on weakly-ordered CPUs.
        lastActivityNanos.setOpaque(System.nanoTime())
        // JFR (disabled by default): when recording, wrap the continuation so its mounted stretch
        // becomes a ContinuationRun duration event, correlated to the ContinuationScheduled below
        // via the identity hash. Cost when disabled: this one branch.
        var continuation = raw
        var scheduled: ContinuationScheduled? = null
        if (ContinuationScheduled.INSTANCE.isEnabled()) {
            val hash = System.identityHashCode(raw)
            scheduled = ContinuationScheduled().also {
                it.hash = hash
                it.submitter = Thread.currentThread().name
            }
            continuation = Runnable {
                val run = ContinuationRun()
                run.begin()
                try {
                    raw.run()
                } finally {
                    run.end()
                    run.hash = hash
                    run.commit()
                }
            }
        }
        // orElse(null) is rejected by ScopedValue; isBound alone is the single cheap lookup on the
        // common (unbound) path, and get() runs only when actually bound.
        if (CONTINUATION_INTERCEPTOR.isBound) {
            if (STATS_ENABLED) VirtualLoopStats.interceptedContinuations.incrementAndGet()
            scheduled?.let { it.mode = 1; it.commit() }
            CONTINUATION_INTERCEPTOR.get().execute(continuation)
            return@Executor
        }
        if (INLINE_NEXT.orElse(false) && !Thread.currentThread().isVirtual) {
            if (STATS_ENABLED) VirtualLoopStats.inlineContinuations.incrementAndGet()
            scheduled?.let { it.mode = 2; it.commit() }
            // Set by inline(..) (timer callbacks), which runs on the loop's platform thread and
            // hands off work synchronously - running the continuation directly skips the task-queue
            // hop and mounts the virtual thread on the thread that would carry it anyway. The
            // isVirtual guard makes the hint inert on virtual callers: a virtual thread can never
            // carry another virtual thread's continuation. Loom swaps ScopedValue bindings on
            // mount, so nested/other submissions never see this hint.
            ScopedValue.where(INLINE_NEXT, false).run(continuation)
        } else {
            // SAME-CARRIER fast path: if the submitter is a virtual thread currently MOUNTED ON
            // this loop's carrier, the carrier is provably awake right now (it is running us) - a
            // selector wakeup is pure waste. lazyExecute enqueues without waking; the loop's
            // canBlock() (!hasTasks) check guarantees the task is seen before any blocking select.
            // True inlining is impossible here (a mounted virtual thread cannot carry another one),
            // so wakeup-free enqueue is the maximum. NOTE: no Thread.yield() in this path - the
            // scheduler can be invoked from Loom's switchToCarrierThread window (unpark by a
            // virtual thread), where yielding is illegal; lazyExecute is a non-blocking offer only.
            val current = Thread.currentThread()
            val mountedOn = if (current.isVirtual) PrivateLoomSupport.carrierOf(current) else null
            if (mountedOn != null && carrier.isExecutorThread(mountedOn)) {
                if (STATS_ENABLED) VirtualLoopStats.sameCarrierContinuations.incrementAndGet()
                scheduled?.let { it.mode = 3; it.commit() }
                try {
                    carrier.lazyExecute(continuation)
                } catch (e: java.util.concurrent.RejectedExecutionException) {
                    if (!carrier.isShuttingDown) throw e
                    (PrivateLoomSupport.defaultCarrierScheduler ?: ForkJoinPool.commonPool()).execute(continuation)
                }
                return@Executor
            }
            if (STATS_ENABLED) VirtualLoopStats.queuedContinuations.incrementAndGet()
            try {
                scheduled?.let { it.mode = 4; it.commit() }
                carrier.execute(continuation)
            } catch (e: java.util.concurrent.RejectedExecutionException) {
                if (!carrier.isShuttingDown) throw e
                // The carrier is gone but the virtual thread still has work (it is resuming from a
                // park). Dropping the continuation would freeze the thread forever; during teardown
                // losing single-thread affinity is preferable, so run it on GlobalEventExecutor's
                // (platform) thread instead.
                (PrivateLoomSupport.defaultCarrierScheduler ?: ForkJoinPool.commonPool()).execute(continuation)
            }
        }
    }

    /** Creates the loop's virtual threads: scheduler = [scheduler], so [carrier]'s thread carries them. */
    private val threadFactory = run {
        val builder = Thread.ofVirtual().name("virtual-io-task-", 0)
        PrivateLoomSupport.setScheduler(builder, scheduler)
        builder.factory()
            .let { ThreadExecutorMap.apply(it, this) }
    }

    /** Starts a fresh virtual thread (carried by [carrier]'s loop thread) per submitted task / IO event. */
    private val executor = Executors.newThreadPerTaskExecutor(threadFactory)

//    private val cache = InternalThreadLocalMap()
    /** Set once shutdown begins; drives [isShuttingDown] independently of the executor's hard state. */
    private val shuttingDown = java.util.concurrent.atomic.AtomicBoolean(false)

    /** Diagnostic only (JFR [LoopShutdown.forced]): did shutdown escalate to shutdownNow. */
    @Volatile
    private var shutdownForced = false

    /** Live registrations' handles, retained so shutdown can close the loop's own channels. */
    private val liveHandles = ConcurrentHashMap.newKeySet<DelegatedHandle>()

    init {
        PeriodicStats.ensureRegistered()
    }

    override fun execute(command: Runnable) {
        try {
            executor.execute(command)
        } catch (e: RejectedExecutionException) {
            if (!shuttingDown.get()) throw e
            // Teardown straggler (e.g. AbstractChannel.invokeLater running deregister /
            // fireChannelInactive after the executor was shut down). Rejecting it would silently
            // drop channelInactive/channelUnregistered, so run it on an untracked virtual thread
            // that still uses our scheduler (same carrier, same inEventLoop identity).
            threadFactory.newThread(command).start()
        }
    }

    /**
     * Runs [command] on a fresh virtual thread, hinting that its first continuation should run inline
     * rather than take a [carrier].execute queue hop. Only meaningful when called from the loop thread
     * (e.g. a scheduled-timer callback that starts a task synchronously); the [ScopedValue] confines
     * the hint to that synchronous window.
     */
    private fun inline(command: Runnable) {
        ScopedValue.where(INLINE_NEXT, true).run { execute(command) }
    }

    override fun isSuspended(): Boolean {
        return carrier.isSuspended
    }

    // Pure delegation is complete here: trySuspend is only a state CAS (STARTED -> SUSPENDING);
    // the actual suspension decision happens later in the carrier's run loop via canSuspend(),
    // which our structure satisfies correctly - live channels hold carrier registrations
    // (numRegistrations > 0 blocks it), an armed SchedulerLoop timer blocks it via the scheduled
    // deadline, and a suspension while task VTs are merely PARKED is transparent (their unpark
    // goes through carrier.execute, which restarts a suspended loop).
    override fun trySuspend(): Boolean {
        return carrier.trySuspend()
    }

    override fun <V> newPromise(): Promise<V> = BlockingPromise(this, carrier)

    override fun <V> newProgressivePromise(): ProgressivePromise<V> = BlockingPromise(this, carrier)

    // --- tasks: submit / newTaskFor / (inherited) invokeAll / invokeAny -----------------------
    // The task type is RunnableNettyTask: java FutureTask underneath (its run() captures the task's
    // dedicated virtual thread, so cancel(true) genuinely INTERRUPTS the running task - something
    // Netty promises can never do), implementing Netty's Future via a BlockingPromise view (VT-safe
    // sync/await, listeners on virtual threads).
    //
    // newTaskFor is overridable here (unlike Netty's AbstractEventExecutor, where it is final and
    // mints guard-locked PromiseTasks - the reason this class avoids that base). Because of it, the
    // INHERITED AbstractExecutorService.invokeAll/invokeAny are correct as-is: each task runs on
    // its own virtual thread via execute(), internal waits use java FutureTask.get() (no Netty
    // dead-lock guard; parks the calling virtual thread), and cancelled losers are interrupted.
    // The submit overloads are restated only for Netty's covariant return type.

    // notifyExecutor = this: listeners notified on the completing virtual thread when in-loop, and
    // re-dispatched onto a fresh virtual thread via execute() otherwise - never on the raw carrier.
    // blockGuard = carrier: awaiting ON the carrier platform thread is the genuine dead-lock the
    // guard exists for; virtual threads just park.

    override fun <T> newTaskFor(runnable: Runnable, value: T): RunnableNettyFuture<T> =
        BlockingTask(Executors.callable(runnable, value), this, carrier)

    override fun <T> newTaskFor(callable: Callable<T>): RunnableNettyFuture<T> =
        BlockingTask(callable, this, carrier)

    // Direct equivalents of AbstractExecutorService.submit (whose base declarations became
    // abstract to resolve the covariant-return clash in the Java superclass): create the
    // RunnableNettyTask, run it on a fresh virtual thread, return it under Netty's Future type.

    override fun submit(task: Runnable): Future<*> {
        val t = super.submit(task)
        return t as Future<*>
    }

    override fun <T> submit(task: Runnable, result: T): Future<T> {
        val t = super.submit(task)
        return t as Future<T>
    }

    override fun <T> submit(task: Callable<T>): Future<T> {
        val t = super.submit(task)
        return t as Future<T>
    }


    override fun next(): IoEventLoop = this

    override fun parent(): IoEventLoopGroup? = group

    // Netty's documented "sensible defaults" for the no-arg variant: 2s quiet period, 15s timeout.
    override fun shutdownGracefully(): Future<*> =
        shutdownGracefully(2, 15, TimeUnit.SECONDS)

    private val children = Collections.singleton(this)

    override fun iterator() = children.iterator()

    @Deprecated("Deprecated in Netty")
    override fun shutdownNow(): MutableList<Runnable> {
        @Suppress("DEPRECATION")
        shutdown()
      //  carrier.executeAfterEventLoopIteration {  }
        return mutableListOf()
    }

    // --- event-loop membership --------------------------------------------------------------
    // A thread belongs to this loop if it is one of *our* virtual threads, i.e. a virtual thread
    // scheduled by our scheduler (this covers both per-task virtual threads and per-event dispatch
    // virtual threads). The running thread is the virtual thread, not the underlying platform loop
    // thread, so we compare its scheduler rather than comparing thread identity.

    // The no-arg variant additionally DISGUISES itself from Netty's dead-lock guard
    // (DefaultPromise.checkDeadLock throws BlockingOperationException when executor().inEventLoop()
    // is true). Detected with a depth-limited StackWalker, the guard gets a HAZARD answer instead of
    // a membership answer:
    //  - our virtual threads: "false" - awaiting parks the thread and frees the carrier, so the
    //    block the guard fears cannot happen (Netty-internal channel promises hard-code the guard,
    //    this is the only way to relax it for them);
    //  - the raw carrier PLATFORM thread: "true" - blocking it really would deadlock (it must stay
    //    free to run the completing continuations), even though it is not "in" the virtual loop for
    //    execution decisions.
    // The match is on method name AND DefaultPromise-family declaring class: notifyListeners() also
    // calls inEventLoop() and must keep getting the truthful answer (else every listener would take
    // an extra execute() hop). Only this no-arg variant is disguised - inEventLoop(Thread) and
    // isExecutorThread stay truthful for pipeline inline-execution and IoHandler decisions.

    // Investigated and closed: a @CallerSensitive Reflection.getCallerClass intrinsic (~10ns) would
    // remove the walk below, but HotSpot ignores the annotation on application-classloader classes
    // (probed empirically: supported=false on JDK 25), and boot-classpath injection cannot work
    // because the annotated method must be inEventLoop() itself, which references Netty types the
    // boot loader cannot see. StackWalker.getCallerClass (~260ns) is the floor.
    override fun inEventLoop(): Boolean {
        val current = Thread.currentThread()
        val member = inEventLoop(current)
        if (!member && !carrier.isExecutorThread(current)) {
            return false
        }
        // Kill-switch (-Dvirtualloop.disguise=false): a static final boolean the JIT folds
        // away, restoring the membership-only answer for deployments that prefer explicit
        // VtFutures/newVtPromise over the transparent disguise.
        if (!DISGUISE_ENABLED) {
            return member
        }
        // Tier 1 (hot path): only DefaultPromise-family callers can be checkDeadLock - every
        // pipeline caller (AbstractChannelHandlerContext etc.) exits here with the truthful
        // membership answer (a hand-rolled skip/limit(1) walk measured ~2x worse than
        // getCallerClass). Family test cached per class via ClassValue.
        val c = CLASS_WALKER.callerClass

        if (!PROMISE_FAMILY.get(c)) {
            return member
        }
  //      println(c)
        // Tier 2 (rare: only await/notify paths): distinguish checkDeadLock (gets the HAZARD
        // answer) from notifyListeners (which must keep the truthful membership answer).
        val askedByGuard = true // calledFromCheckDeadLock()
        return if (member) {
            // Our virtual thread: awaiting parks it and frees the carrier - tell the guard "false".
            !askedByGuard
        } else {
            // The raw carrier PLATFORM thread: blocking it on a future served by this loop is the
            // genuine deadlock the guard exists for - tell the guard "true" and let it throw.
            askedByGuard
        }
    }

    override fun inEventLoop(thread: Thread): Boolean =
        thread.isVirtual && PrivateLoomSupport.schedulerOf(thread) === scheduler

    override fun isExecutorThread(thread: Thread): Boolean = inEventLoop(thread)



    // --- registration & IO-event dispatch ---------------------------------------------------
    // Channels are registered with `this` loop, so channel.eventLoop() resolves to the virtual loop
    // and never exposes the backing SingleThreadIoEventLoop. The registered IoHandle is wrapped per
    // transport (each transport's IoHandler casts the handle to its own sub-type, so a generic
    // wrapper would be rejected). The wrapper's handle(..) re-dispatches the actual IO handling onto
    // a fresh virtual thread via [dispatch]; registered()/unregistered()/close() stay synchronous on
    // the loop thread, as Netty expects for lifecycle ordering.



    private val classCache = InterfaceCache()


    override fun register(handle: IoHandle): Future<IoRegistration> {


        // LinkedTransferQueue, NOT LinkedBlockingQueue - see the taskQueue doc on DelegatedHandle:
        // lock-free add/unpark is what lets the INLINE dispatch mount succeed on the first try.
        val jobList: BlockingQueue<Runnable> = LinkedTransferQueue()

        // getInterfaces() returns only DIRECTLY declared interfaces - the actual handle is e.g.
        // NioSocketChannel$NioSocketChannelUnsafe, which declares none; NioIoHandle comes from a
        // superclass (AbstractNioUnsafe). Walk the whole hierarchy so the proxy really implements
        // the transport's handle interface (each transport's IoHandler casts to its own sub-type).
        // io_uring is completion-based: every event is a distinct completion, so duplicates must
        // never be coalesced there. All readiness-based transports (NIO/epoll/kqueue) may coalesce.
//        val coalesceEvents = handle !is io.netty.channel.uring.IoUringIoHandle
        val continuationHolder = AtomicReference<Runnable>()
        val delegate = DelegatedHandle(handle, jobList, continuationHolder)
        val proxy = java.lang.reflect.Proxy.newProxyInstance(handle.javaClass.classLoader, classCache.get(handle.javaClass), delegate) as IoHandle
        val innerPromise = carrier.register(proxy)

        val outerPromise = newPromise<IoRegistration>()
        outerPromise.setUncancellable()
        // Complete with the WRAPPED registration (not a plain cascade): callers must cancel through
        // DelegatedRegistration so the drain thread gets its direct terminal signal, and the raw
        // inner registration stays hidden.
        innerPromise.addListener { done ->
            if (done.isSuccess) {
                outerPromise.trySuccess(DelegatedRegistration(done.now as IoRegistration, delegate))
            } else {
                outerPromise.tryFailure(done.cause())
            }
        }
        val poll = Runnable {
            val result = runCatching { innerPromise.get() }
                .onSuccess {
                    // Run the already-queued registered() callback BEFORE completing the promise:
                    // completing it fires the register0 listener chain (handlerAdded,
                    // channelRegistered/Active, beginRead) inline on this thread, and the IoHandle
                    // contract promises registered() happens before any of that.
                    while (true) {
                        val queued = jobList.poll() ?: break
                        runCatching { queued.run() }
                    }
                }
            val t = result.getOrNull()
            if (t != null) {
                // This virtual thread runs FOR the registration's lifetime, bounded by the terminal
                // signals (see the flags on DelegatedHandle): it parks in take() while the
                // registration is live, switches to a bounded grace poll once CANCELLED (the
                // trailing close() of Netty's "cancel -> unregistered -> close" cascade must still
                // be caught), and after CLOSE drains the remainder non-blocking and exits
                // immediately - close is a hard terminal, nothing can follow it. isValid is kept as
                // a belt-and-braces companion to the cancelled flag.
                liveHandles.add(delegate)
                try {
                    while (true) {
                        val job = try {
                            when {
                                delegate.closed ->
                                    jobList.poll() ?: break
                                delegate.cancelled || !t.isValid ->
                                    jobList.poll(1, TimeUnit.SECONDS) ?: break
                                else ->
                                    jobList.take()
                            }
                        } catch (_: InterruptedException) {
                            // shutdownNow() interrupted us: run whatever is already queued
                            // (including any pending unregistered()/close()) without blocking,
                            // then exit - never abandon terminal jobs.
                            while (true) {
                                val tail = jobList.poll() ?: break
                                runCatching { tail.run() }
                            }
                            break
                        }
                        try {
                            job.run()
                        } catch (e: Throwable) {
                            // A throwing job (e.g. a pipeline exception escaping handle()) must not
                            // kill the drain thread - later jobs, including close(), still have to run.
                            val self = Thread.currentThread()
                            self.uncaughtExceptionHandler.uncaughtException(self, e)
                        }
                    }
                } finally {
                    liveHandles.remove(delegate)
                }
            }
        }
        // Starting the drain virtual thread inside the interceptor scope captures its (stable,
        // state-CAS-guarded) runContinuation into the holder instead of scheduling it; we then hand
        // that first mount to the carrier explicitly. FIFO with carrier.register's internal task
        // means the drain's innerPromise.get() never actually parks.
        ScopedValue.where(CONTINUATION_INTERCEPTOR, Executor {
            continuationHolder.set(it)
        }).run {
            // FastThreadLocal bridge: the drain virtual thread is LONG-LIVED (it lasts the whole
            // registration), which is exactly what Netty's official virtual-thread hook is for -
            // Recycler keeps a real local pool (instead of NOOP handles) and PooledByteBufAllocator
            // grants a real PoolThreadCache, both cleaned up via FastThreadLocal.removeAll() when
            // the drain exits. Short-lived task virtual threads are deliberately NOT wrapped: the
            // API is documented for "long-running" threads, and per-task caches would just churn.
            // (The 4.2 default adaptive allocator is FTL-agnostic either way.)
            executor.execute {
                FastThreadLocalThread.runWithFastThreadLocal(poll)
            }
        }
        continuationHolder.get()?.let { carrier.execute(it) }
        return outerPromise
    }

    override fun isCompatible(handleType: Class<out IoHandle>): Boolean {
       return carrier.isCompatible(handleType)
    }

    override fun isIoType(handlerType: Class<out IoHandler>): Boolean {
       return carrier.isIoType(handlerType)
    }

    override fun register(channel: Channel): ChannelFuture =
        register(BlockingChannelPromise(channel, this, carrier))

    override fun register(promise: ChannelPromise): ChannelFuture {
        promise.channel().unsafe().register(this, promise)
        return promise
    }

    @Deprecated("Deprecated in Netty", ReplaceWith("register(promise)"))
    override fun register(channel: Channel, promise: ChannelPromise): ChannelFuture {
        channel.unsafe().register(this, promise)
        return promise
    }

    // --- scheduling -------------------------------------------------------------------------
    // Timing is driven by the carrier's timer; the timer callback runs on the carrier thread and
    // starts the task's virtual thread there, so inline(..) lets its first continuation run inline
    // instead of taking another carrier.execute queue hop.
    //
    // Every method returns a DelegatedScheduledFuture over OUR promise, never the carrier's
    // ScheduledFuture directly: the carrier future's executor is the carrier loop, so listeners
    // added to it would be notified ON the carrier platform thread (and completion happens there
    // too), exposing the carrier. Our promise completes on the task's virtual thread, and its
    // listeners are notified either inline on a virtual thread or via execute() (a fresh one).

    override fun schedule(command: Runnable, delay: Long, unit: TimeUnit): ScheduledFuture<*> {
        val newTask = newTaskFor(command, Unit)
        val nettyTask = nettyScheduler.schedule({ inline(newTask) }, delay, unit)
        armDriver()
        return OneShotScheduledFuture(nettyTask, newTask)
    }

    override fun <V> schedule(callable: Callable<V>, delay: Long, unit: TimeUnit): ScheduledFuture<V> {
        val newTask = newTaskFor(callable)
        val nettyTask = nettyScheduler.schedule({ inline(newTask) }, delay, unit)
        armDriver()
        return OneShotScheduledFuture(nettyTask, newTask)
    }


    private val schedulerDriver = SchedulerLoop(nettyScheduler, carrier)

    /** Nudges the driver after any submission to [nettyScheduler], from any thread. */
    private fun armDriver() {
        // The driver's state is carrier-confined; a virtual thread MOUNTED on the carrier is
        // physically on the carrier thread, so it may arm directly (same reasoning as
        // Scheduler.inEventLoop's mounted-VT clause) - this makes a periodic round's re-schedule
        // completely marshalling-free.
        val current = Thread.currentThread()
        val onCarrier = carrier.inEventLoop() ||
            (current.isVirtual && PrivateLoomSupport.carrierOf(current)?.let { carrier.isExecutorThread(it) } == true)
        if (onCarrier) {
            schedulerDriver.arm()
        } else {
            carrier.execute { schedulerDriver.arm() }
        }
    }




    // --- periodic: purpose-built MULTI-STEP chain for virtual-thread execution -----------------
    //
    // Why the stock periodic machinery cannot be reused here: Netty's ScheduledFutureTask computes
    // the next round AFTER task.run() RETURNS - but a virtual-thread body returns its continuation
    // at the FIRST suspension point, not at completion. Delegating to the stock timer therefore
    // measures "time until first park" as the execution time: fixedDelay becomes "delay after first
    // park" and both variants can start the next round while the previous one is still parked -
    // violating the no-concurrent-execution guarantee both contracts make.
    //
    // The chain instead schedules each round as a ONE-SHOT; the round's virtual thread runs the
    // command TO COMPLETION (parks and all) and only then computes the next deadline and schedules
    // the next one-shot. Guarantees: never concurrent; fixedDelay = delay measured from true
    // completion; fixedRate = deadline += period from the original baseline (late rounds catch up
    // back-to-back, Netty/JUC style, but never overlap). The future completes only on cancellation
    // or when a round throws (which also stops the chain); cancel(true) interrupts the in-flight
    // round's virtual thread.

    override fun scheduleAtFixedRate(
        command: Runnable,
        initialDelay: Long,
        period: Long,
        unit: TimeUnit,
    ): ScheduledFuture<*> {
        require(period > 0) { "period: $period (expected > 0)" }
        return PeriodicVirtualTask(command, initialDelay, period, unit, fixedRate = true).also { it.start() }
    }

    override fun scheduleWithFixedDelay(
        command: Runnable,
        initialDelay: Long,
        delay: Long,
        unit: TimeUnit,
    ): ScheduledFuture<*> {
        require(delay > 0) { "delay: $delay (expected > 0)" }
        return PeriodicVirtualTask(command, initialDelay, delay, unit, fixedRate = false).also { it.start() }
    }

    /**
     * Purpose-built MULTI-STEP periodic task for virtual-thread execution, riding
     * [java.util.concurrent.FutureTask.runAndReset] - the exact primitive
     * ScheduledThreadPoolExecutor uses for periodics - instead of hand-rolled state:
     *
     *  - Each round is scheduled as a ONE-SHOT; the round's virtual thread calls [runAndReset],
     *    which runs the command TO COMPLETION (parks included) under FutureTask's CAS state
     *    machine: the runner is captured, so cancel(true) interrupts the in-flight round with the
     *    proper cancellation-interrupt handshake (no interrupt leaks past the round); a throwing
     *    round records the failure terminally; a normal round resets the task to runnable.
     *  - Only after runAndReset returns true is the next deadline computed - fixedRate:
     *    deadline += period from the baseline (late rounds catch up back-to-back, never
     *    concurrently); fixedDelay: true completion + delay - and the next one-shot scheduled.
     *  - Terminal states (cancel, failed round) flow through the inherited [done]: listeners and
     *    the CompletionStage view complete exactly like any other loop task. The future never
     *    succeeds - periodic contract.
     *
     * Thread-safety: the only mutable state outside FutureTask's machinery is [nextDeadlineNanos]
     * (written solely by the single in-flight round, volatile for getDelay readers) and
     * [roundTimer] (racing cancel is closed by the re-check in [scheduleNext]).
     */
    private inner class PeriodicVirtualTask(
        command: Runnable,
        initialDelay: Long,
        period: Long,
        unit: TimeUnit,
        private val fixedRate: Boolean,
    ) : BlockingTask<Unit>(Executors.callable(command, Unit), this@VirtualIoEventLoop, this@VirtualIoEventLoop.carrier),
        ScheduledFuture<Unit> {

        private val periodNanos = unit.toNanos(period)

        /** Next round's absolute deadline on the scheduler's clock; volatile for getDelay readers. */
        @Volatile
        private var nextDeadlineNanos = nettyScheduler.ticker().nanoTime() + unit.toNanos(initialDelay)

        /** The currently armed one-shot (round trigger); replaced every round. */
        @Volatile
        private var roundTimer: ScheduledFuture<*>? = null

        fun start() {
            scheduleNext(nextDeadlineNanos - nettyScheduler.ticker().nanoTime())
        }

        private fun scheduleNext(delayNanos: Long) {
            if (isDone) {
                return
            }
            val timer = try {
                nettyScheduler.schedule({ inline(this::runOnce) }, maxOf(0, delayNanos), TimeUnit.NANOSECONDS)
            } catch (_: Throwable) {
                // The loop is going down (carrier rejected the marshalled add): terminate the chain
                // instead of leaving the future incomplete forever.
                cancel(false)
                return
            }
            // Propagate TIMER cancellation to the chain (mirror of DelegatedScheduledFuture1's
            // one-shot listener): shutdown's cancelScheduledTasks() cancels pending timer tasks
            // directly - without this, the chain would die silently with the future never
            // completing. A successfully-fired timer has isCancelled == false: no-op. Our own
            // cancel() cancelling the timer re-enters cancel(false), which is a guarded no-op.
            timer.addListener {
                if (it.isCancelled) {
                    cancel(false)
                }
            }
            roundTimer = timer
            armDriver()
            if (isDone) {
                timer.cancel(false) // cancellation raced the re-schedule
            }
        }

        private fun runOnce() {
            while (true) {
                // JFR (disabled by default): one duration event per round, lateness = how far past
                // the round's deadline the command actually started.
                var round: PeriodicRound? = null
                if (PeriodicRound.INSTANCE.isEnabled()) {
                    round = PeriodicRound().also {
                        it.fixedRate = fixedRate
                        it.latenessNanos = nettyScheduler.ticker().nanoTime() - nextDeadlineNanos
                        it.begin()
                    }
                }
                // false = cancelled (before or during the round) or the round threw; both are
                // terminal and already recorded by FutureTask - done() has notified
                // listeners/CompletionStage.
                val ran = runAndReset()
                round?.let { it.end(); it.commit() }
                if (!ran) {
                    return
                }
                val now = nettyScheduler.ticker().nanoTime()
                val delayNanos = if (fixedRate) {
                    nextDeadlineNanos += periodNanos
                    nextDeadlineNanos - now
                } else {
                    nextDeadlineNanos = now + periodNanos
                    periodNanos
                }
                if (delayNanos > 0) {
                    scheduleNext(delayNanos)
                    return
                }
                // fixedRate overrun catch-up: the next round is ALREADY due - run it back-to-back
                // on THIS virtual thread instead of paying a timer/queue round trip per late round.
                // Thread.yield() first, for fairness: the yielded continuation is resubmitted from
                // the carrier's own context (inEventLoop -> plain enqueue, no wakeup) BEHIND
                // whatever IO and tasks the carrier has pending, so catch-up bursts can never
                // starve the loop. Safe here: this is plain virtual-thread code, not the
                // scheduler's switchToCarrierThread window.
                Thread.yield()
                if (isDone) {
                    return // cancelled while yielded
                }
            }
        }

        override fun cancel(mayInterruptIfRunning: Boolean): Boolean {
            val cancelled = super.cancel(mayInterruptIfRunning)
            if (cancelled) {
                roundTimer?.cancel(false)
            }
            return cancelled
        }

        override fun getDelay(unit: TimeUnit): Long =
            unit.convert(nextDeadlineNanos - nettyScheduler.ticker().nanoTime(), TimeUnit.NANOSECONDS)

        override fun compareTo(other: Delayed): Int =
            getDelay(TimeUnit.NANOSECONDS).compareTo(other.getDelay(TimeUnit.NANOSECONDS))
    }




    // --- lifecycle --------------------------------------------------------------------------
    // Shutdown design (non-blocking, idempotent, works in both carrier-ownership modes):
    //  1. Flag shuttingDown (drives isShuttingDown and the execute() straggler fallback).
    //  2. Close this loop's OWN registrations by enqueueing close() through each live handle's
    //     serial queue - the same cancel -> unregistered -> close cascade the carrier's
    //     prepareToDestroy would produce, so the drain loop's grace phase already handles it.
    //     This is what makes guest mode a real lifecycle end (channels/FDs actually close)
    //     instead of a mere submission gate.
    //  3. QUIET PHASE on the CARRIER's own timer: a raw runnable (no virtual thread!)
    //     self-reschedules via carrier.schedule at exact deadlines. Raw is load-bearing - it runs
    //     on the carrier thread and never passes through [scheduler], so the watch's own wake-ups
    //     do not stamp [lastActivityNanos]. A loop-carried watcher VT would reset its own quiet
    //     window on every sleep wake-up and never see the loop as quiet. (Deliberately NOT
    //     [nettyScheduler]: the carrier timer needs no driver arming and is a different queue from
    //     the one cancelScheduledTasks clears, so no ordering constraint either.)
    //  4. TERMINATION PHASE on a loop-carried virtual thread: once the quiet decision is made,
    //     activity stamps no longer matter, so a factory-direct VT (NOT executor.execute - a
    //     tracked task would awaitTermination on itself) parks in awaitTermination, escalates to
    //     shutdownNow at the timeout, then completes terminationFuture - completion still means
    //     actual quiescence. Carrier death mid-park resumes via the scheduler's REE fallback.
    //  5. Safety net for guest mode: if the carrier terminates while the quiet watch is pending,
    //     the timer goes silent forever - a carrier terminationFuture listener force-finishes.
    //  The quiet period follows Netty's semantics: any continuation scheduled on this loop (task
    //  start, park-resume, IO drain wake-up - all funnel through [scheduler], which stamps
    //  [lastActivityNanos]) restarts the window; the executor is only shut once the loop has been
    //  quiet for the full period, capped by the hard timeout.
    override fun shutdownGracefully(quietPeriod: Long, timeout: Long, unit: TimeUnit): Future<*> {
        if (!shuttingDown.compareAndSet(false, true)) {
            return terminationFuture
        }
        // JFR (disabled by default): one span for the whole graceful shutdown; committed by the
        // terminationFuture listener so the duration covers quiet wait + termination watch.
        if (LoopShutdown.INSTANCE.isEnabled) {
            val ev = LoopShutdown()
            ev.quietMillis = unit.toMillis(quietPeriod)
            ev.timeoutMillis = unit.toMillis(timeout)
            ev.begin()
            terminationFuture.addListener { _ ->
                ev.end()
                ev.forced = shutdownForced
                ev.commit()
            }
        }
        val quietNanos = unit.toNanos(quietPeriod)
        val timeoutNanos = unit.toNanos(timeout)
        // System.nanoTime, NOT carrier.ticker(): the ticker is epoch-shifted (START_TIME-based),
        // while quietWatch/awaitQuiescence/lastActivityNanos all measure on the System epoch -
        // mixing them makes (now - start) enormous and voids the quiet period instantly.
        val start = System.nanoTime()
        try {
            carrier.execute {
                liveHandles.forEach { runCatching { it.close() } }
                // Drop pending timer work; ScopedValue-disguised so the queue's inEventLoop checks pass.
                ScopedValue.where(nettyScheduler.scopedValue, true).run {
                    nettyScheduler.cancelScheduledTasks()
                }
                quietWatch(start, quietNanos, timeoutNanos)
            }
        } catch (_: RejectedExecutionException) {
            // Carrier already gone (guest mode): no cleanup turn, no timer - skip straight to
            // termination; the awaiter VT rides the scheduler's REE fallback.
            beginTermination(start, timeoutNanos)
        }
        carrier.terminationFuture().addListener {
            if (!terminationFuture.isDone) {
                shutdownForced = true; executor.shutdownNow()
                terminationFuture.trySuccess(Unit)
            }
        }
        return terminationFuture
    }

    /** Quiet-period watch step; runs on the carrier thread (initial turn and every timer re-fire). */
    private fun quietWatch(startNanos: Long, quietNanos: Long, timeoutNanos: Long) {
        val now = System.nanoTime()
        val idle = now - lastActivityNanos.get()
        if (quietNanos <= 0 || now - startNanos >= timeoutNanos || idle >= quietNanos) {
            beginTermination(startNanos, timeoutNanos)
            return
        }
        val waitNanos = minOf(quietNanos - idle, timeoutNanos - (now - startNanos)).coerceAtLeast(1)
        try {
            carrier.schedule({ quietWatch(startNanos, quietNanos, timeoutNanos) }, waitNanos, TimeUnit.NANOSECONDS)
        } catch (_: Throwable) {
            beginTermination(startNanos, timeoutNanos) // timer rejected (carrier dying): stop waiting
        }
    }

    private fun beginTermination(startNanos: Long, timeoutNanos: Long) {
        executor.shutdown()
        awaitQuiescence(startNanos + timeoutNanos, forced = false)
    }

    /**
     * Termination watch step: same carrier-timer self-rescheduling shape as [quietWatch], polling
     * the task executor instead of parking a virtual thread in awaitTermination. The common case
     * (loop already idle when the quiet decision lands) sees isTerminated true on the first check
     * and never schedules at all. On [forced]=false the deadline escalates to shutdownNow plus a
     * 1s forced grace; on forced=true the deadline completes the future regardless.
     */
    private fun awaitQuiescence(deadlineNanos: Long, forced: Boolean) {
        if (executor.isTerminated) {
            terminationFuture.trySuccess(Unit)
            return
        }
        val now = System.nanoTime()
        if (now >= deadlineNanos) {
            if (forced) {
                terminationFuture.trySuccess(Unit)
                return
            }
            shutdownForced = true; executor.shutdownNow()
            awaitQuiescence(now + TimeUnit.SECONDS.toNanos(1), forced = true)
            return
        }
        val waitNanos = minOf(deadlineNanos - now, TERMINATION_POLL_NANOS)
        try {
            carrier.schedule({ awaitQuiescence(deadlineNanos, forced) }, waitNanos, TimeUnit.NANOSECONDS)
        } catch (_: Throwable) {
            // Carrier dying mid-watch: nothing left to poll from - force-finish, exactly like the
            // carrier-terminationFuture safety net would.
            shutdownForced = true; executor.shutdownNow()
            terminationFuture.trySuccess(Unit)
        }
    }

    override fun terminationFuture(): Future<Unit> {
        return terminationFuture
    }

    @Deprecated("Deprecated in Netty")
    override fun shutdown() {
        shutdownGracefully(0, 100, TimeUnit.MILLISECONDS)
    }

    override fun awaitTermination(p0: Long, p1: TimeUnit): Boolean {
        return terminationFuture.await(p0, p1)
    }

    override fun isShutdown(): Boolean {
        return executor.isShutdown
    }

    override fun isTerminated(): Boolean {
        return terminationFuture.isDone
    }

    override fun isShuttingDown(): Boolean {
        return shuttingDown.get()
    }





}

/** Poll cadence of the shutdown termination watch (see [VirtualIoEventLoop.awaitQuiescence]). */
private val TERMINATION_POLL_NANOS = TimeUnit.MILLISECONDS.toNanos(10)
