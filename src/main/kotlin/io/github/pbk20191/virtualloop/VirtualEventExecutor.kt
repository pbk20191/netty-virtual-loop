package io.github.pbk20191.virtualloop

import io.netty.util.concurrent.*
import io.netty.util.concurrent.Future
import io.netty.util.concurrent.ScheduledFuture
import io.netty.util.internal.ThreadExecutorMap
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

/**
 * The MINIMAL-INVASION adoption path: keep a 100% vanilla Netty event loop group and only move
 * the BLOCKING handlers onto virtual threads, per handler, via Netty's own offload mechanism:
 *
 * ```
 * pipeline.addLast(virtualExecutorGroup, blockingHandler)
 * ```
 *
 * Each [next] mints a fresh child [VirtualEventExecutor] (pairing with Netty's default
 * SINGLE_EVENTEXECUTOR_PER_GROUP so each pipeline gets one). The child runs everything on ONE
 * long-lived drain virtual thread whose Loom scheduler forwards continuations to the CHANNEL'S
 * OWN EVENT LOOP - captured automatically from [ThreadExecutorMap.currentExecutor] on the first
 * submission (which is always the loop dispatching handlerAdded). So the handler:
 *  - runs on a virtual thread CARRIED BY the channel's loop thread (locality preserved, no
 *    cross-pool hop as with a classic offload thread pool),
 *  - may block freely - the drain parks, the loop thread keeps polling IO,
 *  - keeps Netty's offload ordering contract (serial, never-overlapping per context),
 *  - may call `future.sync()` UNMODIFIED: channel-level futures (closeFuture etc.) belong to the
 *    loop executor and pass naturally; ctx.write promises belong to THIS lane, where the same
 *    StackWalker dead-lock disguise as the full loop answers the guard (see [inEventLoop]).
 *
 * Contrast with [VirtualIoEventLoopGroup]: that replaces the group and dispatches EVERY IO event
 * on virtual threads; this leaves the group untouched and virtualizes only the handlers you mark.
 * Writes from the drain pay one loop-queue hop (exactly like the classic offload pattern).
 *
 * v1 limits: the quietPeriod of shutdownGracefully is not honored (drain exits when the queue is
 * empty); if the first submission does not come from a Netty executor thread the drain falls
 * back to the JDK's default virtual-thread scheduler (unpinned, still correct).
 */
class VirtualEventExecutorGroup : AbstractEventExecutorGroup() {

    private val children = ConcurrentHashMap.newKeySet<VirtualEventExecutor>()
    private val terminationPromise = DefaultPromise<Unit>(GlobalEventExecutor.INSTANCE)
    private val shuttingDown = AtomicBoolean(false)

    override fun next(): EventExecutor {
        if (shuttingDown.get()) {
            throw RejectedExecutionException("VirtualEventExecutorGroup is shutting down")
        }
        val child = VirtualEventExecutor(this)
        children.add(child)
        return child
    }

    override fun iterator(): MutableIterator<EventExecutor> =
        children.toMutableList<EventExecutor>().iterator()

    override fun isShuttingDown(): Boolean = shuttingDown.get()

    override fun shutdownGracefully(quietPeriod: Long, timeout: Long, unit: TimeUnit): Future<*> {
        if (!shuttingDown.compareAndSet(false, true)) {
            return terminationPromise
        }
        val kids = children.toList()
        if (kids.isEmpty()) {
            terminationPromise.trySuccess(Unit)
            return terminationPromise
        }
        val remaining = AtomicInteger(kids.size)
        kids.forEach { child ->
            child.shutdownGracefully(quietPeriod, timeout, unit).addListener {
                if (remaining.decrementAndGet() == 0) {
                    terminationPromise.trySuccess(Unit)
                }
            }
        }
        return terminationPromise
    }

    override fun terminationFuture(): Future<*> = terminationPromise

    @Deprecated("Deprecated in Netty")
    override fun shutdown() {
        shutdownGracefully(0, 100, TimeUnit.MILLISECONDS)
    }

    override fun isShutdown(): Boolean = shuttingDown.get()

    override fun isTerminated(): Boolean = terminationPromise.isDone

    override fun awaitTermination(timeout: Long, unit: TimeUnit): Boolean =
        terminationPromise.await(timeout, unit)
}

/**
 * One offload lane: a serial task queue drained by a single long-lived virtual thread carried by
 * the captured channel event loop. See [VirtualEventExecutorGroup] for the model.
 */
class VirtualEventExecutor internal constructor(
    private val group: VirtualEventExecutorGroup?,
) : AbstractExecutorService(), OrderedEventExecutor {

    private val taskQueue = LinkedTransferQueue<Runnable>()
    private val continuationHolder = AtomicReference<Runnable>()
    private val captureScope = ScopedValue.where(CONTINUATION_INTERCEPTOR, Executor { continuationHolder.set(it) })
    private val terminationPromise = DefaultPromise<Unit>(GlobalEventExecutor.INSTANCE)
    private val drainStarted = AtomicBoolean(false)

    @Volatile
    private var shuttingDown = false

    /** The channel's event loop, captured from the first submission's ThreadExecutorMap entry. */
    @Volatile
    private var capturedCarrier: EventExecutor? = null

    /** The drain virtual thread; identity check backs [inEventLoop]. */
    @Volatile
    private var drainThread: Thread? = null

    /** Blocking-guard view of the captured carrier: only ITS platform thread must never block. */
    private val carrierGuard = object : ThreadAwareExecutor {
        override fun execute(command: Runnable) {
            (capturedCarrier ?: GlobalEventExecutor.INSTANCE).execute(command)
        }
        override fun isExecutorThread(thread: Thread): Boolean =
            capturedCarrier?.inEventLoop(thread) ?: false
    }

    /** Loom scheduler of the drain VT: continuations become tasks of the captured loop. */
    private val vtScheduler = Executor { continuation ->
        if (CONTINUATION_INTERCEPTOR.isBound) {
            CONTINUATION_INTERCEPTOR.get().execute(continuation)
            return@Executor
        }
        val carrier = capturedCarrier
        if (carrier == null) {
            // Uncaptured fallback lane (first submit was not from a Netty executor thread).
            (PrivateLoomSupport.defaultCarrierScheduler ?: ForkJoinPool.commonPool()).execute(continuation)
            return@Executor
        }
        // Same-carrier fast path, as in VirtualIoEventLoop's scheduler: a virtual thread mounted
        // on the captured loop's thread proves the loop is awake - enqueue without a wakeup.
        val current = Thread.currentThread()
        val mountedOn = if (current.isVirtual) PrivateLoomSupport.carrierOf(current) else null
        try {
            if (carrier is SingleThreadEventExecutor && mountedOn != null && carrier.inEventLoop(mountedOn)) {
                carrier.lazyExecute(continuation)
            } else {
                carrier.execute(continuation)
            }
        } catch (e: RejectedExecutionException) {
            if (!carrier.isShuttingDown && !shuttingDown) throw e
            // The loop died under us mid-park: losing carrier affinity beats freezing the drain.
            (PrivateLoomSupport.defaultCarrierScheduler ?: ForkJoinPool.commonPool()).execute(continuation)
        }
    }

    // --- execute: capture, start drain lazily, enqueue with the inline-mount trick -------------

    override fun execute(command: Runnable) {
        if (isTerminated) {
            // Teardown straggler (channelInactive/handlerRemoved arriving after this lane was
            // shut, e.g. because the IO group is being torn down last): rejecting would silently
            // drop lifecycle events, so run it on an untracked virtual thread. Ordering is
            // best-effort at this point, same rationale as VirtualIoEventLoop's fallback.
            Thread.ofVirtual().name("virtual-exec-straggler").start(command)
            return
        }
        if (drainStarted.compareAndSet(false, true)) {
            // First submission: in pipeline use this is the channel's loop dispatching
            // handlerAdded, so ThreadExecutorMap knows the loop. Capture BEFORE starting the
            // drain so its virtual thread is born with the right scheduler.
            val current = ThreadExecutorMap.currentExecutor()
            // Never capture ANOTHER lane as the carrier (the drain registers itself in
            // ThreadExecutorMap, so a submission from lane A's drain would otherwise be captured
            // by lane B): a lane's drain is a virtual thread, and a virtual thread cannot carry
            // another virtual thread's continuations.
            if (current != null && current !== this && current !is VirtualEventExecutor) {
                capturedCarrier = current
            }
            startDrain()
        }
        enqueueAndRun(command)
    }

    /**
     * Same trick as DelegatedHandle.enqueueAndRun: when the caller IS the captured loop's
     * platform thread, bind the interceptor around the add (the queue's unpark fires our
     * scheduler synchronously, the capture diverts the fresh continuation into the holder) and
     * mount the drain right here - the event begins executing without a loop-queue round trip.
     */
    private fun enqueueAndRun(job: Runnable) {
        val carrier = capturedCarrier
        if (carrier != null && carrier.inEventLoop() && !Thread.currentThread().isVirtual) {
            // Clear first so only a continuation captured by THIS add is mounted - a stale
            // holder from a previous cycle must not be run while the drain is parked elsewhere
            // (e.g. in a promise await rather than the queue take).
            continuationHolder.set(null)
            captureScope.run {
                taskQueue.add(job)
            }
            continuationHolder.get()?.run()
        } else {
            taskQueue.add(job)
        }
    }

    private fun startDrain() {
        val builder = Thread.ofVirtual().name("virtual-exec-drain")
        if (capturedCarrier != null && PrivateLoomSupport.isSupported) {
            PrivateLoomSupport.setScheduler(builder, vtScheduler)
        }
        val drain = builder.unstarted {
            drainThread = Thread.currentThread()
            ThreadExecutorMap.apply(
                Runnable{
                    FastThreadLocalThread.runWithFastThreadLocal {
                        while (true) {
                            if (shuttingDown && taskQueue.isEmpty()) {
                                break
                            }
                            val job = try {
                                taskQueue.poll(100, TimeUnit.MILLISECONDS) ?: continue
                            } catch (_: InterruptedException) {
                                continue // cancel(true) of an already-finished round, or shutdown nudge
                            }
                            if (job === WAKE) continue
                            try {
                                job.run()
                            } catch (t: Throwable) {
                                val self = Thread.currentThread()
                                self.uncaughtExceptionHandler.uncaughtException(self, t)
                            }
                        }
                    }
                },
                this
            ).run()
            terminationPromise.trySuccess(Unit)
        }
        if (capturedCarrier != null) {
            // Interceptor-captured first mount, FIFO behind whatever the loop is doing.
            ScopedValue.where(CONTINUATION_INTERCEPTOR, Executor { continuationHolder.set(it) }).run {
                drain.start()
            }
            continuationHolder.get()?.let { carrierGuard.execute(it) }
        } else {
            drain.start()
        }
    }

    // --- membership / promises ----------------------------------------------------------------

    override fun inEventLoop(thread: Thread): Boolean = thread === drainThread

    // Write promises minted by ctx.write BELONG TO THIS LANE (AbstractChannelHandlerContext
    // creates them with the context's executor). That makes the no-arg answer here LOAD-BEARING
    // in three conflicting directions, discriminated by caller (StackWalker), because Netty 4.2's
    // promise plumbing would otherwise SELF-DEADLOCK an unmodified handler sync():
    //  1. ensurePromiseUseCorrectExecutor (every outbound op) replaces any promise whose executor
    //     is not "in" on the current thread and CASCADES completion through a listener - and
    //     DefaultPromise dispatches listener notification through the promise's executor. For a
    //     lane promise completed on the LOOP, that queues the cascade BEHIND the drain job that
    //     is awaiting it: a deadlock stock offload executors surface as BlockingOperationException.
    //     Answer: the captured loop thread IS a member -> no replacement, notification runs
    //     inline on the loop (vanilla-parity for listeners).
    //  2. DefaultPromise family: same loop-is-member answer for notification dispatch; but
    //     checkDeadLock asked ON THE DRAIN gets "false" (awaiting parks the drain, the loop
    //     completes the promise independently - the disguise, as in VirtualIoEventLoop). Asked on
    //     the LOOP thread it stays "true": the loop awaiting a lane promise IS the real deadlock.
    //  3. Everything else - crucially pipeline DISPATCH (invokeChannelRead etc., the SAME class
    //     as ensure!) - gets pure drain identity, or handlers would run inline on the loop.
    override fun inEventLoop(): Boolean {
        val current = Thread.currentThread()
        val member = current === drainThread
        if (!DISGUISE_ENABLED) {
            return member
        }
        val caller = CLASS_WALKER.callerClass
        if (PIPELINE_CONTEXT.get(caller)) {
            if (!member && calledFromEnsurePromiseExecutor()) {
                return capturedCarrier?.inEventLoop(current) ?: false
            }
            return member
        }
        if (PROMISE_FAMILY.get(caller)) {
            if (member) {
                return !calledFromCheckDeadLock()
            }
            return capturedCarrier?.inEventLoop(current) ?: false
        }
        return member
    }

    override fun <V> newPromise(): Promise<V> = BlockingPromise(this, carrierGuard)

    override fun <V> newProgressivePromise(): ProgressivePromise<V> = BlockingPromise(this, carrierGuard)

    // Task futures ride RunnableNettyTask like the full loop: cancel(true) interrupts, awaits
    // from virtual threads park. Guard = the captured loop's platform thread only; awaiting a
    // task of THIS executor from ITS OWN drain is a genuine self-deadlock we do not detect (the
    // same hazard exists on Netty's DefaultEventExecutor).
    override fun <T> newTaskFor(runnable: Runnable, value: T): RunnableNettyFuture<T> =
        BlockingTask(Executors.callable(runnable, value), this, carrierGuard)

    override fun <T> newTaskFor(callable: Callable<T>): RunnableNettyFuture<T> =
        BlockingTask(callable, this, carrierGuard)

    override fun submit(task: Runnable): Future<*> = super.submit(task) as Future<*>

    @Suppress("UNCHECKED_CAST")
    override fun <T> submit(task: Runnable, result: T): Future<T> = super.submit(task, result) as Future<T>

    @Suppress("UNCHECKED_CAST")
    override fun <T> submit(task: Callable<T>): Future<T> = super.submit(task) as Future<T>

    // --- timer: reuse the loop-owned Scheduler/SchedulerLoop machinery, carrier = captured loop.
    // Fired callbacks are re-enqueued onto the serial drain, preserving the executor's ordering
    // contract (a timeout callback never overlaps channelRead of the same handler).

    private val timerInit = AtomicBoolean(false)

    @Volatile
    private var nettyScheduler: Scheduler? = null

    @Volatile
    private var schedulerDriver: SchedulerLoop? = null

    private fun timer(): Scheduler {
        if (timerInit.compareAndSet(false, true)) {
            val timerCarrier = capturedCarrier ?: GlobalEventExecutor.INSTANCE
            val sched = Scheduler(this, carrierGuard)
            nettyScheduler = sched
            schedulerDriver = SchedulerLoop(sched, timerCarrier)
        }
        while (nettyScheduler == null) Thread.onSpinWait()
        return nettyScheduler!!
    }

    private fun armDriver() {
        val driver = schedulerDriver ?: return
        val carrier = capturedCarrier ?: GlobalEventExecutor.INSTANCE
        if (carrier.inEventLoop()) {
            driver.arm()
        } else {
            carrier.execute { driver.arm() }
        }
    }

    override fun schedule(command: Runnable, delay: Long, unit: TimeUnit): ScheduledFuture<*> {
        val newTask = newTaskFor(command, Unit)
        val nettyTask = timer().schedule({ execute(newTask) }, delay, unit)
        armDriver()
        return OneShotScheduledFuture(nettyTask, newTask)
    }

    override fun <V> schedule(callable: Callable<V>, delay: Long, unit: TimeUnit): ScheduledFuture<V> {
        val newTask = newTaskFor(callable)
        val nettyTask = timer().schedule({ execute(newTask) }, delay, unit)
        armDriver()
        return OneShotScheduledFuture(nettyTask, newTask)
    }

    override fun scheduleAtFixedRate(command: Runnable, initialDelay: Long, period: Long, unit: TimeUnit): ScheduledFuture<*> {
        require(period > 0) { "period must be positive" }
        return SerialPeriodic(command, unit.toNanos(initialDelay), unit.toNanos(period), fixedRate = true).also { it.start() }
    }

    override fun scheduleWithFixedDelay(command: Runnable, initialDelay: Long, delay: Long, unit: TimeUnit): ScheduledFuture<*> {
        require(delay > 0) { "delay must be positive" }
        return SerialPeriodic(command, unit.toNanos(initialDelay), unit.toNanos(delay), fixedRate = false).also { it.start() }
    }

    /**
     * Periodic chain for the serial lane: each round is a one-shot timer whose callback enqueues
     * the round onto the drain; the NEXT round is armed only after the round truly completes
     * (runAndReset), so rounds never overlap and fixedDelay measures from true completion - the
     * same semantics as the full loop's PeriodicVirtualTask, with the drain replacing the
     * per-round virtual thread.
     */
    private inner class SerialPeriodic(
        command: Runnable,
        initialDelayNanos: Long,
        private val periodNanos: Long,
        private val fixedRate: Boolean,
    ) : BlockingTask<Unit>(Executors.callable(command, Unit), this@VirtualEventExecutor, carrierGuard),
        ScheduledFuture<Unit> {

        @Volatile
        private var nextDeadlineNanos = timer().ticker().nanoTime() + initialDelayNanos

        fun start() = scheduleNext(nextDeadlineNanos - timer().ticker().nanoTime())

        private fun scheduleNext(delayNanos: Long) {
            if (isDone) return
            val timerFuture = try {
                timer().schedule({ execute(this::runOnce) }, maxOf(0, delayNanos), TimeUnit.NANOSECONDS)
            } catch (_: Throwable) {
                cancel(false)
                return
            }
            timerFuture.addListener { if (it.isCancelled) cancel(false) }
            armDriver()
            if (isDone) timerFuture.cancel(false)
        }

        private fun runOnce() {
            if (!runAndReset()) return
            val now = timer().ticker().nanoTime()
            val delayNanos = if (fixedRate) {
                nextDeadlineNanos += periodNanos
                nextDeadlineNanos - now
            } else {
                nextDeadlineNanos = now + periodNanos
                periodNanos
            }
            // Overrun catch-up re-enters through the drain queue (not back-to-back on a thread):
            // the serial lane must let other queued events interleave between late rounds.
            scheduleNext(maxOf(0, delayNanos))
        }

        override fun getDelay(unit: TimeUnit): Long =
            unit.convert(nextDeadlineNanos - timer().ticker().nanoTime(), TimeUnit.NANOSECONDS)

        override fun compareTo(other: Delayed): Int =
            getDelay(TimeUnit.NANOSECONDS).compareTo(other.getDelay(TimeUnit.NANOSECONDS))
    }

    // --- lifecycle ------------------------------------------------------------------------------

    override fun isShuttingDown(): Boolean = shuttingDown

    override fun shutdownGracefully(): Future<*> = shutdownGracefully(2, 15, TimeUnit.SECONDS)

    override fun shutdownGracefully(quietPeriod: Long, timeout: Long, unit: TimeUnit): Future<*> {
        if (shuttingDown) return terminationPromise
        shuttingDown = true
        nettyScheduler?.let { sched ->
            val timerCarrier = capturedCarrier ?: GlobalEventExecutor.INSTANCE
            try {
                timerCarrier.execute {
                    ScopedValue.where(sched.scopedValue, true).run { sched.cancelScheduledTasks() }
                }
            } catch (_: RejectedExecutionException) {
                // Carrier already terminated: its timer died with it, nothing left to cancel.
            }
        }
        if (!drainStarted.get()) {
            terminationPromise.trySuccess(Unit)
        } else {
            taskQueue.add(WAKE)
        }
        return terminationPromise
    }

    override fun terminationFuture(): Future<*> = terminationPromise

    // Same rationale as VirtualIoEventLoop.close(): the JDK ExecutorService.close() default would
    // route through the deprecated 100ms shutdown(); and the lane's own drain thread cannot wait
    // for itself.
    override fun close() {
        shutdownGracefully(2, 15, TimeUnit.SECONDS)
        val current = Thread.currentThread()
        if (inEventLoop(current) || (capturedCarrier?.inEventLoop(current) == true)) {
            return
        }
        var interrupted = false
        while (!terminationPromise.isDone) {
            try {
                terminationPromise.await()
            } catch (_: InterruptedException) {
                interrupted = true
            }
        }
        if (interrupted) {
            current.interrupt()
        }
    }

    @Deprecated("Deprecated in Netty")
    override fun shutdown() {
        shutdownGracefully(0, 100, TimeUnit.MILLISECONDS)
    }

    @Deprecated("Deprecated in Netty")
    override fun shutdownNow(): MutableList<Runnable> {
        @Suppress("DEPRECATION")
        shutdown()
        return mutableListOf()
    }

    override fun isShutdown(): Boolean = shuttingDown

    override fun isTerminated(): Boolean = terminationPromise.isDone

    override fun awaitTermination(timeout: Long, unit: TimeUnit): Boolean =
        terminationPromise.await(timeout, unit)

    override fun parent(): EventExecutorGroup? = group

    override fun next(): EventExecutor = this

    override fun iterator(): MutableIterator<EventExecutor> =
        mutableListOf<EventExecutor>(this).iterator()

    private companion object {
        /** No-op job: wakes the parked drain so it re-checks the shutdown flag. */
        val WAKE = Runnable { }
    }
}
