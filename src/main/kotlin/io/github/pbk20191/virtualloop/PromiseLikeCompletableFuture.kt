package io.github.pbk20191.virtualloop

import io.netty.util.concurrent.*
import java.util.Queue
import java.util.concurrent.*
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import io.netty.util.concurrent.Future as NettyFuture

/**
 * A [CompletableFuture] that is also a full Netty [ProgressivePromise]: the CompletableFuture IS
 * the state machine (one source of truth - no view-synchronization window), while the Netty surface
 * adds listeners, sync/await, progress events and promise completion semantics on top. Useful
 * wherever legacy code wants to hand the same object to Netty APIs (as a Promise) and to
 * CompletionStage-based code (thenApply/whenComplete/...).
 *
 * Completion notification rides a single `whenComplete` dependent registered at construction, so
 * every standard completion path (complete, completeExceptionally, cancel, completeAsync,
 * completeOnTimeout, ...) triggers Netty listener notification without per-path hooks. The
 * `obtrude*` overrides re-notify manually, since obtruding does not re-run dependents.
 *
 * Netty promise contract notes: [setSuccess]/[setFailure] throw IllegalStateException when already
 * complete (try* variants return false); [setUncancellable] blocks subsequent [cancel];
 * [cause] reports the shared stackless CancellationException for cancelled state.
 */
open class PromiseLikeCompletableFuture<V> : CompletableFuture<V>(), ProgressivePromise<V> {

    private val lock = ReentrantLock(true)

    private val list = ArrayList<GenericFutureListener<out NettyFuture<in V>>>()

    private val notifying = AtomicBoolean(false)

    private val uncancellable = AtomicBoolean(false)

    init {
        // One dependent covers every standard completion path.
        super.thenRun(this::notifyListeners)
    }

    // --- Netty future surface -----------------------------------------------------------------

    override fun isSuccess(): Boolean = state() == Future.State.SUCCESS

    override fun isCancellable(): Boolean = !isDone && !uncancellable.get()

    override fun cause(): Throwable? = when (state()) {
        Future.State.SUCCESS, Future.State.RUNNING -> null
        Future.State.FAILED -> exceptionNow()
        Future.State.CANCELLED -> NettyListenerSupport.cancellationCause
    }

    override fun getNow(): V? = takeIf { isSuccess }?.resultNow()

    // --- promise completion ---------------------------------------------------------------------

    override fun setSuccess(result: V?): ProgressivePromise<V> {
        if (!trySuccess(result)) {
            throw IllegalStateException("complete already: $this")
        }
        return this
    }

    override fun trySuccess(result: V?): Boolean = super.complete(result)

    override fun setFailure(cause: Throwable): ProgressivePromise<V> {
        if (!tryFailure(cause)) {
            throw IllegalStateException("complete already: $this", cause)
        }
        return this
    }

    override fun tryFailure(cause: Throwable): Boolean = super.completeExceptionally(cause)

    override fun setUncancellable(): Boolean {
        if (isDone) {
            return false
        }
        return uncancellable.compareAndSet(false, true)
    }

    override fun cancel(mayInterruptIfRunning: Boolean): Boolean =
        !uncancellable.get() && super.cancel(mayInterruptIfRunning)

    // obtrude* replaces the outcome without re-running dependents, so the construction-time
    // whenComplete will not fire again - re-notify Netty listeners manually.

    override fun obtrudeValue(value: V?) {
        super.obtrudeValue(value)
        notifyListeners()
    }

    override fun obtrudeException(ex: Throwable) {
        super.obtrudeException(ex)
        notifyListeners()
    }

    // --- progress -------------------------------------------------------------------------------

    override fun setProgress(progress: Long, total: Long): ProgressivePromise<V> {
        if (!tryProgress(progress, total)) {
            throw IllegalStateException("complete already: $this")
        }
        return this
    }

    override fun tryProgress(progress: Long, total: Long): Boolean {
        // DefaultProgressivePromise contract: total < 0 means "unknown total" (progress just has to
        // be non-negative); otherwise 0 <= progress <= total. No progress after completion.
        if (total < 0) {
            if (progress < 0 || isDone) {
                return false
            }
        } else if (progress < 0 || progress > total || isDone) {
            return false
        }
        val snapshot = lock.withLock { list.toMutableList() }.filterIsInstance<GenericProgressiveFutureListener<*>>()
        for (listener in snapshot) {
            try {
                @Suppress("UNCHECKED_CAST")
                (listener as GenericProgressiveFutureListener<ProgressiveFuture<V>>)
                    .operationProgressed(this, progress, total)
            } catch (t: Throwable) {
                val self = Thread.currentThread()
                self.uncaughtExceptionHandler.uncaughtException(self, t)
            }
        }
        return true
    }

    // --- listeners ------------------------------------------------------------------------------

    override fun addListener(listener: GenericFutureListener<out NettyFuture<in V>>): ProgressivePromise<V> =
        addListeners(listener)

    override fun addListeners(vararg listeners: GenericFutureListener<out NettyFuture<in V>>): ProgressivePromise<V> {
        lock.withLock {
            list.addAll(listeners)
        }
        if (isDone) {
            notifyListeners()
        }
        return this
    }

    override fun removeListener(listener: GenericFutureListener<out NettyFuture<in V>>): ProgressivePromise<V> =
        removeListeners(listener)

    override fun removeListeners(vararg listeners: GenericFutureListener<out NettyFuture<in V>>): ProgressivePromise<V> {
        lock.withLock {
            list.removeAll(listeners.toSet())
        }
        return this
    }

    protected open val executor: EventExecutor get() = ImmediateEventExecutor.INSTANCE

    private fun notifyListeners() {
        if (executor.isExecutorThread(Thread.currentThread())) {
            notify0()
        } else {
            executor.execute(this::notify0)
        }
    }

    /** Same lost-listener-race-free drain as [RunnableNettyTask.notify0]. */
    private fun notify0() {
        while (true) {
            if (!notifying.compareAndSet(false, true)) {
                return
            }
            try {
                while (true) {
                    val batch = lock.withLock {
                        if (list.isEmpty()) {
                            null
                        } else {
                            val snapshot = list.toTypedArray()
                            list.clear()
                            snapshot
                        }
                    } ?: break
                    for (listener in batch) {
                        NettyListenerSupport.notifyOne(executor, this, listener)
                    }
                }
            } finally {
                notifying.set(false)
            }
            if (lock.withLock { list.isEmpty() }) {
                return
            }
        }
    }

    // --- blocking waits (CompletableFuture machinery: virtual-thread-park friendly) -------------

    /** Blocking the guarded platform thread would genuinely dead-lock; virtual threads just park. */
    protected open fun checkDeadLock() {

    }

    @Throws(InterruptedException::class)
    override fun sync(): ProgressivePromise<V> {
        await()
        cause()?.let { throw it }
        return this
    }

    override fun syncUninterruptibly(): ProgressivePromise<V> {
        awaitUninterruptibly()
        cause()?.let { throw it }
        return this
    }

    @Throws(InterruptedException::class)
    override fun await(): ProgressivePromise<V> {
        if (isDone) {
            return this
        }
        checkDeadLock()
        try {
            get()
        } catch (_: CancellationException) {
        } catch (_: ExecutionException) {
        }
        return this
    }

    override fun awaitUninterruptibly(): ProgressivePromise<V> {
        if (isDone) {
            return this
        }
        checkDeadLock()
        try {
            join()
        } catch (_: CompletionException) {
        } catch (_: CancellationException) {
        }
        return this
    }

    @Throws(InterruptedException::class)
    private fun await0(timeout: Long, unit: TimeUnit, interruptable: Boolean): Boolean {
        if (isDone) {
            return true
        }
        if (timeout <= 0) {
            return isDone
        }
        if (interruptable && Thread.interrupted()) {
            throw InterruptedException(toString())
        }
        checkDeadLock()

        if (interruptable) {
            try {
                get(timeout, unit)
            } catch (_: TimeoutException) {
                return false
            } catch (_: CancellationException) {
            } catch (_: ExecutionException) {
            }
            return true
        }

        val startTime = System.nanoTime()
        val timeoutNanos = unit.toNanos(timeout)
        var interrupted = false
        var waitTime = timeoutNanos
        try {
            while (!isDone && waitTime > 0) {
                try {
                    get(waitTime, TimeUnit.NANOSECONDS)
                    return true
                } catch (_: InterruptedException) {
                    interrupted = true
                } catch (_: TimeoutException) {
                    return false
                } catch (_: CancellationException) {
                    return true
                } catch (_: ExecutionException) {
                    return true
                }
                waitTime = timeoutNanos - (System.nanoTime() - startTime)
            }
            return isDone
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt()
            }
        }
    }

    @Throws(InterruptedException::class)
    override fun await(timeout: Long, unit: TimeUnit): Boolean = await0(timeout, unit, true)

    @Throws(InterruptedException::class)
    override fun await(timeoutMillis: Long): Boolean = await0(timeoutMillis, TimeUnit.MILLISECONDS, true)

    override fun awaitUninterruptibly(timeout: Long, unit: TimeUnit): Boolean = await0(timeout, unit, false)

    override fun awaitUninterruptibly(timeoutMillis: Long): Boolean =
        await0(timeoutMillis, TimeUnit.MILLISECONDS, false)
}

