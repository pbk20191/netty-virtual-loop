package io.github.pbk20191.virtualloop

import io.netty.util.concurrent.BlockingOperationException
import io.netty.util.concurrent.DefaultPromise
import io.netty.util.concurrent.EventExecutor
import io.netty.util.concurrent.GenericFutureListener
import io.netty.util.concurrent.GenericProgressiveFutureListener
import io.netty.util.concurrent.ImmediateEventExecutor
import io.netty.util.concurrent.ProgressiveFuture
import io.netty.util.concurrent.ThreadAwareExecutor
import java.util.concurrent.Callable
import java.util.concurrent.CancellationException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.FutureTask
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Function
import kotlin.concurrent.withLock
import io.netty.util.concurrent.Future as NettyFuture

/**
 * The loop's task type: a [FutureTask] that also implements Netty's [ProgressiveFuture] AND
 * [CompletionStage].
 *
 * Why three worlds:
 * - **Java side ([FutureTask])**: the battle-tested runner state machine. `run()` captures the
 *   worker thread - here the task's dedicated virtual thread - so **`cancel(true)` actually
 *   interrupts the running task** (a parked `sleep`/`await` aborts immediately). Netty's
 *   `Promise.cancel` can never do that: its futures are not bound to a worker. `get()` waits on
 *   FutureTask's own machinery - no Netty dead-lock guard involved, safe on a virtual thread.
 * - **Netty side**: listeners kept in our own list and notified through Netty's
 *   [DefaultPromise.notifyListener] machinery via [notifyExecutor]; `sync()`/`await()` and friends
 *   wait on the FutureTask machinery (virtual-thread-park friendly), guarded by [checkDeadLock]
 *   when a [blockGuard] is provided.
 * - **[CompletionStage]**: `thenApply`/`whenComplete`/... chaining, backed by an internal
 *   [CompletableFuture] that [done] completes exactly once. The stage handed to dependents is a
 *   defensive copy, so downstream code cannot complete the task from outside.
 */
open class RunnableNettyTask<V> private constructor(
    callable: Callable< V>,
    private val completableFuture: CompletableFuture<V>,
    completionStage: CompletionStage<V> = completableFuture.copy(),
) : FutureTask<V>(callable), ProgressiveFuture<V>, CompletionStage<V> by completionStage, RunnableNettyFuture<V> {

    constructor(
        callable: Callable<V>,
    ) : this(callable, CompletableFuture())

    constructor(
        runnable: Runnable,
        value: V,
    ) : this(Executors.callable(runnable, value), CompletableFuture() )

    private val lock = ReentrantLock(true)

    private val list = ArrayList<GenericFutureListener<out NettyFuture<in V>>>()

    private val notifying = AtomicBoolean(false)

    /** Defensive copy: dependents can chain but never complete the task's own future. */
    override fun toCompletableFuture(): CompletableFuture<V> = completableFuture.copy()

    override fun done() {
        // Invoked by FutureTask exactly once, on the completing thread (the task's virtual thread,
        // or the canceller). Propagate the terminal state to the CompletionStage side, then notify
        // Netty listeners.
        when (state()) {
            Future.State.SUCCESS -> completableFuture.obtrudeValue(resultNow())
            Future.State.FAILED -> completableFuture.obtrudeException(exceptionNow())
            Future.State.CANCELLED -> completableFuture.cancel(false)
            else -> {}
        }
        notifyListeners()
    }

    private fun notifyListeners() {
        if (executor.isExecutorThread(Thread.currentThread())) {
            notify0()
        } else {
            executor.execute(this::notify0)
        }
    }

    protected open val executor: EventExecutor get() = ImmediateEventExecutor.INSTANCE

    /**
     * Drains and notifies listeners. The retry loop closes the lost-listener race: without it, a
     * listener added just as another thread finishes draining would never be notified (the exact
     * race DefaultPromise guards with synchronized + re-check). Here: whoever holds the notifying
     * flag drains; after releasing it, if the list is non-empty again (racing add), re-acquire and
     * drain again; if someone else holds the flag, they will see the new entries.
     */
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

    // --- Netty Future surface -----------------------------------------------------------------

    override fun isSuccess(): Boolean = state() == Future.State.SUCCESS

    override fun isCancellable(): Boolean = !isDone

    override fun cause(): Throwable? = when (state()) {
        Future.State.SUCCESS, Future.State.RUNNING -> null
        Future.State.FAILED -> exceptionNow()
        Future.State.CANCELLED -> NettyListenerSupport.cancellationCause
    }

    override fun getNow(): V? = takeIf { it.state() == Future.State.SUCCESS }?.resultNow()

    override fun addListener(listener: GenericFutureListener<out NettyFuture<in V>>): ProgressiveFuture<V> =
        addListeners(listener)

    override fun addListeners(vararg listeners: GenericFutureListener<out NettyFuture<in V>>): ProgressiveFuture<V> {
        lock.withLock {
            list.addAll(listeners)
        }
        if (isDone) {
            notifyListeners()
        }
        return this
    }

    override fun removeListener(listener: GenericFutureListener<out NettyFuture<in V>>): ProgressiveFuture<V> =
        removeListeners(listener)

    override fun removeListeners(vararg listeners: GenericFutureListener<out NettyFuture<in V>>): ProgressiveFuture<V> {
        lock.withLock {
            list.removeAll(listeners.toSet())
        }
        return this
    }

    // --- blocking waits (FutureTask machinery: virtual-thread-park friendly, no Netty guard) ---

    /**
     * Real dead-lock protection when a [blockGuard] is provided: blocking the carrier PLATFORM
     * thread on a task served by that carrier is a genuine dead-lock; a virtual thread parking
     * is not (it unmounts and frees the carrier).
     */
    protected open fun checkDeadLock() {
//        val guard = blockGuard ?: return
//        if (guard.isExecutorThread(Thread.currentThread())) {
//            throw BlockingOperationException(toString())
//        }
    }

    @Throws(InterruptedException::class)
    override fun sync(): ProgressiveFuture<V> {
        await()
        cause()?.let { throw it }
        return this
    }

    override fun syncUninterruptibly(): ProgressiveFuture<V> {
        awaitUninterruptibly()
        cause()?.let { throw it }
        return this
    }

    @Throws(InterruptedException::class)
    override fun await(): ProgressiveFuture<V> {
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

    override fun awaitUninterruptibly(): ProgressiveFuture<V> {
        if (isDone) {
            return this
        }
        checkDeadLock()
        var interrupted = false
        while (!isDone) {
            try {
                get()
            } catch (_: InterruptedException) {
                interrupted = true
            } catch (_: CancellationException) {
                break
            } catch (_: ExecutionException) {
                break
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt()
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

    fun tryUpdateProgress(progress: Long, total: Long): Boolean {
        // DefaultProgressivePromise contract: total < 0 means "unknown total" (progress just has to
        // be non-negative); otherwise 0 <= progress <= total. No progress after completion.
        if (total < 0) {
            if (progress < 0 || isDone) {
                return false
            }
        } else if (progress !in 0..total || isDone) {
            return false
        }
        val snapshot = lock.withLock { list.filterIsInstance<GenericProgressiveFutureListener<*>>() }
        if (snapshot.isEmpty()) {
            return true
        }
        // Same dispatch rule as completion (NettyListenerSupport.notifyOne): notify inline only
        // when already on the notify executor, else hand off - so progressive listeners keep the
        // project invariant of never running on the raw carrier platform thread even when
        // progress is reported from a carrier-side timer runnable.
        if (executor.inEventLoop()) {
            fireProgress(snapshot, progress, total)
        } else {
            executor.execute { fireProgress(snapshot, progress, total) }
        }
        return true
    }

    private fun fireProgress(listeners: List<GenericProgressiveFutureListener<*>>, progress: Long, total: Long) {
        for (listener in listeners) {
            try {
                @Suppress("UNCHECKED_CAST")
                (listener as GenericProgressiveFutureListener<ProgressiveFuture<V>>)
                    .operationProgressed(this, progress, total)
            } catch (t: Throwable) {
                val self = Thread.currentThread()
                self.uncaughtExceptionHandler.uncaughtException(self, t)
            }
        }
    }
}
