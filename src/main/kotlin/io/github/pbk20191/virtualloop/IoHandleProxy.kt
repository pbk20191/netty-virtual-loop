package io.github.pbk20191.virtualloop

import io.netty.channel.*
import io.netty.util.concurrent.*
import io.netty.util.concurrent.Future
import io.netty.util.concurrent.ScheduledFuture
import java.lang.reflect.InvocationHandler
import java.lang.reflect.Method
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.*

// The dynamic-proxy IoHandle wrapper (serial per-registration dispatch onto the drain virtual
// thread), the registration wrapper that hooks cancel() as the drain's terminal signal, and the
// per-class proxy-interface cache.

internal class DelegatedHandle(
    val actual: IoHandle,
    /**
     * Must be a LOCK-FREE BlockingQueue (LinkedTransferQueue): with LinkedBlockingQueue the
     * drain thread parks under the queue's takeLock condition, so the inline wake-up mount (see
     * [handle]) fires while the enqueuing thread still holds takeLock - the drain thread mounts,
     * fails the lock, parks again, and only a second mount does work. LinkedTransferQueue's
     * take() parks with no lock held and add() unparks the waiter after CAS-linking the node,
     * so the inline mount succeeds on the first try - while keeping blocking take()/poll
     * semantics for the drain loop.
     */
    val taskQueue: BlockingQueue<Runnable>,

    val continuationHolder: AtomicReference<Runnable>
    ): InvocationHandler, IoHandle {

    // NOTE: the InvocationHandler contract passes args == null (not an empty array) for no-arg
    // methods like selectableChannel()/registered(), so the parameter must be a nullable array -
    // a `vararg args: Any?` declaration NPEs on Kotlin's intrinsic null-check before any dispatch.
    //
    // Dispatch is matched by name+arity rather than declaringClass: a transport sub-interface
    // that redeclares close()/handle() would report ITS class as declaringClass and silently
    // bypass the serial queue if we compared classes.
    override fun invoke(proxy: Any, method: Method, args: Array<out Any?>?): Any? {
        when (method.declaringClass) {
            Any::class.java -> return when (method.name) {
                // Standard proxy identity: routing these to `actual` would make equals
                // asymmetric and let the proxy masquerade as the raw handle in maps.
                "hashCode" -> System.identityHashCode(proxy)
                "equals" -> proxy === args!![0]
                else -> "DelegatedHandle($actual)"
            }

            IoHandle::class.java -> return method.invoke(this,*(args ?: EMPTY_ARGS))
        }
        when (method.name) {
//                IoHandle::registered.name -> if (method.parameterCount == 0) return registered()
//                IoHandle::unregistered.name -> if (method.parameterCount == 0) return unregistered()
            IoHandle::close.name -> if (method.parameterCount == 0) return close()
//                IoHandle::handle.name -> if (method.parameterCount == 2) {
//                    return handle(args!![0] as IoRegistration, args[1] as IoEvent)
//                }
        }
        return method.invoke(actual, *(args ?: EMPTY_ARGS))
    }

    private companion object {
        val EMPTY_ARGS = emptyArray<Any?>()

        /** No-op job: wakes the parked drain thread so it re-checks the terminal flags. */
        val WAKE = Runnable { }

    }

    /** Refreshes [continuationHolder] whenever the drain thread's unpark reaches the scheduler. */
    private val capture = Executor { continuationHolder.set(it) }

    /**
     * Pre-built binding carrier: the (scoped value, capture) pair is constant per handle, and
     * ScopedValue.Carrier is immutable and reusable - caching it removes one allocation from
     * every dispatched IO event.
     */
    private val captureScope = ScopedValue.where(CONTINUATION_INTERCEPTOR, capture)

    /**
     * Serializes [job] onto the drain virtual thread and runs it inline when possible.
     *
     * The interceptor is bound around the add: LinkedTransferQueue's internal unpark of the
     * parked drain thread fires scheduler.execute on THIS (platform) thread, and the capture
     * diverts that fresh continuation into the holder - refreshing it on every park cycle and
     * preventing a duplicate no-op task from landing on the carrier's queue. The .run() then
     * mounts the drain thread right here: runContinuation is state-CAS-guarded, so if the drain
     * thread was not parked this is a harmless no-op and the running drain picks the job up
     * from its queue instead.
     */
    private fun enqueueAndRun(job: Runnable) {
        captureScope.run {
            taskQueue.add(job)
        }
        continuationHolder.get()?.run()
    }

    override fun registered() {
        if (Thread.currentThread().isVirtual) {
            actual.registered()
        } else {
            enqueueAndRun { actual.registered() }
        }
    }

    // --- terminal lifecycle signals: they bound the drain virtual thread's lifetime ----------
    // The drain thread runs FOR: registration cancelled (unregistered) OR close received.
    //  - close() is a hard terminal: Netty delivers nothing after it, so the drain exits as
    //    soon as the queue is empty - no grace wait.
    //  - cancelled (unregistered) keeps a short grace window, because registration.close()
    //    produces "cancel -> unregistered -> close" in one cascade and the trailing close()
    //    job must still be caught.
    // Flags are set BEFORE the enqueue so the drain observes them together with the job. The
    // virtual-thread branches must also WAKE the parked drain (plain add; the unpark routes
    // through the queued scheduler path - a virtual thread cannot mount another one inline),
    // or it would idle in take() long after the terminal signal.

    /** Set once close() was delivered - nothing can follow it. */
    @Volatile
    var closed = false

    /** Set once unregistered() was delivered (registration cancelled). */
    @Volatile
    var cancelled = false

    /** Direct cancel signal from [DelegatedRegistration.cancel]: flag + wake the parked drain. */
    fun markCancelled() {
        cancelled = true
        taskQueue.add(WAKE)
    }

    override fun unregistered() {
        cancelled = true
        if (Thread.currentThread().isVirtual) {
            actual.unregistered()
            taskQueue.add(WAKE)
        } else {
            enqueueAndRun { actual.unregistered() }
        }
    }

    override fun close() {
        closed = true
        if (Thread.currentThread().isVirtual) {
            actual.close()
            taskQueue.add(WAKE)
        } else {
            enqueueAndRun { actual.close() }
        }
    }

    override fun handle(registration: IoRegistration, ioEvent: IoEvent) {
        if (Thread.currentThread().isVirtual) {
            actual.handle(registration, ioEvent)
        } else {
            enqueueAndRun { actual.handle(registration, ioEvent) }
        }
    }

}

/**
 * The registration handed back to registrants (channels store it and cancel through it on
 * deregister). Wrapping it does two things: [cancel] is the DIRECT terminal signal for the
 * drain thread's lifetime (flag + wake, no reliance on the unregistered() side-effect alone),
 * and the raw inner registration - a carrier-loop internal - is never exposed. Netty-internal
 * cancellation (prepareToDestroy closes the inner registration directly) still reaches the
 * drain via the unregistered()/close() callback flags.
 */
internal class DelegatedRegistration(
    private val inner: IoRegistration,
    private val handle: DelegatedHandle,
) : IoRegistration {
    override fun <T> attachment(): T = inner.attachment()
    override fun submit(ops: IoOps): Long = inner.submit(ops)
    override fun isValid(): Boolean = inner.isValid
    override fun cancel(): Boolean {
        val cancelled = inner.cancel()
        if (cancelled) {
            handle.markCancelled()
        }
        return cancelled
    }
}

internal class InterfaceCache: ClassValue<Array<Class<*>>>() {

    override fun computeValue(type: Class<*>): Array<Class<*>> {
        return generateSequence(type) { it.superclass }
            .flatMap { it.interfaces.asSequence() }
            .distinct()
            .filter { !it.isHidden && !it.isSealed && java.lang.reflect.Modifier.isPublic(it.modifiers) }
            .toList()
            .toTypedArray()
    }

}
