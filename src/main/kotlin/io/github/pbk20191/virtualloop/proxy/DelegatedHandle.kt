package io.github.pbk20191.virtualloop.proxy

import io.github.pbk20191.virtualloop.CONTINUATION_INTERCEPTOR
import io.github.pbk20191.virtualloop.IoEventHandled
import io.netty.channel.*
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
) : CanonicalInvocationHandler<IoHandle>(actual), IoHandle {

    // Lifecycle membership is decided by the canonical-method map: IoHandle's signatures resolved
    // against the incoming Method's own declaringClass - exact on parameter types (foreign
    // overloads never match), independent of WHERE the method was declared (redeclaring
    // sub-interface, Closeable-foremost view, covariant-return bridges), and the translation to
    // the canonical Method makes the reflective call on this wrapper legal. All counterexamples
    // pinned by IoHandleProxyDispatchTest; shared machinery in CanonicalInvocationHandler.kt.
    override val methodMap: AbstractInterfaceMethodMap<IoHandle> get() = IoHandleProxyMethodCache


    private companion object {
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

    /** Cached for the JFR event; avoids a per-event getClass().getSimpleName(). */
    private val handleTypeName: String = actual.javaClass.simpleName

    override fun handle(registration: IoRegistration, ioEvent: IoEvent) {
        if (Thread.currentThread().isVirtual) {
            if (IoEventHandled.INSTANCE.isEnabled) {
                dispatchRecorded(registration, ioEvent, direct = true)
            } else {
                actual.handle(registration, ioEvent)
            }
        } else {
            if (IoEventHandled.INSTANCE.isEnabled) {
                enqueueAndRun { dispatchRecorded(registration, ioEvent, direct = false) }
            } else {
                enqueueAndRun { actual.handle(registration, ioEvent) }
            }
        }
    }

    /** JFR-wrapped dispatch; the event is created ON the drain thread (begin/commit same thread). */
    private fun dispatchRecorded(registration: IoRegistration, ioEvent: IoEvent, direct: Boolean) {
        val ev = IoEventHandled()
        ev.handleType = handleTypeName
        ev.direct = direct
        ev.begin()
        try {
            actual.handle(registration, ioEvent)
        } finally {
            ev.end()
            ev.commit()
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
    internal data object IoHandleProxyMethodCache : AbstractInterfaceMethodMap<IoHandle>() {
        override val clazz: Class<IoHandle> = IoHandle::class.java
    }

}