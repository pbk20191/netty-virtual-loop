package io.github.pbk20191.virtualloop

import java.lang.invoke.MethodHandle
import java.lang.invoke.VarHandle
import java.lang.reflect.Field
import java.util.concurrent.Executor
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.ForkJoinWorkerThread
import kotlin.reflect.KFunction

/**
 * Reflective access to the JDK's private virtual-thread internals.
 *
 * A virtual thread's "scheduler" is a [Executor]: whenever the thread must run or resume, the JDK
 * calls `scheduler.execute(continuation)`, and whichever platform thread runs that [Runnable]
 * becomes the virtual thread's carrier for that stretch. By setting the scheduler of a
 * [Thread.Builder.OfVirtual] to an executor that forwards to a Netty event loop, every virtual
 * thread created from that builder is carried by the event loop thread.
 *
 * All of this touches package-private / private members of `java.base/java.lang`, so the JVM must
 * be started with `--add-opens=java.base/java.lang=ALL-UNNAMED`. Ported from Micronaut's
 * `PrivateLoomSupport`, but using plain reflection instead of signature-polymorphic
 * `MethodHandle.invokeExact` (which Kotlin cannot express cleanly).
 */
object PrivateLoomSupport {

    private val failure: Throwable?

    /** `java.lang.ThreadBuilders$VirtualThreadBuilder.scheduler` — the per-builder scheduler. */
    private val builderSchedulerField: VarHandle?

    /** `java.lang.VirtualThread.scheduler` — the scheduler a running virtual thread belongs to. */
    private val threadSchedulerField: MethodHandle?

    /** `java.lang.VirtualThread.carrierThread` — the platform thread currently carrying the VT. */
    private val carrierThreadField: VarHandle?

    val defaultCarrierScheduler: ForkJoinPool?

    private val defaultCarrierFactory: ForkJoinPool.ForkJoinWorkerThreadFactory?

    init {
        var builderScheduler: VarHandle? = null
        var threadScheduler: MethodHandle? = null
        var carrierThread: VarHandle? = null
        var failed: Throwable? = null
        var defaultCarrierFactory: ForkJoinPool.ForkJoinWorkerThreadFactory? = null
        var defaultCarrierScheduler: ForkJoinPool? = null
        val virtualThread = Thread.ofVirtual().unstarted {  }.javaClass
        try {
            val lookup = LookupUnsafe.lookupIn(virtualThread)

            builderScheduler = Thread.ofVirtual().javaClass
                .getDeclaredField("scheduler") // getDeclaredField needs no opens; only setAccessible would
                .let { LookupUnsafe.lookupIn(Thread.ofVirtual().javaClass).unreflectVarHandle(it) }

            threadScheduler = virtualThread.getDeclaredField("scheduler").let { lookup.unreflectGetter(it) }
            carrierThread = virtualThread.getDeclaredField("carrierThread").let { lookup.unreflectVarHandle(it) }
            // getDeclaredField, NOT getField: DEFAULT_SCHEDULER is private static (getField only
            // sees public members and would throw, poisoning the whole init -> isSupported=false).
            defaultCarrierScheduler = virtualThread.getDeclaredMethod("defaultScheduler")
                .let(lookup::unreflect).let{
                    it.invoke() as? ForkJoinPool
                }
            defaultCarrierFactory = defaultCarrierScheduler?.factory
        } catch (t: Throwable) {
            failed = t
        }
        builderSchedulerField = builderScheduler
        threadSchedulerField = threadScheduler
        carrierThreadField = carrierThread
        failure = failed
        this.defaultCarrierFactory = defaultCarrierFactory
        this.defaultCarrierScheduler = defaultCarrierScheduler
    }

    /** Whether the loom internals could be accessed. False usually means the `--add-opens` flag is missing. */
    val isSupported: Boolean get() = failure == null

    private fun requireSupported() {
        if (!isSupported) {
            throw IllegalStateException(
                "Cannot access virtual-thread internals: both the opened-module path " +
                    "(--add-opens=java.base/java.lang=ALL-UNNAMED) and the sun.misc.Unsafe " +
                    "fallback failed (LookupUnsafe.strategy=${LookupUnsafe.strategy})",
                failure,
            )
        }
    }

    /** Sets the custom [scheduler] on a virtual-thread [builder] before its factory is created. */
    fun setScheduler(builder: Thread.Builder.OfVirtual, scheduler: Executor) {
        requireSupported()
        builderSchedulerField!!.set(builder, scheduler)
    }

    /** The scheduler the given (virtual) [thread] belongs to, or null if it is not a virtual thread. */
    fun schedulerOf(thread: Thread): Executor? {
        requireSupported()
        if (!thread.isVirtual) return null
        return threadSchedulerField!!.invoke(thread) as Executor?
    }

    /** The platform thread currently carrying the given virtual [thread], or null if unmounted. */
    fun carrierOf(thread: Thread): Thread? {
        requireSupported()

        if (!thread.isVirtual) return null
        return carrierThreadField!!.get(thread) as Thread?
    }
}
