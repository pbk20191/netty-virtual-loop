package io.github.pbk20191.virtualloop

import java.util.concurrent.atomic.AtomicLong

/**
 * Cheap counters at the [VirtualIoEventLoop] scheduler choke point, for benchmarks and diagnosis:
 * how many virtual-thread continuations ran inline on the loop thread (the INLINE_NEXT fast path)
 * versus were queued through carrier.execute.
 */
object VirtualLoopStats {
    @JvmField
    val inlineContinuations = AtomicLong()

    @JvmField
    val queuedContinuations = AtomicLong()

    /** Continuations diverted by CONTINUATION_INTERCEPTOR (captured into a holder, run directly). */
    @JvmField
    val interceptedContinuations = AtomicLong()

    /** Continuations submitted by a virtual thread mounted on THIS loop's carrier (wakeup-free). */
    @JvmField
    val sameCarrierContinuations = AtomicLong()

    fun snapshot(): Triple<Long, Long, Long> =
        Triple(inlineContinuations.get(), queuedContinuations.get(), interceptedContinuations.get())
}
