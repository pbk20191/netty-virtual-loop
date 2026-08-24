package io.github.pbk20191.virtualloop

import io.netty.util.concurrent.DefaultPromise
import io.netty.util.concurrent.EventExecutor
import io.netty.util.concurrent.Future
import io.netty.util.concurrent.GenericFutureListener
import io.netty.util.concurrent.ImmediateEventExecutor

/**
 * Shared bridge into Netty's protected-static listener machinery. Being a [io.netty.util.concurrent.DefaultPromise]
 * subclass is what grants access to [notifyListener]; the instance itself is a
 * permanently-cancelled promise whose [cause] doubles as the shared, stackless
 * [java.util.concurrent.CancellationException] that cancelled tasks report from their own `cause()`.
 * NOTE: `DefaultPromise(null)` throws (checkNotNull) - the executor must be a real one.
 */
internal object NettyListenerSupport : DefaultPromise<Unit>(ImmediateEventExecutor.INSTANCE) {
    init {
        cancel(false)
    }

    /** The shared CancellationException reported for cancelled tasks/promises. */
    val cancellationCause: Throwable = cause()!!

    fun notifyOne(executor: EventExecutor, future: Future<*>, listener: GenericFutureListener<*>) {
        notifyListener(executor, future, listener)
    }
}