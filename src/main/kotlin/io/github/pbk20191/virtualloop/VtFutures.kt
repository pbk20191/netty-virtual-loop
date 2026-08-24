package io.github.pbk20191.virtualloop

import io.netty.channel.Channel
import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelPromise
import io.netty.channel.DefaultChannelPromise
import io.netty.util.concurrent.Future
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Virtual-thread-native blocking helpers for Netty futures.
 *
 * Netty's `await()`/`sync()` guard against dead-lock with `BlockingOperationException` when called
 * from the future's own event loop - correct for a platform event-loop thread, but overly strict on
 * a [VirtualIoEventLoop] task/handler virtual thread, where blocking parks the thread and frees the
 * carrier. Futures created BY the loop already relax the guard; futures Netty constructs internally
 * (channel write/connect/close futures) cannot be replaced - so these helpers wait WITHOUT invoking
 * the guard at all: a listener completes a latch, and the latch park is what suspends the caller.
 * Note the guard only fires on futures that are not yet done, e.g. a write pending under
 * backpressure - an already-completed future returns before the guard is even consulted.
 */

/** Waits for completion (interruptibly) without Netty's dead-lock guard. */
fun <V, F : Future<V>> F.awaitVt(): F {
    if (isDone) return this
    val done = CountDownLatch(1)
    addListener { done.countDown() }
    done.await()
    return this
}

/** Waits up to the given timeout without Netty's dead-lock guard; true if completed in time. */
fun <V> Future<V>.awaitVt(timeout: Long, unit: TimeUnit): Boolean {
    if (isDone) return true
    val done = CountDownLatch(1)
    addListener { done.countDown() }
    return done.await(timeout, unit)
}

/** [awaitVt], then rethrows the failure cause if the future failed (like `sync()`). */
fun <V, F : Future<V>> F.syncVt(): F {
    awaitVt()
    cause()?.let { throw it }
    return this
}

/** [syncVt], then returns the result (like a guard-free blocking `get()`). */
fun <V> Future<V>.getVt(): V {
    syncVt()
    return now
}

/**
 * A [ChannelPromise] whose dead-lock guard is relaxed, for handing into promise-accepting channel
 * operations (e.g. `channel.writeAndFlush(msg, channel.newVtPromise())`) so the returned future can
 * be `sync()`ed/`await()`ed directly from a task or handler virtual thread.
 */
fun Channel.newVtPromise(): ChannelPromise = object : DefaultChannelPromise(this) {
    override fun checkDeadLock() {
        // Awaiting on a virtual thread parks it and frees the carrier - no deadlock possible.
    }
}

/** `writeAndFlush` whose returned future is safe to `sync()`/`await()` from a virtual thread. */
fun Channel.writeAndFlushVt(msg: Any): ChannelFuture = writeAndFlush(msg, newVtPromise())
