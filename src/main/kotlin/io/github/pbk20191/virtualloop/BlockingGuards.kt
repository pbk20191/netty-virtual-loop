package io.github.pbk20191.virtualloop

import io.netty.channel.*
import io.netty.util.concurrent.*
import io.netty.util.concurrent.Future
import io.netty.util.concurrent.ScheduledFuture
import java.util.ArrayDeque
import java.util.concurrent.*

// Carrier-guarded promise/task types: Netty's dead-lock guard re-aimed at the REAL hazard -
// blocking the carrier PLATFORM thread. Virtual threads just park (unmount, carrier freed).

// --- promises ---------------------------------------------------------------------------
// Promises created by this loop relax Netty's BlockingOperationException dead-lock guard. The
// guard exists because blocking the single event-loop thread on a future that only that thread
// can complete is a deadlock - but here the "event loop thread" a caller sits on is a VIRTUAL
// thread: awaiting parks it, the carrier is freed, and the completing code runs as a different
// continuation on that same carrier. So sync()/await() inside a task/handler is safe for every
// future this loop creates (submit/schedule/register results). Note: promises Netty creates
// internally (e.g. channel write futures) are not ours to construct and keep their guard.
class BlockingPromise<V>(override val executor: EventExecutor, private val carrier: ThreadAwareExecutor): PromiseLikeCompletableFuture<V>() {
    override fun defaultExecutor(): Executor = executor

    override fun checkDeadLock() {
        if (carrier.isExecutorThread(Thread.currentThread())) {
            throw BlockingOperationException(toString())

        }
    }

}

open class BlockingTask<V>(
    callable: Callable<V>,
    override val executor: EventExecutor, private val carrier: ThreadAwareExecutor

    ): RunnableNettyTask<V>(callable) {

    override fun checkDeadLock() {
        if (carrier.isExecutorThread(Thread.currentThread())) {
            throw BlockingOperationException(toString())
        }
    }

}

class BlockingChannelPromise(channel: Channel, executor: EventExecutor,  private val carrier: ThreadAwareExecutor): DefaultChannelPromise(channel, executor) {

    override fun checkDeadLock() {
        if (carrier.isExecutorThread(Thread.currentThread()) && channel().isRegistered) {
            throw BlockingOperationException(toString())
        }
    }

}
