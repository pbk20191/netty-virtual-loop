package io.github.pbk20191.virtualloop

import io.netty.channel.*
import io.netty.channel.nio.NioIoHandler
import io.netty.util.concurrent.EventExecutor
import io.netty.util.concurrent.EventExecutorChooserFactory
import io.netty.util.concurrent.EventExecutorChooserFactory.EventExecutorChooser
import io.netty.util.concurrent.Future
import io.netty.util.concurrent.MultithreadEventExecutorGroup
import java.util.concurrent.Executor
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * An [io.netty.channel.IoEventLoopGroup] whose children are [VirtualIoEventLoop]s, each wrapping
 * one child of a backing [MultiThreadIoEventLoopGroup] (the "carrier group") one-to-one. Child
 * selection reuses the carrier's own chooser via [ChooserFactory].
 *
 * Carrier ownership ([ownsCarrier]):
 * - **Consume** (`ownsCarrier = true`, the default): this group owns the carrier - shutting down
 *   the virtual group also shuts the carrier group down. This is always the mode when the carrier
 *   is created internally by the convenience constructors (nobody else holds a reference to it).
 * - **Guest** (`ownsCarrier = false`): this group is a guest service on a carrier that belongs to
 *   someone else (e.g. an application-wide event loop group). Shutting down the virtual group stops
 *   its own children/virtual threads but leaves the carrier untouched; the carrier's owner remains
 *   responsible for its lifecycle.
 *
 * @param carrier the backing group whose children carry the virtual threads.
 * @param ownsCarrier whether shutting this group down also shuts down [carrier] (see above).
 */
class VirtualIoEventLoopGroup(

    private val carrier: MultiThreadIoEventLoopGroup,
    private val ownsCarrier: Boolean = true,
    counter: AtomicInteger = AtomicInteger(0),
    buffer: MutableList<IoEventLoop> = arrayListOf()
) : MultithreadEventExecutorGroup(carrier.executorCount(), carrier, ChooserFactory(carrier), counter, buffer), IoEventLoopGroup {

    constructor(
        nThreads: Int = 0,
        executor: Executor? = null,
        ioHandlerFactory: IoHandlerFactory = NioIoHandler.newFactory(),
        chooserFactory: EventExecutorChooserFactory,
    ): this(MultiThreadIoEventLoopGroup(nThreads, executor, chooserFactory, ioHandlerFactory))

    constructor(
        nThreads: Int = 0,
        threadFactory: ThreadFactory? = null,
        ioHandlerFactory: IoHandlerFactory = NioIoHandler.newFactory(),
    ): this(MultiThreadIoEventLoopGroup(nThreads, threadFactory, ioHandlerFactory))

    private class ChooserFactory(
        val carrier: IoEventLoopGroup
    ): EventExecutorChooserFactory {

        override fun newChooser(vararg  executor: EventExecutor): EventExecutorChooser {
            // The children array is allocated as EventExecutor[] by MultithreadEventExecutorGroup,
            // so the ARRAY itself can never be cast to Array<VirtualIoEventLoop> (reified array
            // casts throw CCE); cast the elements instead.
            val buffer = mutableMapOf<IoEventLoop, VirtualIoEventLoop>()
            for (event in executor) {
                val loop = event as VirtualIoEventLoop
                buffer[loop.carrier] = loop
            }
            return Chooser(carrier, buffer.toMap())
        }

        private class Chooser(val carrier: IoEventLoopGroup, val map:Map<IoEventLoop, VirtualIoEventLoop>): EventExecutorChooser {
            override fun next(): EventExecutor {
               return map[carrier.next()]!!
            }

        }

    }

    override fun newChild(executor: Executor?, vararg args: Any?): IoEventLoop {
        // Called from the SUPER constructor, before this class's `carrier` field is assigned - so
        // the field is still null here. The carrier is exactly the Executor we passed to super, so
        // use the parameter instead of the field.
        val carrier = executor as MultiThreadIoEventLoopGroup

        val counter = args[0] as AtomicInteger
        val buffer = args[1] as MutableList<IoEventLoop>

        val index = counter.getAndIncrement()
        if (index == 0) {
            buffer.addAll(carrier.iterator().asSequence().toList() as Collection<IoEventLoop>)
        }
        val single = buffer[index] as SingleThreadIoEventLoop
        val loop = VirtualIoEventLoop(this, single)
        return loop
    }

    override fun next(): IoEventLoop {
        return super.next() as IoEventLoop
    }

    override fun register(
        channel: Channel,
        promise: ChannelPromise
    ): ChannelFuture {
        return next().register(channel, promise)
    }

    // Ordering: children FIRST, carrier LAST. Everything the children need to wind down (their
    // shutdown watchers aside, the drain/teardown continuations and channel-close cascades all run
    // through child.execute) requires a live carrier - shutting the carrier first can silently drop
    // those continuations and leave termination futures incomplete forever. So in consume mode the
    // carrier is only shut once every child has actually terminated (group terminationFuture), and
    // it receives the caller's quietPeriod/timeout rather than hard-coded defaults.

    override fun shutdownGracefully(quietPeriod: Long, timeout: Long, unit: TimeUnit): Future<*> {
        val future = super.shutdownGracefully(quietPeriod, timeout, unit)
        if (ownsCarrier) {
            terminationFuture().addListener {
                carrier.shutdownGracefully(quietPeriod, timeout, unit)
            }
        }
        return future
    }


    override fun shutdown() {
        @Suppress("DEPRECATION")
        super.shutdown()
        if (ownsCarrier) {
            terminationFuture().addListener {
                @Suppress("DEPRECATION")
                carrier.shutdown()
            }
        }
    }



}
