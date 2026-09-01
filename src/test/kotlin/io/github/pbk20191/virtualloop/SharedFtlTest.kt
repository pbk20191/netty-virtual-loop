package io.github.pbk20191.virtualloop

import io.netty.bootstrap.Bootstrap
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.util.concurrent.FastThreadLocal
import io.netty.util.internal.InternalThreadLocalMap
import io.netty.util.internal.ThreadExecutorMap
import java.net.InetSocketAddress
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test

/**
 * Loop-level FastThreadLocal sharing (opt-in): every virtual thread of one VirtualIoEventLoop -
 * task VTs and drain VTs alike - shares ONE InternalThreadLocalMap, so FastThreadLocal state,
 * ThreadExecutorMap.currentExecutor and (through Netty's executor gates) the allocator caches
 * become loop-scoped instead of dying with each thread. Cleaned exactly once at termination.
 */
class SharedFtlTest {
    @Test
    fun oneMapPerLoopSharedAcrossTasksAndDrains() {
        check(PrivateLoomSupport.isSupported) { "run with --add-opens=java.base/java.lang=ALL-UNNAMED" }
        check(SharedFtlSupport.isSupported) { "shared-FTL reflection unavailable" }

        val group = VirtualIoEventLoopGroup(nThreads = 1, sharedFastThreadLocals = true)
        val loop = group.next()
        val removed = AtomicBoolean()
        val ftl = object : FastThreadLocal<String>() {
            override fun onRemoval(value: String) {
                removed.set(true)
            }
        }

        try {
            // 1. One map, many virtual threads.
            val map1 = loop.submit(Callable { System.identityHashCode(InternalThreadLocalMap.get()) }).get()
            val map2 = loop.submit(Callable { System.identityHashCode(InternalThreadLocalMap.get()) }).get()
            println("task-VT map identity        : $map1 / $map2")
            check(map1 == map2) { "task virtual threads do not share the loop map" }

            // 2. The executor stamp: currentExecutor() == the loop, on every loop thread - this is
            // what opens PooledByteBufAllocator's and AdaptivePoolingAllocator's cache gates.
            val exec = loop.submit(Callable { ThreadExecutorMap.currentExecutor() === loop }).get()
            println("currentExecutor == loop     : $exec")
            check(exec) { "ThreadExecutorMap not stamped into the shared map" }

            // 3. FastThreadLocal state is loop-scoped: set in one task, visible in another.
            loop.submit { ftl.set("loop-scoped") }.get()
            val seen = loop.submit(Callable { ftl.get() }).get()
            println("FTL value across tasks      : $seen")
            check(seen == "loop-scoped") { "FastThreadLocal not shared across task virtual threads" }

            // 4. The DRAIN shares the same map, and its fallback registration keeps the Recycler
            // real (round-trip actually recycles) without runWithFastThreadLocal's removeAll.
            val drainMap = AtomicInteger()
            val drainRecycled = AtomicBoolean()
            val done = CountDownLatch(1)
            val server = ServerBootstrap().group(group).channel(NioServerSocketChannel::class.java)
                .childHandler(object : ChannelInitializer<SocketChannel>() {
                    override fun initChannel(ch: SocketChannel) {
                        ch.pipeline().addLast(object : ChannelInboundHandlerAdapter() {
                            override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
                                (msg as ByteBuf).release()
                                drainMap.set(System.identityHashCode(InternalThreadLocalMap.get()))
                                val seenObjs = HashSet<Int>()
                                repeat(32) {
                                    val o = TestRecycler.RECYCLER.get()
                                    if (!seenObjs.add(System.identityHashCode(o))) drainRecycled.set(true)
                                    o.recycle()
                                }
                                done.countDown()
                            }
                        })
                    }
                })
            val port = (server.bind(InetSocketAddress("127.0.0.1", 0)).sync().channel()
                .localAddress() as InetSocketAddress).port
            Bootstrap().group(group).channel(NioSocketChannel::class.java)
                .handler(object : ChannelInitializer<SocketChannel>() {
                    override fun initChannel(ch: SocketChannel) {}
                })
                .connect(InetSocketAddress("127.0.0.1", port)).sync().channel()
                .writeAndFlush(Unpooled.copiedBuffer("x", Charsets.UTF_8))
            check(done.await(10, TimeUnit.SECONDS)) { "server read missing" }
            println("drain map identity          : ${drainMap.get()} (tasks: $map1)")
            println("drain Recycler recycled     : ${drainRecycled.get()}")
            check(drainMap.get() == map1) { "drain does not share the loop map" }
            check(drainRecycled.get()) { "Recycler did not recycle on a shared-FTL drain" }
        } finally {
            group.shutdownGracefully(0, 2, TimeUnit.SECONDS).await(10, TimeUnit.SECONDS)
        }

        // 5. Termination cleanup: removeAll ran once under the shared map -> onRemoval hooks fired.
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3)
        while (!removed.get() && System.nanoTime() < deadline) Thread.sleep(20)
        println("onRemoval after termination : ${removed.get()}")
        check(removed.get()) { "shared map was not cleaned at loop termination" }
        println("RESULT: one InternalThreadLocalMap per loop - tasks, drains, executor stamp, recycler and cleanup verified.")
    }
}
