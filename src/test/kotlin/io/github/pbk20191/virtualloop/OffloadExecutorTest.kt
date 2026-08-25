package io.github.pbk20191.virtualloop

import io.netty.bootstrap.Bootstrap
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelInitializer
import io.netty.channel.MultiThreadIoEventLoopGroup
import io.netty.channel.nio.NioIoHandler
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import java.net.InetSocketAddress
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test

/**
 * The minimal-invasion path: a 100% VANILLA Netty group, with only the blocking handler moved to
 * a [VirtualEventExecutorGroup] via `pipeline.addLast(group, handler)`. Verifies:
 *  - the handler runs on a virtual thread CARRIED BY the channel's own event loop thread
 *  - blocking in the handler does not stall the loop (a second channel echoes while the first
 *    handler sleeps)
 *  - Netty's offload ordering contract holds (serial, never-overlapping, in order)
 *  - unmodified writeAndFlush().sync() works in the handler (lane promises need the
 *    caller-sensitive inEventLoop answers - see VirtualEventExecutor.inEventLoop)
 *  - ctx.executor().schedule fires on the drain (inEventLoop true)
 *  - graceful shutdown terminates the lanes
 */
class OffloadExecutorTest {
    @Test
    fun blockingHandlerOnVanillaLoopViaOffloadGroup() {
        check(PrivateLoomSupport.isSupported) { "run with --add-opens=java.base/java.lang=ALL-UNNAMED" }

        val vanilla = MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory())
        val offload = VirtualEventExecutorGroup()
        val handlerReport = AtomicReference<String>()
        val maxInFlight = AtomicInteger()
        val inFlight = AtomicInteger()
        val order = StringBuilder()
        val scheduledOnDrain = AtomicReference<Boolean>()

        try {
            val payloadCount = 3
            val received = CountDownLatch(payloadCount)
            val server = ServerBootstrap()
                .group(vanilla)
                .channel(NioServerSocketChannel::class.java)
                .childHandler(object : ChannelInitializer<SocketChannel>() {
                    override fun initChannel(ch: SocketChannel) {
                        ch.pipeline().addLast(offload, object : ChannelInboundHandlerAdapter() {
                            override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
                                val text = (msg as ByteBuf).toString(Charsets.UTF_8)
                                msg.release()
                                // Client B is a DIFFERENT channel = a different offload lane, so
                                // it legitimately runs concurrently with A's lane - keep it out
                                // of A's serialization accounting.
                                val isA = text != "B"
                                if (isA) {
                                    val now = inFlight.incrementAndGet()
                                    maxInFlight.accumulateAndGet(now) { a, b -> maxOf(a, b) }
                                }
                                val t = Thread.currentThread()
                                val carrier = PrivateLoomSupport.carrierOf(t)
                                handlerReport.compareAndSet(
                                    null,
                                    "virtual=${t.isVirtual} carrier=${carrier?.name} loopThread=${vanilla.next().inEventLoop(carrier)}",
                                )
                                Thread.sleep(120) // BLOCKING on purpose - must not stall the vanilla loop
                                if (isA) synchronized(order) { order.append(text) }
                                // unmodified sync() on the write future (a LANE promise: Netty
                                // 4.2 promise plumbing handled by the caller-sensitive inEventLoop)
                                ctx.writeAndFlush(Unpooled.copiedBuffer(text, Charsets.UTF_8)).sync()
                                // executor timer contract: callback runs ON this executor
                                val ex = ctx.executor()
                                ex.schedule({ scheduledOnDrain.compareAndSet(null, ex.inEventLoop()) }, 10, TimeUnit.MILLISECONDS)
                                if (isA) {
                                    inFlight.decrementAndGet()
                                    // TCP may coalesce A's three 1-byte writes into one read:
                                    // count BYTES, not reads.
                                    repeat(text.length) { received.countDown() }
                                }
                            }
                        })
                    }
                })
            val port = (server.bind(InetSocketAddress("127.0.0.1", 0)).sync().channel()
                .localAddress() as InetSocketAddress).port

            // Client A: sends 3 payloads back-to-back into the BLOCKING handler
            val echoedA = StringBuilder()
            val doneA = CountDownLatch(1)
            val clientA = newClient(vanilla, port, echoedA, doneA, expect = 3)
            "abc".forEach { c ->
                clientA.writeAndFlush(Unpooled.copiedBuffer(c.toString(), Charsets.UTF_8))
            }

            // Client B: while A's handler sleeps, B must still echo promptly - proving the
            // vanilla loop thread is NOT blocked by A's sleeping handler.
            val echoedB = StringBuilder()
            val doneB = CountDownLatch(1)
            val clientB = newClient(vanilla, port, echoedB, doneB, expect = 1)
            val bStart = System.nanoTime()
            clientB.writeAndFlush(Unpooled.copiedBuffer("B", Charsets.UTF_8))
            check(doneB.await(10, TimeUnit.SECONDS)) { "client B echo missing" }
            val bMillis = (System.nanoTime() - bStart) / 1_000_000

            check(received.await(10, TimeUnit.SECONDS)) { "server did not receive all payloads" }
            check(doneA.await(10, TimeUnit.SECONDS)) { "client A echo missing; got='$echoedA'" }

            println("handler on offload lane : ${handlerReport.get()}")
            println("A order (server side)   : $order   maxInFlight=${maxInFlight.get()}")
            println("A echoed                : $echoedA")
            println("B echoed in ${bMillis}ms while A's handler was sleeping")
            Thread.sleep(50) // let the scheduled callback fire
            println("schedule() on drain     : ${scheduledOnDrain.get()}")

            check(handlerReport.get()!!.startsWith("virtual=true")) { "handler not on a virtual thread" }
            check(handlerReport.get()!!.contains("loopThread=true")) { "drain VT not carried by the vanilla loop thread" }
            check(order.toString() == "abc") { "offload ordering broken: '$order'" }
            check(maxInFlight.get() == 1) { "handler overlapped: maxInFlight=${maxInFlight.get()}" }
            check(echoedA.toString() == "abc") { "echo corrupted: '$echoedA'" }
            check(bMillis < 2_000) { "loop was stalled by the blocking handler (B took ${bMillis}ms)" }
            check(scheduledOnDrain.get() == true) { "scheduled callback not on the executor lane" }
            println("RESULT: vanilla loop + addLast(offloadGroup): blocking handler on loop-carried VT, ordering and sync() intact.")
        } finally {
            // IO group first: channel teardown events (channelInactive/handlerRemoved) still flow
            // through the live offload lanes; then the lanes drain and terminate.
            vanilla.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS)
            val offloadDone = offload.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS)
            println("offload group terminated within 5s: $offloadDone")
        }
    }

    private fun newClient(
        group: MultiThreadIoEventLoopGroup,
        port: Int,
        sink: StringBuilder,
        done: CountDownLatch,
        expect: Int,
    ): io.netty.channel.Channel {
        return Bootstrap()
            .group(group)
            .channel(NioSocketChannel::class.java)
            .handler(object : ChannelInitializer<SocketChannel>() {
                override fun initChannel(ch: SocketChannel) {
                    ch.pipeline().addLast(object : ChannelInboundHandlerAdapter() {
                        override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
                            val buf = msg as ByteBuf
                            synchronized(sink) {
                                sink.append(buf.toString(Charsets.UTF_8))
                                if (sink.length >= expect) done.countDown()
                            }
                            buf.release()
                        }
                    })
                }
            })
            .connect(InetSocketAddress("127.0.0.1", port)).sync().channel()
    }
}
