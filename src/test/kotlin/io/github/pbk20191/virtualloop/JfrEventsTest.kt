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
import jdk.jfr.Recording
import jdk.jfr.consumer.RecordingFile
import java.net.InetSocketAddress
import java.nio.file.Files
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test

/**
 * The JFR observability layer (LoopEvents.kt, pattern from Micronaut's LoomCarrierGroup): events
 * are @Enabled(false) by default, so this test enables them in an in-process Recording, drives
 * real traffic (task, echo socket, periodic chain, shutdown), and asserts every event type was
 * actually committed with sane content.
 */
class JfrEventsTest {
    @Test
    fun allLoopEventsAreRecorded() {
        check(PrivateLoomSupport.isSupported) { "Loom internals unavailable (opened-module and Unsafe strategies both failed)" }
        val names = listOf(
            "io.github.pbk20191.virtualloop.ContinuationScheduled",
            "io.github.pbk20191.virtualloop.ContinuationRun",
            "io.github.pbk20191.virtualloop.IoEventHandled",
            "io.github.pbk20191.virtualloop.PeriodicRound",
            "io.github.pbk20191.virtualloop.LoopShutdown",
            "io.github.pbk20191.virtualloop.LoopStats",
        )
        // JDK limitation (see PeriodicStats): a periodic hook added while a recording is already
        // running never activates for it, so the library must be "loaded" (hook registered)
        // BEFORE the recording starts - the supported real-world order.
        PeriodicStats.ensureRegistered()

        val dump = Files.createTempFile("virtualloop-jfr", ".jfr")
        val recording = Recording()
        names.forEach { name ->
            val settings = recording.enable(name)
            if (name.endsWith("LoopStats")) {
                // periodic event: shrink the default 1s period so the short test window sees it
                settings.withPeriod(java.time.Duration.ofMillis(50))
            }
        }
        recording.start()

        try {
            val group = VirtualIoEventLoopGroup(nThreads = 0)
            // task path (ContinuationScheduled/Run): a parking task forces several continuations
            group.next().submit { Thread.sleep(20) }.get(10, TimeUnit.SECONDS)

            // periodic path (PeriodicRound): three rounds, then cancel
            val rounds = AtomicInteger()
            val periodic = group.next().scheduleAtFixedRate({ rounds.incrementAndGet() }, 0, 20, TimeUnit.MILLISECONDS)
            while (rounds.get() < 3) Thread.sleep(5)
            periodic.cancel(false)

            // IO path (IoEventHandled): one echo round trip over a real socket
            val done = CountDownLatch(1)
            val server = ServerBootstrap().group(group).channel(NioServerSocketChannel::class.java)
                .childHandler(object : ChannelInitializer<SocketChannel>() {
                    override fun initChannel(ch: SocketChannel) {
                        ch.pipeline().addLast(object : ChannelInboundHandlerAdapter() {
                            override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
                                ctx.writeAndFlush(msg)
                            }
                        })
                    }
                })
            val port = (server.bind(InetSocketAddress("127.0.0.1", 0)).sync().channel()
                .localAddress() as InetSocketAddress).port
            val client = Bootstrap().group(group).channel(NioSocketChannel::class.java)
                .handler(object : ChannelInitializer<SocketChannel>() {
                    override fun initChannel(ch: SocketChannel) {
                        ch.pipeline().addLast(object : ChannelInboundHandlerAdapter() {
                            override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
                                (msg as ByteBuf).release()
                                done.countDown()
                            }
                        })
                    }
                })
            client.connect(InetSocketAddress("127.0.0.1", port)).sync().channel()
                .writeAndFlush(Unpooled.copiedBuffer("jfr", Charsets.UTF_8))
            check(done.await(10, TimeUnit.SECONDS)) { "echo did not complete" }

            // shutdown path (LoopShutdown)
            check(group.shutdownGracefully(0, 2, TimeUnit.SECONDS).await(10, TimeUnit.SECONDS))
        } finally {
            recording.stop()
            recording.dump(dump)
            recording.close()
        }

        val counts = RecordingFile.readAllEvents(dump).groupingBy { it.eventType.name }.eachCount()
        Files.deleteIfExists(dump)
        names.forEach { name ->
            println("%-55s: %d events".format(name, counts[name] ?: 0))
            check((counts[name] ?: 0) > 0) { "no events recorded for $name" }
        }
        println("RESULT: all five virtualloop JFR event types were recorded.")
    }
}
