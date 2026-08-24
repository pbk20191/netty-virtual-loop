package io.github.pbk20191.virtualloop

import kotlin.test.Test

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
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Re-fire probe: a server handler that BLOCKS 300ms on each read. The client sends "AAAA", then 100ms
 * later (while the first handler is still parked) sends "BBBB". If the loop re-dispatches the channel
 * while the first read's handler is parked, the handler is re-entered concurrently on the same channel
 * (maxInFlight > 1) - the corruption hazard. If dispatch is serialized per channel, maxInFlight stays 1.
 */
class ReFireTest {
    @Test
    fun parkedHandlerIsNeverReenteredConcurrently() {
        check(PrivateLoomSupport.isSupported)
        val group = VirtualIoEventLoopGroup(nThreads = 0)
        val inFlight = AtomicInteger()
        val maxInFlight = AtomicInteger()
        val reads = ConcurrentLinkedQueue<String>()
        val bothRead = CountDownLatch(2)

        try {
            val server = ServerBootstrap()
                .group(group)
                .channel(NioServerSocketChannel::class.java)
                .childHandler(object : ChannelInitializer<SocketChannel>() {
                    override fun initChannel(ch: SocketChannel) {
                        ch.pipeline().addLast(object : ChannelInboundHandlerAdapter() {
                            override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
                                val depth = inFlight.incrementAndGet()
                                maxInFlight.accumulateAndGet(depth) { a, b -> maxOf(a, b) }
                                val buf = msg as ByteBuf
                                val content = buf.toString(Charsets.UTF_8)
                                reads.add("read='$content' depth=$depth vthread=${Thread.currentThread().name}")
                                Thread.sleep(300) // block while parked
                                buf.release()
                                inFlight.decrementAndGet()
                                bothRead.countDown()
                            }
                        })
                    }
                })
            val serverChannel = server.bind(InetSocketAddress("127.0.0.1", 0)).sync().channel()
            val port = (serverChannel.localAddress() as InetSocketAddress).port

            val client = Bootstrap()
                .group(group)
                .channel(NioSocketChannel::class.java)
                .handler(object : ChannelInitializer<SocketChannel>() {
                    override fun initChannel(ch: SocketChannel) {
                        ch.pipeline().addLast(object : ChannelInboundHandlerAdapter() {})
                    }
                })
            val clientChannel = client.connect(InetSocketAddress("127.0.0.1", port)).sync().channel()

            clientChannel.writeAndFlush(Unpooled.copiedBuffer("AAAA", Charsets.UTF_8))
            Thread.sleep(100) // let the first read's handler start and park
            clientChannel.writeAndFlush(Unpooled.copiedBuffer("BBBB", Charsets.UTF_8))

            check(bothRead.await(10, TimeUnit.SECONDS)) { "handlers did not both complete; reads=$reads" }
            reads.forEach(::println)
            println("maxInFlight (concurrent handlers on one channel): ${maxInFlight.get()}")
            check(maxInFlight.get() == 1) {
                "RE-FIRE: the channel was re-entered concurrently while its handler was parked " +
                    "(maxInFlight=${maxInFlight.get()})"
            }
            println("RESULT: no re-fire - dispatch stayed serialized per channel.")
        } finally {
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS)
        }
    }
}
