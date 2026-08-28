package io.github.pbk20191.virtualloop

import kotlin.test.Test

import io.netty.bootstrap.Bootstrap
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioIoHandler
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.util.concurrent.AutoScalingEventExecutorChooserFactory
import java.net.InetSocketAddress
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

/**
 * Real loopback traffic over a VirtualIoEventLoopGroup: does an inbound handler run on a virtual
 * thread carried by the loop, and does a *blocking* handler echo correctly?
 */
class EchoTest {
    @Test
    fun blockingHandlerEchoesOverRealSocket() {
        check(PrivateLoomSupport.isSupported) { "run with --add-opens=java.base/java.lang=ALL-UNNAMED" }

        val payload = "hello-from-loom-over-netty"
        val group = VirtualIoEventLoopGroup(nThreads = Runtime.getRuntime().availableProcessors())
        val handlerThread = AtomicReference<String>()
        val handlerVirtual = AtomicReference<Boolean>(false)
        var clientCloseFuture: io.netty.channel.ChannelFuture? = null

        try {
            val server = ServerBootstrap()
                .group(group)
                .channel(NioServerSocketChannel::class.java)
                .childHandler(object : ChannelInitializer<SocketChannel>() {
                    override fun initChannel(ch: SocketChannel) {
                        ch.pipeline().addLast(object : ChannelInboundHandlerAdapter() {
                            override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
                                val t = Thread.currentThread()
                                handlerThread.set("${t.name}  carrier=${PrivateLoomSupport.carrierOf(t)?.name}")
                                handlerVirtual.set(t.isVirtual)
                                // FastThreadLocal bridge: the drain VT is wrapped in
                                // runWithFastThreadLocal, so Netty treats it as FTL-capable and
                                // the Recycler keeps a REAL local pool here (objects come back).
                                check(
                                    io.netty.util.concurrent.FastThreadLocalThread
                                        .currentThreadHasFastThreadLocal(),
                                ) { "drain VT should be FastThreadLocal-bridged" }
                                val seen = HashSet<Int>()
                                var recycled = false
                                repeat(32) {
                                    val o = TestRecycler.RECYCLER.get()
                                    if (!seen.add(System.identityHashCode(o))) recycled = true
                                    o.recycle()
                                }
                                check(recycled) { "Recycler did not recycle on the drain VT" }
                                Thread.sleep(200) // blocking handler: must park the VT, not pin the carrier
                                ctx.writeAndFlush(msg) // echo
                            }
                        })
                    }
                })
            val serverChannel = server.bind(InetSocketAddress("127.0.0.1", 0)).sync().channel()
            val port = (serverChannel.localAddress() as InetSocketAddress).port

            // client: accumulate echoed bytes until we've seen the whole payload back
            val echoed = StringBuilder()
            val done = CountDownLatch(1)
            val client = Bootstrap()
                .group(group)
                .channel(NioSocketChannel::class.java)
                .handler(object : ChannelInitializer<SocketChannel>() {
                    override fun initChannel(ch: SocketChannel) {
                        ch.pipeline().addLast(object : ChannelInboundHandlerAdapter() {
                            override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
                                val buf = msg as ByteBuf
                                synchronized(echoed) {
                                    echoed.append(buf.toString(Charsets.UTF_8))
                                    if (echoed.length >= payload.length) done.countDown()
                                }
                                buf.release()
                            }
                        })
                    }
                })
            val clientChannel = client.connect(serverChannel.localAddress()).sync().channel()
            clientCloseFuture = clientChannel.closeFuture()

            val start = System.nanoTime()
            clientChannel.writeAndFlush(Unpooled.copiedBuffer(payload, Charsets.UTF_8))
            check(done.await(10, TimeUnit.SECONDS)) { "did not receive echo; got='$echoed'" }
            val elapsedMs = (System.nanoTime() - start) / 1_000_000

            println("inbound handler thread : ${handlerThread.get()}")
            println("inbound handler virtual: ${handlerVirtual.get()}")
            println("payload sent           : '$payload'")
            println("payload echoed back    : '$echoed'  (in ${elapsedMs}ms, incl. 200ms blocking handler)")
            check(handlerVirtual.get()) { "inbound handler did NOT run on a virtual thread" }
            check(echoed.toString() == payload) { "echo corrupted: '$echoed'" }
            println("RESULT: inbound IO dispatched on a virtual thread; blocking handler echoed correctly over a real socket.")
        } finally {
            val terminated = group.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS)
            println("group terminationFuture completed within 5s: $terminated")
            // Proves shutdown-time close() reached the channel: if the loop drops the close job
            // (the v2 post-cancel drain bug), this future never completes.
            val closed = clientCloseFuture?.await(5, TimeUnit.SECONDS) ?: false
            println("client closeFuture completed after shutdown: $closed")
        }
    }
}

/** Minimal recyclable object for verifying Recycler behaviour on loop threads. */
internal object TestRecycler {
    internal class Pooled(private val handle: io.netty.util.Recycler.Handle<Pooled>) {
        fun recycle() = handle.recycle(this)
    }

    val RECYCLER = object : io.netty.util.Recycler<Pooled>() {
        override fun newObject(handle: Handle<Pooled>): Pooled = Pooled(handle)
    }
}
