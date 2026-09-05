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
import java.net.InetSocketAddress
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test

/**
 * Loop-level ScopedValue binding: Loom does not inherit ScopedValues into freshly-created threads,
 * and this loop starts a fresh virtual thread per task and per IO event, so [bindScopedValue] makes
 * request-scoped context visible in both task VTs and IO drain VTs via `key.get()`.
 */
class ScopedValueBindingTest {
    @Test
    fun bindingIsVisibleInTaskAndIoVirtualThreads() {
        check(PrivateLoomSupport.isSupported) { "run with --add-opens or Unsafe fallback" }

        val tenant: ScopedValue<String> = ScopedValue.newInstance()
        val trace: ScopedValue<Int> = ScopedValue.newInstance()
        val group = VirtualIoEventLoopGroup(
            nThreads = 1,
            scopeCarrier = ScopedValue.where(tenant, "acme").where(trace, 42),
        )

        try {
            // 1. Task VT sees both bindings.
            val fromTask = group.next().submit(Callable {
                "${tenant.orElse("?")}/${if (trace.isBound) trace.get() else -1}"
            }).get(5, TimeUnit.SECONDS)
            println("task VT scope      : $fromTask")
            check(fromTask == "acme/42") { "task VT did not see the bound ScopedValues: $fromTask" }

            // 2. Binding survives a park (blocking task).
            val afterPark = group.next().submit(Callable {
                Thread.sleep(50)
                tenant.orElse("?")
            }).get(5, TimeUnit.SECONDS)
            check(afterPark == "acme") { "binding lost across a park: $afterPark" }

            // 3. IO drain VT (real socket, inbound handler) sees the binding too.
            val ioScope = AtomicReference<String>()
            val done = CountDownLatch(1)
            val server = ServerBootstrap().group(group).channel(NioServerSocketChannel::class.java)
                .childHandler(object : ChannelInitializer<SocketChannel>() {
                    override fun initChannel(ch: SocketChannel) {
                        ch.pipeline().addLast(object : ChannelInboundHandlerAdapter() {
                            override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
                                (msg as ByteBuf).release()
                                ioScope.set(tenant.orElse("?"))
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
            println("IO drain VT scope  : ${ioScope.get()}")
            check(ioScope.get() == "acme") { "IO drain VT did not see the bound ScopedValue: ${ioScope.get()}" }

            println("RESULT: bound ScopedValues visible in task VTs and IO drain VTs, and survive parks.")
        } finally {
            group.shutdownGracefully(0, 2, TimeUnit.SECONDS).await(10, TimeUnit.SECONDS)
        }
    }
}
