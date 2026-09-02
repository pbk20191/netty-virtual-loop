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
import io.netty.handler.ssl.SslContextBuilder
import io.netty.handler.ssl.SslHandler
import io.netty.pkitesting.CertificateBuilder
import java.net.InetSocketAddress
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test

/**
 * TLS through the virtual loop: SslHandler is the heaviest "real Netty" consumer of an event
 * executor - it schedules handshake timeouts through ctx.executor().schedule() (our timer),
 * completes handshake promises on it, and interleaves wrapped/unwrapped buffers with pooled
 * allocations. This test proves a TLS handshake + echo works when both ends run on
 * VirtualIoEventLoopGroup and the server-side application handler BLOCKS for 150ms per message.
 */
class TlsEchoTest {
    @Test
    fun tlsHandshakeAndEchoThroughBlockingHandler() {
        check(PrivateLoomSupport.isSupported) { "Loom internals unavailable (opened-module and Unsafe strategies both failed)" }

        val cert = CertificateBuilder()
            .subject("CN=localhost")
            .setIsCertificateAuthority(true)
            .buildSelfSigned()
        val serverSsl = SslContextBuilder
            .forServer(cert.keyPair.private, *cert.certificatePath)
            .build()
        val clientSsl = SslContextBuilder
            .forClient()
            .trustManager(cert.toTrustManagerFactory())
            .build()

        val payload = "tls-echo-over-virtual-loop"
        val group = VirtualIoEventLoopGroup(nThreads = 0)
        val handlerVirtual = AtomicReference(false)
        val handlerThread = AtomicReference<String>()
        var clientCloseFuture: io.netty.channel.ChannelFuture? = null

        try {
            val server = ServerBootstrap()
                .group(group)
                .channel(NioServerSocketChannel::class.java)
                .childHandler(object : ChannelInitializer<SocketChannel>() {
                    override fun initChannel(ch: SocketChannel) {
                        ch.pipeline().addLast(serverSsl.newHandler(ch.alloc()))
                        ch.pipeline().addLast(object : ChannelInboundHandlerAdapter() {
                            override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
                                val t = Thread.currentThread()
                                handlerVirtual.set(t.isVirtual)
                                handlerThread.set("${t.name}  carrier=${PrivateLoomSupport.carrierOf(t)?.name}")
                                Thread.sleep(150) // blocking behind TLS: must park the VT, not the loop
                                ctx.writeAndFlush(msg)
                            }
                        })
                    }
                })
            val serverChannel = server.bind(InetSocketAddress("127.0.0.1", 0)).sync().channel()
            val port = (serverChannel.localAddress() as InetSocketAddress).port

            val echoed = StringBuilder()
            val done = CountDownLatch(1)
            val client = Bootstrap()
                .group(group)
                .channel(NioSocketChannel::class.java)
                .handler(object : ChannelInitializer<SocketChannel>() {
                    override fun initChannel(ch: SocketChannel) {
                        ch.pipeline().addLast(clientSsl.newHandler(ch.alloc(), "localhost", port))
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
            val clientChannel = client.connect(InetSocketAddress("127.0.0.1", port)).sync().channel()
            clientCloseFuture = clientChannel.closeFuture()

            // The handshake promise completes on our loop; awaiting from the test (non-loop)
            // thread is guard-free.
            val sslHandler = clientChannel.pipeline().get(SslHandler::class.java)
            check(sslHandler.handshakeFuture().await(10, TimeUnit.SECONDS)) { "handshake timed out" }
            sslHandler.handshakeFuture().sync()
            val protocol = sslHandler.engine().session.protocol

            clientChannel.writeAndFlush(Unpooled.copiedBuffer(payload, Charsets.UTF_8))
            check(done.await(10, TimeUnit.SECONDS)) { "did not receive TLS echo; got='$echoed'" }

            println("negotiated protocol      : $protocol")
            println("server handler thread    : ${handlerThread.get()}")
            println("server handler virtual   : ${handlerVirtual.get()}")
            println("payload echoed over TLS  : '$echoed'")
            check(protocol.startsWith("TLS")) { "unexpected protocol: $protocol" }
            check(handlerVirtual.get()) { "server handler did NOT run on a virtual thread" }
            check(echoed.toString() == payload) { "echo corrupted: '$echoed'" }
            println("RESULT: TLS handshake, 150ms-blocking handler and echo all worked on the virtual loop.")
        } finally {
            val terminated = group.shutdownGracefully(0, 2, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS)
            println("group terminationFuture completed within 5s: $terminated")
            val closed = clientCloseFuture?.await(5, TimeUnit.SECONDS) ?: false
            println("client closeFuture completed after shutdown: $closed")
        }
    }
}
