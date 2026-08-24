package io.github.pbk20191.virtualloop.bench

import io.github.pbk20191.virtualloop.PrivateLoomSupport
import io.github.pbk20191.virtualloop.VirtualIoEventLoopGroup
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
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

/**
 * Loopback ping-pong latency bench: client sends 4 bytes, server echoes, client waits for the echo,
 * repeat. Measures average round-trip time - dominated by the per-IO-event dispatch path
 * (selector -> handle() -> drain virtual thread -> handler), so it directly shows dispatch-hop
 * overhead. Non-blocking handlers throughout.
 */
object PingPongBench {
    @JvmStatic
    fun main(args: Array<String>) {
        check(PrivateLoomSupport.isSupported)
        val vanilla = args.contains("vanilla")
        println("group: ${if (vanilla) "vanilla MultiThreadIoEventLoopGroup" else "VirtualIoEventLoopGroup"}")
        val group: io.netty.channel.IoEventLoopGroup =
            if (vanilla) {
                io.netty.channel.MultiThreadIoEventLoopGroup(2, io.netty.channel.nio.NioIoHandler.newFactory())
            } else {
                VirtualIoEventLoopGroup(nThreads = 2)
            }
        try {
            val server = ServerBootstrap()
                .group(group)
                .channel(NioServerSocketChannel::class.java)
                .childHandler(object : ChannelInitializer<SocketChannel>() {
                    override fun initChannel(ch: SocketChannel) {
                        ch.pipeline().addLast(object : ChannelInboundHandlerAdapter() {
                            override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
                                ctx.writeAndFlush(msg) // echo, zero work
                            }
                        })
                    }
                })
            val serverChannel = server.bind(InetSocketAddress("127.0.0.1", 0)).sync().channel()
            val port = (serverChannel.localAddress() as InetSocketAddress).port

            val pongs = LinkedBlockingQueue<Unit>()
            val client = Bootstrap()
                .group(group)
                .channel(NioSocketChannel::class.java)
                .handler(object : ChannelInitializer<SocketChannel>() {
                    override fun initChannel(ch: SocketChannel) {
                        ch.pipeline().addLast(object : ChannelInboundHandlerAdapter() {
                            override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
                                (msg as ByteBuf).release()
                                pongs.add(Unit)
                            }
                        })
                    }
                })
            val clientChannel = client.connect(InetSocketAddress("127.0.0.1", port)).sync().channel()

            fun roundTrips(n: Int): Double {
                val t0 = System.nanoTime()
                repeat(n) {
                    clientChannel.writeAndFlush(Unpooled.wrappedBuffer(byteArrayOf(1, 2, 3, 4)))
                    check(pongs.poll(5, TimeUnit.SECONDS) != null) { "no pong for ping $it" }
                }
                return (System.nanoTime() - t0).toDouble() / n / 1000.0
            }

            roundTrips(2_000) // warmup
            val (inline0, queued0, captured0) = io.github.pbk20191.virtualloop.VirtualLoopStats.snapshot()
            val us = roundTrips(5_000)
            val (inline1, queued1, captured1) = io.github.pbk20191.virtualloop.VirtualLoopStats.snapshot()
            println(String.format("ping-pong round trip: %.1f us/rtt (5000 rtts, echo server, no handler work)", us))
            println(
                "continuations during run: inline=${inline1 - inline0} queued=${queued1 - queued0} " +
                    "intercepted=${captured1 - captured0}",
            )
        } finally {
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(10, TimeUnit.SECONDS)
        }
    }
}
