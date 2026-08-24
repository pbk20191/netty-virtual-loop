package io.github.pbk20191.virtualloop.bench

import io.github.pbk20191.virtualloop.PrivateLoomSupport
import io.github.pbk20191.virtualloop.VirtualIoEventLoopGroup
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit

/**
 * Micro-benchmark for the inEventLoop() StackWalker disguise overhead, run from a task virtual
 * thread (the hot caller in practice). Compares the membership-only inEventLoop(Thread) baseline
 * against the no-arg inEventLoop() that carries the disguise logic. Rough nanoTime measurement -
 * good for order of magnitude, not for publication.
 */
object DisguiseBench {
    @JvmStatic
    fun main(args: Array<String>) {
        check(PrivateLoomSupport.isSupported)
        val group = VirtualIoEventLoopGroup(nThreads = 0)
        try {
            val loop = group.next()
            val report = loop.submit(
                Callable {
                    val current = Thread.currentThread()
                    var acc = 0
                    repeat(500_000) { // warmup both paths
                        if (loop.inEventLoop(current)) acc++
                        if (loop.inEventLoop()) acc++
                    }
                    val n = 2_000_000
                    var t0 = System.nanoTime()
                    repeat(n) { if (loop.inEventLoop(current)) acc++ }
                    val memberNs = (System.nanoTime() - t0).toDouble() / n
                    t0 = System.nanoTime()
                    repeat(n) { if (loop.inEventLoop()) acc++ }
                    val noArgNs = (System.nanoTime() - t0).toDouble() / n
                    String.format(
                        "inEventLoop(thread) membership-only: %6.1f ns/op%n" +
                            "inEventLoop() with disguise logic  : %6.1f ns/op%n" +
                            "disguise overhead                  : %6.1f ns/op   (acc=%d)",
                        memberNs, noArgNs, noArgNs - memberNs, acc,
                    )
                },
            ).sync().now
            println(report)
        } finally {
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(10, TimeUnit.SECONDS)
        }
    }
}
