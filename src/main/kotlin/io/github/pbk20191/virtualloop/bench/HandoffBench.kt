package io.github.pbk20191.virtualloop.bench

import io.github.pbk20191.virtualloop.PrivateLoomSupport
import io.github.pbk20191.virtualloop.VirtualIoEventLoopGroup
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit

/**
 * Same-carrier handoff micro-bench: a loop virtual thread spawns a child task and waits for its
 * completion. Each iteration exercises the same-carrier submission path twice (child start
 * continuation + our unpark continuation), so it measures exactly the scheduler path optimized for
 * same-carrier submitters (wakeup-free lazyExecute, optional yield handoff).
 */
object HandoffBench {
    @JvmStatic
    fun main(args: Array<String>) {
        check(PrivateLoomSupport.isSupported)
        val group = VirtualIoEventLoopGroup(nThreads = 1)
        try {
            val loop = group.next()
            val report = loop.submit(
                Callable {
                    repeat(20_000) { loop.submit(Callable { }).sync() } // warmup
                    val n = 100_000
                    val t0 = System.nanoTime()
                    repeat(n) { loop.submit(Callable { }).sync() }
                    val ns = (System.nanoTime() - t0).toDouble() / n
                    String.format("same-carrier handoff (spawn child + await): %.0f ns/op (%d ops)", ns, n)
                },
            ).sync().now
            println(report)
            val (inline, queued, intercepted) = io.github.pbk20191.virtualloop.VirtualLoopStats.snapshot()
            println("continuations total: inline=$inline queued=$queued intercepted=$intercepted")
        } finally {
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(10, TimeUnit.SECONDS)
        }
    }
}
