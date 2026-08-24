package io.github.pbk20191.virtualloop

import io.netty.util.concurrent.DefaultPromise
import io.netty.util.concurrent.ImmediateEventExecutor
import java.util.concurrent.TimeUnit
import kotlin.test.Test

/**
 * Measures the disguise's TIER-2 cost: an `inEventLoop()` call whose caller class IS in the
 * DefaultPromise family, which forces the full [calledFromCheckDeadLock] stack walk (the tier-1
 * `getCallerClass` pre-filter cannot exit early there). This is the path DefaultPromise's
 * notifyListeners/checkDeadLock hit on every promise completion.
 */
class WalkBenchTest {

    /** Caller-class == DefaultPromise-family, so PROMISE_FAMILY passes and the full walk runs. */
    private class PromiseFamilyCaller(
        private val loop: VirtualIoEventLoop,
    ) : DefaultPromise<Unit>(ImmediateEventExecutor.INSTANCE) {
        fun callInEventLoop(): Boolean = loop.inEventLoop()
    }

    @Test
    fun measureDisguiseTiers() {
        check(PrivateLoomSupport.isSupported) { "run with --add-opens=java.base/java.lang=ALL-UNNAMED" }
        val group = VirtualIoEventLoopGroup(nThreads = 1)
        try {
            val loop = group.next() as VirtualIoEventLoop
            val familyCaller = PromiseFamilyCaller(loop)
            val iters = 300_000
            var acc = 0

            fun measure(label: String, op: () -> Boolean): Double {
                repeat(50_000) { if (op()) acc++ } // warmup
                val t0 = System.nanoTime()
                repeat(iters) { if (op()) acc++ }
                val perOp = (System.nanoTime() - t0).toDouble() / iters
                println("%-42s: %7.1f ns/op".format(label, perOp))
                return perOp
            }

            // The disguise tiers only run for threads that pass the membership check, so measure
            // ON a loop VT (where DefaultPromise would actually call them).
            loop.submit {
                measure("tier-0 membership-only inEventLoop(t)") { loop.inEventLoop(Thread.currentThread()) }
                measure("tier-1 getCallerClass (plain caller)") { loop.inEventLoop() }
                measure("tier-2 full walk (promise-family caller)") { familyCaller.callInEventLoop() }
                measure("calledFromCheckDeadLock() raw") { calledFromCheckDeadLock() }
                println("(acc=$acc)")
            }.sync()
        } finally {
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS)
        }
    }
}
