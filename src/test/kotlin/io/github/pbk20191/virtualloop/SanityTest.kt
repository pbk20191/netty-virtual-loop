package io.github.pbk20191.virtualloop

import kotlin.test.Test

import io.netty.channel.IoHandle
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Sanity demo for [VirtualIoEventLoopGroup].
 *
 * Run with: `./gradlew runMain`
 * (the `runMain` task passes `--add-opens=java.base/java.lang=ALL-UNNAMED`).
 *
 * It submits a few tasks to one virtual loop and prints, for each, whether it ran on a virtual
 * thread, which platform thread carried it, and whether `inEventLoop()` holds. All tasks should
 * report the SAME carrier (the backing NIO loop thread) but DISTINCT virtual threads.
 */
class SanityTest {
    @Test
    fun tasksRunOnFreshVirtualThreadsCarriedByTheLoop() {
        check(PrivateLoomSupport.isSupported) {
            "Loom internals not accessible - run with --add-opens=java.base/java.lang=ALL-UNNAMED"
        }
        val group = VirtualIoEventLoopGroup(nThreads = 0)
        try {
            val loop = group.next()
            val latch = CountDownLatch(3)
            repeat(3) { i ->
                loop.execute {
                    val self = Thread.currentThread()
                    val carrier = PrivateLoomSupport.carrierOf(self)
                    println(
                        "task #$i | virtual=${self.isVirtual} | thread=$self | " +
                            "carrier=${carrier?.name} | inEventLoop=${loop.inEventLoop()}",
                    )
                    latch.countDown()
                }
            }
            check(latch.await(5, TimeUnit.SECONDS)) { "tasks did not complete in time" }

            // next() should hand out distinct loops backed by distinct carriers.
            val a = group.next()
            val b = group.next()
            val carriers = java.util.concurrent.ConcurrentHashMap<String, Boolean>()
            val loopLatch = CountDownLatch(2)
            listOf(a, b).forEach { l ->
                l.execute {
                    carriers[PrivateLoomSupport.carrierOf(Thread.currentThread())!!.name] = true
                    loopLatch.countDown()
                }
            }
            check(loopLatch.await(5, TimeUnit.SECONDS))
            println("distinct carriers across 2 loops: ${carriers.keys.sorted()}")

            // A parking task must not stall other work on the same carrier: task P sleeps (VT parks,
            // unmounting the carrier), task Q must still run and finish first.
            val order = java.util.concurrent.CopyOnWriteArrayList<String>()
            val parkLatch = CountDownLatch(2)
            loop.execute {
                Thread.sleep(300)   // parks the virtual thread; carrier is freed
                order.add("slow")
                parkLatch.countDown()
            }
            loop.execute {
                order.add("fast")
                parkLatch.countDown()
            }
            check(parkLatch.await(5, TimeUnit.SECONDS))
            println("execution order (fast should precede slow): $order")

            // A scheduled task must also run on a virtual thread carried by the loop (exercises the
            // inlineSignal fast-path in VirtualThreadScheduler).
            val scheduled = loop.schedule(
                java.util.concurrent.Callable {
                    val self = Thread.currentThread()
                    "scheduled | virtual=${self.isVirtual} | carrier=${PrivateLoomSupport.carrierOf(self)?.name}"
                },
                100, TimeUnit.MILLISECONDS,
            )
            println(scheduled.get(5, TimeUnit.SECONDS))
        } finally {
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS)
        }
    }
}
