package io.github.pbk20191.virtualloop

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.Test

/**
 * Pins the AutoCloseable contract of [VirtualIoEventLoop.close]. Without the explicit override,
 * the JDK 19+ ExecutorService.close() default routes through the deprecated shutdown() - i.e.
 * shutdownGracefully(0, 100ms) - and TRUNCATES in-flight work; and a self-close (from a loop
 * virtual thread or from the carrier) would wait for its own termination.
 */
class CloseContractTest {

    @Test
    fun closeWaitsForInFlightWorkInsteadOfTruncatingAt100ms() {
        check(PrivateLoomSupport.isSupported) { "Loom internals unavailable (opened-module and Unsafe strategies both failed)" }
        val group = VirtualIoEventLoopGroup(nThreads = 1)
        val loop = group.next()
        val completed = AtomicBoolean()
        loop.execute {
            Thread.sleep(300) // longer than the deprecated shutdown()'s 100ms hard timeout
            completed.set(true)
        }
        Thread.sleep(50) // let the task start
        loop.close() // must block until the task finished and the loop terminated
        println("in-flight task completed across close(): ${completed.get()}; terminated=${loop.isTerminated}")
        check(completed.get()) { "close() truncated an in-flight task (100ms shutdown path)" }
        check(loop.isTerminated) { "close() returned before termination" }
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS)
    }

    @Test
    fun selfCloseFromLoopThreadsDoesNotDeadlock() {
        check(PrivateLoomSupport.isSupported) { "Loom internals unavailable (opened-module and Unsafe strategies both failed)" }
        val group = VirtualIoEventLoopGroup(nThreads = 1)
        val loop = group.next() as VirtualIoEventLoop

        // From a loop VIRTUAL THREAD: close() must initiate shutdown and return, not await itself.
        val fromTask = loop.submit { loop.close() }
        check(fromTask.await(5, TimeUnit.SECONDS)) { "self-close from a loop virtual thread deadlocked" }
        check(loop.terminationFuture().await(10, TimeUnit.SECONDS)) { "loop did not terminate after self-close" }
        println("self-close from loop VT returned and the loop terminated")

        // From the CARRIER platform thread: close() must never block the loop it depends on.
        val group2 = VirtualIoEventLoopGroup(nThreads = 1)
        val loop2 = group2.next() as VirtualIoEventLoop
        val carrierReturned = AtomicBoolean()
        loop2.carrier.execute {
            loop2.close()
            carrierReturned.set(true)
        }
        check(loop2.terminationFuture().await(10, TimeUnit.SECONDS)) { "loop did not terminate after carrier-side close" }
        check(carrierReturned.get()) { "close() blocked the carrier thread" }
        println("carrier-side close returned without stalling the loop")
        group2.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS)
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS)
    }
}
