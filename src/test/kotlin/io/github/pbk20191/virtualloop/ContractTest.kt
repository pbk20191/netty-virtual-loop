package io.github.pbk20191.virtualloop

import kotlin.test.Test

import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit

/**
 * Verifies the v2 executor contracts:
 *  1. invokeAll / invokeAny run every task on a virtual thread, from outside AND from inside a task.
 *  2. sync()/await() on loop-created futures works inside a task (no BlockingOperationException).
 *  3. Graceful-shutdown quiet period is activity-restarted, capped by the hard timeout.
 */
class ContractTest {
    @Test
    fun executorContractsPeriodicSemanticsAndQuietPeriod() {
        check(PrivateLoomSupport.isSupported)

        run {
            // Explicit carrier so case 2c can reach the raw carrier platform thread directly.
            val carrierGroup =
                io.netty.channel.MultiThreadIoEventLoopGroup(1, io.netty.channel.nio.NioIoHandler.newFactory())
            val group = VirtualIoEventLoopGroup(carrierGroup)
            try {
                val loop = group.next()

                // 1a. invokeAll from outside the loop: all tasks on VTs, results correct.
                val all = loop.invokeAll(
                    (1..3).map { i -> Callable { check(Thread.currentThread().isVirtual); i * 10 } },
                )
                check(all.map { it.get() } == listOf(10, 20, 30)) { "invokeAll results wrong: $all" }
                println("invokeAll (outside)  : all on virtual threads, results ${all.map { it.get() }} -> OK")

                // FastThreadLocal bridge boundary: SHORT-LIVED task virtual threads are
                // deliberately NOT bridged (runWithFastThreadLocal is documented for long-running
                // threads; per-task caches would churn) - only the long-lived drain VT is
                // (verified in EchoTest). Netty must see a plain thread here.
                val bridged = loop.submit(
                    Callable {
                        io.netty.util.concurrent.FastThreadLocalThread.currentThreadHasFastThreadLocal()
                    },
                ).sync().now
                check(bridged == false) { "short-lived task VTs must not be FTL-bridged" }
                println("FTL bridge boundary  : task VTs stay unbridged (drain VT bridged in EchoTest) -> OK")

                // 1b. invokeAny from outside: one failing, one slow, one fast - fast wins.
                val any = loop.invokeAny(
                    listOf(
                        Callable<String> { throw IllegalStateException("boom") },
                        Callable { Thread.sleep(2_000); "slow" },
                        Callable { Thread.sleep(50); "fast" },
                    ),
                )
                check(any == "fast") { "invokeAny picked '$any'" }
                println("invokeAny (outside)  : first success wins ('$any') -> OK")

                // 1c. cancel(true) INTERRUPTS the running task's dedicated virtual thread (java
                // FutureTask semantics via VtFutureTask - Netty promises can never interrupt a
                // worker). The parked sleep must abort immediately, not run its full 10s.
                val interrupted = java.util.concurrent.CountDownLatch(1)
                val slow = loop.submit(
                    Callable {
                        try {
                            Thread.sleep(10_000)
                        } catch (_: InterruptedException) {
                            interrupted.countDown()
                        }
                    },
                )
                Thread.sleep(100) // let the task start and park in sleep
                check(slow.cancel(true)) { "cancel(true) returned false" }
                check(interrupted.await(2, TimeUnit.SECONDS)) { "worker virtual thread was not interrupted" }
                check(slow.isCancelled) { "future not cancelled" }
                println("cancel(true)         : parked task's virtual thread interrupted -> OK")

                // 1c+2. From INSIDE a task: invokeAll and future.sync() must not throw
                // BlockingOperationException; the calling VT parks, tasks proceed.
                val nested = loop.submit(
                    Callable {
                        val inner = loop.invokeAll(
                            (1..2).map { i -> Callable { check(Thread.currentThread().isVirtual); i } },
                        )
                        val synced = loop.submit(Callable { "synced" }).sync().now
                        "inner=${inner.map { it.get() }} $synced"
                    },
                ).sync().now
                check(nested == "inner=[1, 2] synced") { "nested result: '$nested'" }
                println("invokeAll + sync (inside a task): '$nested' -> OK")

                // 2b. Guard-armed futures (exactly how Netty constructs internal channel promises):
                // the StackWalker disguise makes inEventLoop() answer false to checkDeadLock, so a
                // plain sync()/await() from a task VT no longer throws BlockingOperationException -
                // it parks, the completing task runs on the same carrier, and it resumes. getVt()
                // (guard-free by construction) must keep working too.
                val outcome = loop.submit(
                    Callable {
                        val guarded =
                            object : io.netty.util.concurrent.DefaultPromise<String>(loop) {}
                        loop.schedule({ guarded.setSuccess("released") }, 100, TimeUnit.MILLISECONDS)
                        val plain = guarded.sync().now
                        val viaVt = guarded.getVt()
                        "sync=$plain getVt=$viaVt"
                    },
                ).sync().now
                check(outcome == "sync=released getVt=released") { "guarded-future outcome: '$outcome'" }
                println("guarded future        : plain sync() parks instead of throwing (StackWalker disguise) -> OK")

                // 2c. On the raw carrier PLATFORM thread the guard must STILL fire: blocking the
                // carrier really would deadlock (it runs the completing continuations). Reach the
                // carrier thread directly through the carrier group and await a loop-executor promise.
                val guardOnCarrier = java.util.concurrent.CompletableFuture<Boolean>()
                carrierGroup.next().execute {
                    val pending = object : io.netty.util.concurrent.DefaultPromise<String>(loop) {}
                    guardOnCarrier.complete(
                        try {
                            pending.await(); false
                        } catch (_: io.netty.util.concurrent.BlockingOperationException) {
                            true
                        },
                    )
                }
                check(guardOnCarrier.get(5, TimeUnit.SECONDS)) {
                    "guard did NOT fire on the carrier platform thread"
                }
                println("guard on carrier      : BlockingOperationException still thrown on the platform thread -> OK")

                // 2c. Listeners on loop-returned futures must never run on the carrier platform
                // thread. Cover the three notification paths: listener added before completion
                // (notified by the completing thread), after completion (notified via execute),
                // and on a scheduled Runnable's future (previously the raw carrier timer future).
                val listenerThreads = java.util.concurrent.ConcurrentLinkedQueue<String>()
                val listened = java.util.concurrent.CountDownLatch(3)
                fun record(label: String) {
                    val t = Thread.currentThread()
                    listenerThreads.add("$label: virtual=${t.isVirtual} thread=${t.name}")
                    if (!t.isVirtual) error("listener '$label' ran on platform thread ${t.name}")
                    listened.countDown()
                }
                loop.submit(Callable { Thread.sleep(50) })
                    .addListener { record("submit(before-completion)") }   // added while pending
                val doneFuture = loop.submit(Callable { "x" }).sync()
                doneFuture.addListener { record("submit(after-completion)") } // added when done
                loop.schedule({ }, 50, TimeUnit.MILLISECONDS)
                    .addListener { record("schedule(runnable)") }
                check(listened.await(5, TimeUnit.SECONDS)) { "listeners did not all fire: $listenerThreads" }
                println("listener threads      : all on virtual threads -> OK")
                listenerThreads.forEach { println("    $it") }

                // 2e. RunnableNettyTask is a CompletionStage too: submit futures chain with
                // thenApply, and the stage handed out is a defensive copy (cannot complete the task).
                val task = loop.submit(Callable { 21 }) as io.github.pbk20191.virtualloop.RunnableNettyTask<Int>
                val staged = task.thenApply { it * 2 }.toCompletableFuture().get(5, TimeUnit.SECONDS)
                check(staged == 42) { "CompletionStage chain gave $staged" }
                check(task.sync().now == 21) { "netty view disagreed: ${task.now}" }
                println("RunnableNettyTask     : FutureTask + Netty Future + CompletionStage agree -> OK")

                // 2f. PromiseLikeCompletableFuture: one object serving Netty promise listeners,
                // CompletionStage dependents, progress events, and Netty promise contracts.
                val plcf = io.github.pbk20191.virtualloop.PromiseLikeCompletableFuture<String>()
                val seen = java.util.concurrent.ConcurrentLinkedQueue<String>()
                plcf.addListener { done -> seen.add("listener:${(done as io.netty.util.concurrent.Future<*>).now}") }
                plcf.thenApply { seen.add("stage:$it"); it }
                var progressSeen = -1L
                plcf.addListener(
                    object : io.netty.util.concurrent.GenericProgressiveFutureListener<
                        io.netty.util.concurrent.ProgressiveFuture<String>,
                        > {
                        override fun operationProgressed(
                            f: io.netty.util.concurrent.ProgressiveFuture<String>,
                            progress: Long,
                            total: Long,
                        ) {
                            progressSeen = progress
                        }

                        override fun operationComplete(f: io.netty.util.concurrent.ProgressiveFuture<String>) {
                            seen.add("progressive-complete")
                        }
                    },
                )
                plcf.setProgress(5, 10)
                check(progressSeen == 5L) { "progress listener not notified: $progressSeen" }
                check(plcf.trySuccess("ok")) { "trySuccess failed" }
                check(!plcf.trySuccess("dup")) { "second trySuccess must fail" }
                val dupGuard = runCatching { plcf.setSuccess("dup") }.exceptionOrNull()
                check(dupGuard is IllegalStateException) { "setSuccess after complete must throw, got $dupGuard" }
                check(plcf.sync().now == "ok" && plcf.get(1, TimeUnit.SECONDS) == "ok")
                check(seen.containsAll(listOf("listener:ok", "stage:ok", "progressive-complete"))) {
                    "notifications incomplete: $seen"
                }
                println("PromiseLikeCF         : promise contract + listeners + stage + progress -> OK")

                // 2g. Periodic scheduling with a PARKING command - the case the stock timer gets
                // wrong (it measures "time until first park" as the execution time, so rounds
                // overlap). The multi-step chain must (a) never run rounds concurrently and
                // (b) for fixedDelay, measure the delay from TRUE completion: with a 300ms parking
                // command and 100ms delay, consecutive round STARTS must be >= ~400ms apart.
                run {
                    val inFlight = java.util.concurrent.atomic.AtomicInteger()
                    val maxInFlight = java.util.concurrent.atomic.AtomicInteger()
                    val starts = java.util.concurrent.ConcurrentLinkedQueue<Long>()
                    val threeRounds = java.util.concurrent.CountDownLatch(3)
                    val fd = loop.scheduleWithFixedDelay({
                        starts.add(System.nanoTime())
                        maxInFlight.accumulateAndGet(inFlight.incrementAndGet()) { a, b -> maxOf(a, b) }
                        Thread.sleep(300) // parks the round's virtual thread
                        inFlight.decrementAndGet()
                        threeRounds.countDown()
                    }, 10, 100, TimeUnit.MILLISECONDS)
                    check(threeRounds.await(5, TimeUnit.SECONDS)) { "fixedDelay rounds did not run" }
                    check(fd.cancel(false)) { "cancel failed" }
                    check(maxInFlight.get() == 1) { "fixedDelay rounds OVERLAPPED: ${maxInFlight.get()}" }
                    val gaps = starts.toList().zipWithNext { a, b -> (b - a) / 1_000_000 }
                    check(gaps.all { it >= 380 }) { "fixedDelay measured from park, not completion: gaps=${gaps}ms" }
                    println("fixedDelay (parking)  : no overlap, start gaps=${gaps}ms (>=400ms expected) -> OK")
                }

                // 2h. fixedRate with a command that outruns its period: rounds must catch up
                // back-to-back but NEVER concurrently (250ms parking command, 100ms period).
                run {
                    val inFlight = java.util.concurrent.atomic.AtomicInteger()
                    val maxInFlight = java.util.concurrent.atomic.AtomicInteger()
                    val threeRounds = java.util.concurrent.CountDownLatch(3)
                    val fr = loop.scheduleAtFixedRate({
                        maxInFlight.accumulateAndGet(inFlight.incrementAndGet()) { a, b -> maxOf(a, b) }
                        Thread.sleep(250)
                        inFlight.decrementAndGet()
                        threeRounds.countDown()
                    }, 10, 100, TimeUnit.MILLISECONDS)
                    check(threeRounds.await(5, TimeUnit.SECONDS)) { "fixedRate rounds did not run" }
                    // cancel(true) must interrupt the in-flight parked round
                    val interrupted = java.util.concurrent.CountDownLatch(1)
                    val victim = loop.scheduleAtFixedRate({
                        try {
                            Thread.sleep(10_000)
                        } catch (_: InterruptedException) {
                            interrupted.countDown()
                        }
                    }, 1, 1_000, TimeUnit.MILLISECONDS)
                    Thread.sleep(100) // let the round start and park
                    check(victim.cancel(true)) { "periodic cancel(true) failed" }
                    check(interrupted.await(2, TimeUnit.SECONDS)) { "in-flight round was not interrupted" }
                    check(fr.cancel(false))
                    check(maxInFlight.get() == 1) { "fixedRate rounds OVERLAPPED: ${maxInFlight.get()}" }
                    println("fixedRate (overrun)   : no overlap under overrun; cancel(true) interrupts round -> OK")
                }
            } finally {
                group.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(10, TimeUnit.SECONDS)
            }
        }

        // 2i. Loop shutdown must TERMINATE pending periodic chains: cancelScheduledTasks cancels the
        // pending round TIMER directly (not the task), and without timer->chain cancellation
        // propagation the periodic future would stay incomplete forever.
        run {
            val group = VirtualIoEventLoopGroup(nThreads = 1)
            val periodic = group.next().scheduleAtFixedRate({ }, 10_000, 10_000, TimeUnit.MILLISECONDS)
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(10, TimeUnit.SECONDS)
            check(periodic.await(5_000)) { "periodic future never completed after loop shutdown" }
            check(periodic.isCancelled) { "periodic future should be cancelled, was: ${periodic.cause()}" }
            println("periodic vs shutdown  : pending chain terminated (cancelled) by loop shutdown -> OK")
        }

        // 3. Quiet period: with quiet=800ms, a task submitted at ~400ms must restart the window,
        // so termination happens no earlier than ~1200ms; and well under the 10s hard timeout.
        run {
            val group = VirtualIoEventLoopGroup(nThreads = 1)
            val loop = group.next()
            loop.execute { }
            val start = System.nanoTime()
            val future = group.shutdownGracefully(800, 10_000, TimeUnit.MILLISECONDS)
            Thread.sleep(400)
            loop.execute { } // activity during the quiet period restarts the window
            check(future!!.await(15, TimeUnit.SECONDS)) { "group did not terminate" }
            val elapsedMs = (System.nanoTime() - start) / 1_000_000
            check(elapsedMs >= 1_100) { "quiet period was not restarted by activity (took ${elapsedMs}ms)" }
            check(elapsedMs < 9_000) { "quiet period overshot toward the hard timeout (${elapsedMs}ms)" }
            println("quiet period          : activity at +400ms restarted the 800ms window (terminated at ${elapsedMs}ms) -> OK")
        }

        println("RESULT: invokeAll/invokeAny on virtual threads, in-task sync/await, and activity-restarted quiet period all verified.")
    }
}
