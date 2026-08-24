package io.github.pbk20191.virtualloop

import io.netty.channel.*
import io.netty.util.concurrent.*
import io.netty.util.concurrent.Future
import io.netty.util.concurrent.ScheduledFuture
import java.util.ArrayDeque
import java.util.concurrent.*

// The loop-owned timer: an AbstractScheduledEventExecutor opened up (AbstractScheduler), the
// carrier-confined Scheduler around it, the SchedulerLoop driver, and the one-shot future the
// loop hands out for schedule().

import java.util.Queue

internal abstract class AbstractScheduler(parent: EventExecutorGroup? = null): AbstractScheduledEventExecutor(parent) {
    
    abstract override fun inEventLoop(thread: Thread): Boolean

    override fun afterScheduledTaskSubmitted(deadlineNanos: Long): Boolean {
        return super.afterScheduledTaskSubmitted(deadlineNanos)
    }

    override fun beforeScheduledTaskSubmitted(deadlineNanos: Long): Boolean {
        return super.beforeScheduledTaskSubmitted(deadlineNanos)
    }

    public override fun fetchFromScheduledTaskQueue(taskQueue: Queue<Runnable>): Boolean {
        return super.fetchFromScheduledTaskQueue(taskQueue)
    }

    public override fun cancelScheduledTasks() {
        super.cancelScheduledTasks()
    }
    
    fun nextScheduledDeadlineNanos() = nextScheduledTaskDeadlineNanos()
    
    fun nextScheduled() = nextScheduledTaskNano()
    
    fun pollTask(nanoTime: Long = this.ticker().nanoTime()): Runnable? = pollScheduledTask(nanoTime)
    
    fun hasScheduled() = hasScheduledTasks()
    
    
}
internal class Scheduler(private val actual: IoEventLoop, private val carrier: ThreadAwareExecutor): AbstractScheduler(actual) {

    val scopedValue = ScopedValue.newInstance<Boolean>()

    override fun inEventLoop(thread: Thread): Boolean {
        if (scopedValue.isBound && scopedValue.get()) {
            return true
        }
        if (carrier.isExecutorThread(thread)) {
            return true
        }
        // A virtual thread MOUNTED on the carrier is physically executing on the carrier
        // thread right now: its queue mutations can never race the driver (also
        // carrier-confined), so schedule()/cancel() from it may touch the scheduled queue
        // DIRECTLY - no marshalling, no wakeup. This is what makes a periodic round's
        // re-schedule (runOnce runs on a mounted virtual thread) wakeup-free.
        return thread.isVirtual &&
            PrivateLoomSupport.carrierOf(thread)?.let { carrier.isExecutorThread(it) } == true
    }

    override fun isShuttingDown(): Boolean {
        return actual.isShuttingDown
    }

    override fun shutdownGracefully(
        quietPeriod: Long,
        timeout: Long,
        unit: TimeUnit?
    ): Future<*> {
       return actual.shutdownGracefully(quietPeriod, timeout, unit)
    }

    override fun terminationFuture(): Future<*>? {
       return actual.terminationFuture()
    }

    override fun shutdown() {
       return actual.shutdown()
    }

    override fun isShutdown(): Boolean {
        return actual.isShutdown
    }

    override fun isTerminated(): Boolean {
       return actual.isTerminated
    }

    override fun awaitTermination(timeout: Long, unit: TimeUnit): Boolean {
        return actual.awaitTermination(timeout, unit)
    }

    override fun execute(command: Runnable) {
        // AbstractScheduledEventExecutor's scheduled-task queue is NOT thread-safe (hence its
        // assert inEventLoop); schedule() from a foreign thread marshals the add through THIS
        // executor. Routing to the carrier serializes every queue mutation with the driver,
        // which also only ever runs on the carrier. Routing to actual.execute (a fresh virtual
        // thread) would race the driver on the priority queue.
        carrier.execute(command)
    }

}

/**
 * The timer driver for [nettyScheduler]. Entirely CARRIER-CONFINED: [run] fires from a carrier
 * timer (or a marshalled [arm] request), drains due tasks, dispatches them under the scheduler's
 * ScopedValue disguise (so ScheduledFutureTask's inEventLoop assertions/paths are satisfied),
 * and re-arms a carrier one-shot for the next deadline. Task bodies we enqueue are start-a-VT
 * shims (`{ inline(task) }`), so dispatch never blocks the carrier.
 *
 * [arm] solves the early-wakeup problem: when a NEW task lands with an earlier deadline than the
 * currently armed one, the stale carrier timer is cancelled and re-armed - without this, a
 * schedule(1s) after a schedule(1h) would not fire for an hour.
 */
internal class SchedulerLoop(
    val scheduler: Scheduler,
    val carrier: SingleThreadIoEventLoop,
): Runnable {
    private val scope = ScopedValue.where(scheduler.scopedValue, true)

    // carrier-confined state
    private var armedDeadlineNanos = Long.MAX_VALUE
    private var armedTimer: java.util.concurrent.Future<*>? = null

    /** Re-arm for the earliest pending deadline. MUST run on the carrier thread. */
    fun arm() {
        val next = scheduler.nextScheduledDeadlineNanos()
        if (next == -1L) {
            return
        }
        val now = scheduler.ticker().nanoTime()
        if (next <= now) {
            // already due - run the drain directly instead of a zero-delay timer hop
            run()
            return
        }
        if (next >= armedDeadlineNanos) {
            return // an earlier-or-equal wakeup is already armed
        }
        armedTimer?.cancel(false)
        armedDeadlineNanos = next
        armedTimer = carrier.schedule(this, next - now, TimeUnit.NANOSECONDS)
    }

    override fun run() {
        armedDeadlineNanos = Long.MAX_VALUE
        armedTimer = null
        val deq = ArrayDeque<Runnable>()
        while (true) {
            // fetchFromScheduledTaskQueue always returns true for an unbounded queue - the
            // loop must terminate on "nothing due", not on the return value.
            scope.call(ScopedValue.CallableOp { scheduler.fetchFromScheduledTaskQueue(deq) })
            if (deq.isEmpty()) {
                break
            }
            val i = deq.iterator()
            while (i.hasNext()) {
                val job = i.next()
                i.remove()
                try {
                    scope.run(job)
                } catch (t: Throwable) {
                    val self = Thread.currentThread()
                    self.uncaughtExceptionHandler.uncaughtException(self, t)
                }
            }
        }
        arm()
    }
}

// OneShot
internal class OneShotScheduledFuture<V>(
    private val timer: ScheduledFuture<*>,
    private val innerHandle: RunnableNettyFuture<V>
): Future<V> by innerHandle, ScheduledFuture<V> {

    init {
        // Propagate CANCELLATION only: the timer also completes on SUCCESS (after starting the
        // task), and in the rare window where the task's virtual thread has been started but
        // run() has not begun, an unconditional cancel(false) would kill a task that fired.
        timer.addListener {
            if (it.isCancelled) {
                innerHandle.cancel(false)
            }
        }
    }

    override fun getDelay(unit: TimeUnit): Long {
        return timer.getDelay(unit)
    }

    override fun compareTo(other: Delayed?): Int {
        return timer.compareTo(other)
    }

    override fun cancel(p0: Boolean): Boolean {
        val t = innerHandle.cancel(p0)
        timer.cancel(p0)
        return t
    }

    override fun exceptionNow(): Throwable? {
        return innerHandle.exceptionNow()
    }

    override fun state(): java.util.concurrent.Future.State? {
        return innerHandle.state()
    }

}
