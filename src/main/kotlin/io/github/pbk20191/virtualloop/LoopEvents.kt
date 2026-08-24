package io.github.pbk20191.virtualloop

import jdk.jfr.Category
import jdk.jfr.Description
import jdk.jfr.Enabled
import jdk.jfr.Event
import jdk.jfr.Label
import jdk.jfr.Name
import jdk.jfr.StackTrace
import jdk.jfr.Timespan

// JFR events for production observability (pattern borrowed from Micronaut's LoomCarrierGroup):
// all events are @Enabled(false) by default and guarded by a static INSTANCE.isEnabled() check,
// so the disabled cost on hot paths is one branch - cheaper than the always-on atomic counters
// in VirtualLoopStats, and recordable/visualizable in production via JFR/JMC. Enable with e.g.
//   jcmd <pid> JFR.start settings=none +io.github.pbk20191.virtualloop.ContinuationScheduled#enabled=true
// or Recording.enable(name) programmatically. @StackTrace(false) everywhere: these fire on hot
// paths where stack capture would dominate the cost.

/** Correlate with [ContinuationRun] via [hash] to measure schedule-to-run latency. */
@Name("io.github.pbk20191.virtualloop.ContinuationScheduled")
@Label("Continuation Scheduled")
@Category("Netty Virtual Loop")
@Description("A virtual-thread continuation entered the loop scheduler; mode: 1=intercepted (drain capture), 2=inline mount, 3=same-carrier lazyExecute, 4=queued to carrier")
@StackTrace(false)
@Enabled(false)
internal class ContinuationScheduled : Event() {
    @JvmField @Label("Continuation hash") var hash: Int = 0
    @JvmField @Label("Dispatch mode") var mode: Int = 0
    @JvmField @Label("Submitter thread") var submitter: String? = null

    companion object { @JvmField val INSTANCE = ContinuationScheduled() }
}

/** Duration of one mounted continuation stretch on the carrier (run-to-park/completion). */
@Name("io.github.pbk20191.virtualloop.ContinuationRun")
@Label("Continuation Run")
@Category("Netty Virtual Loop")
@StackTrace(false)
@Enabled(false)
internal class ContinuationRun : Event() {
    @JvmField @Label("Continuation hash") var hash: Int = 0

    companion object { @JvmField val INSTANCE = ContinuationRun() }
}

/** One IO event delivered to the wrapped IoHandle (duration = handler pipeline execution). */
@Name("io.github.pbk20191.virtualloop.IoEventHandled")
@Label("IO Event Handled")
@Category("Netty Virtual Loop")
@StackTrace(false)
@Enabled(false)
internal class IoEventHandled : Event() {
    @JvmField @Label("Handle type") var handleType: String? = null
    @JvmField @Label("Ran directly on drain VT") var direct: Boolean = false

    companion object { @JvmField val INSTANCE = IoEventHandled() }
}

/** One periodic-chain round (duration = true command execution incl. parks). */
@Name("io.github.pbk20191.virtualloop.PeriodicRound")
@Label("Periodic Round")
@Category("Netty Virtual Loop")
@StackTrace(false)
@Enabled(false)
internal class PeriodicRound : Event() {
    @JvmField @Label("Start lateness") @Timespan(Timespan.NANOSECONDS) var latenessNanos: Long = 0
    @JvmField @Label("Fixed rate") var fixedRate: Boolean = false

    companion object { @JvmField val INSTANCE = PeriodicRound() }
}

/**
 * Periodic snapshot of the [VirtualLoopStats] counters (cumulative; delta them on the JMC
 * timeline). Values are only live while `-Dvirtualloop.stats=true` (the default) keeps the
 * counters incrementing; [statsEnabled] records that so a flat-zero recording is explainable.
 */
@Name("io.github.pbk20191.virtualloop.LoopStats")
@Label("Loop Scheduler Stats")
@Category("Netty Virtual Loop")
@StackTrace(false)
@Enabled(false)
@jdk.jfr.Period("1 s")
internal class LoopStats : Event() {
    @JvmField @Label("Inline continuations") var inline: Long = 0
    @JvmField @Label("Queued continuations") var queued: Long = 0
    @JvmField @Label("Intercepted continuations") var intercepted: Long = 0
    @JvmField @Label("Same-carrier continuations") var sameCarrier: Long = 0
    @JvmField @Label("Counters enabled") var statsEnabled: Boolean = false
}

/**
 * One-time registration of the [LoopStats] periodic hook. addPeriodicEvent only appends to a
 * static task list (no JFR engine start); the hook runs solely while a recording has the event
 * enabled. Called from the loop constructor so registration happens iff the library is used.
 *
 * JDK LIMITATION (verified on 25.0.3): a periodic hook added while a recording is ALREADY
 * running is not activated for that recording - periodic-task enablement is only evaluated on
 * recording state transitions. Start (or restart) the recording after the first
 * VirtualIoEventLoop exists to capture LoopStats.
 */
internal object PeriodicStats {
    @Volatile
    private var registered = false

    fun ensureRegistered() {
        if (registered) return
        synchronized(this) {
            if (registered) return
            try {
                // Explicit type registration: every other event registers itself when its
                // companion INSTANCE is constructed, but LoopStats instances only exist inside
                // the hook - without this, enable-by-name never matches and the hook stays off.
                jdk.jfr.FlightRecorder.register(LoopStats::class.java)
                jdk.jfr.FlightRecorder.addPeriodicEvent(LoopStats::class.java) {
                    val e = LoopStats()
                    e.inline = VirtualLoopStats.inlineContinuations.get()
                    e.queued = VirtualLoopStats.queuedContinuations.get()
                    e.intercepted = VirtualLoopStats.interceptedContinuations.get()
                    e.sameCarrier = VirtualLoopStats.sameCarrierContinuations.get()
                    e.statsEnabled = STATS_ENABLED
                    e.commit()
                }
            } catch (_: Throwable) {
                // JFR unavailable (stripped runtime): counters stay readable programmatically.
            }
            registered = true
        }
    }
}

/** Whole graceful-shutdown span: begin at the request, commit when terminationFuture completes. */
@Name("io.github.pbk20191.virtualloop.LoopShutdown")
@Label("Loop Shutdown")
@Category("Netty Virtual Loop")
@StackTrace(false)
@Enabled(false)
internal class LoopShutdown : Event() {
    @JvmField @Label("Escalated to shutdownNow") var forced: Boolean = false
    @JvmField @Label("Quiet period (ms)") var quietMillis: Long = 0
    @JvmField @Label("Timeout (ms)") var timeoutMillis: Long = 0

    companion object { @JvmField val INSTANCE = LoopShutdown() }
}
