package io.github.pbk20191.virtualloop

import io.netty.util.concurrent.DefaultPromise
import java.util.concurrent.Executor

// Loop-wide runtime signals and flags, shared by the loop, the IoHandle proxy and the timer.
// Top-level (same package) so the extracted collaborators reference them without qualification.

/**
 * Presence-only inline hint for [inline]. A [ScopedValue] (not a shared flag) so it is
 * confined to the exact loop-thread window that bound it: Loom swaps scoped-value bindings on
 * mount, so a started virtual thread and anything it submits observe their own unbound copy.
 */
internal val INLINE_NEXT: ScopedValue<Boolean> = ScopedValue.newInstance()

/** RETAIN_CLASS_REFERENCE so frames expose the real declaring Class for an exact match. */
internal val CLASS_WALKER: StackWalker = StackWalker.getInstance(setOf(StackWalker.Option.RETAIN_CLASS_REFERENCE, StackWalker.Option.DROP_METHOD_INFO),1)
private val STACK_WALKER: StackWalker = StackWalker.getInstance(setOf(StackWalker.Option.RETAIN_CLASS_REFERENCE),5)
/** Cached per-class "is a DefaultPromise subtype" test for the tier-1 caller pre-filter. */
internal object PROMISE_FAMILY:  ClassValue<Boolean>() {
    override fun computeValue(type: Class<*>): Boolean =
        DefaultPromise::class.java.isAssignableFrom(type)
}

internal val CONTINUATION_INTERCEPTOR = ScopedValue.newInstance<Executor>()


/**
 * Whether the checkDeadLock disguise is active (default true). Disable with
 * `-Dvirtualloop.disguise=false` to remove the ~260ns getCallerClass cost from every
 * no-arg inEventLoop() call; Netty-internal futures then need VtFutures/newVtPromise for
 * blocking waits from loop virtual threads.
 */
internal val DISGUISE_ENABLED: Boolean =
    System.getProperty("virtualloop.disguise", "true").toBoolean()

/**
 * Whether the VirtualLoopStats counters are maintained (default true; the benches read
 * them). Disable with `-Dvirtualloop.stats=false` to remove two atomic increments from
 * every continuation on the scheduler hot path.
 */
internal val STATS_ENABLED: Boolean =
    System.getProperty("virtualloop.stats", "true").toBoolean()

/**
 * True if the direct caller of [inEventLoop] is `DefaultPromise.checkDeadLock` (or a
 * subclass override delegating to it). Depth-limited: frame 0 is this function, frame 1 is
 * inEventLoop, so only frames 2-4 are inspected - no full stack capture on the hot path.
 */
internal fun calledFromCheckDeadLock(): Boolean = STACK_WALKER.walk { frames ->
    frames.skip(2).limit(3).anyMatch { f ->
        f.methodName == "checkDeadLock" && PROMISE_FAMILY.get(f.declaringClass)
    }
}

/**
 * Caller-class test for Netty's pipeline context (package-private, so matched by name and cached
 * per class like [PROMISE_FAMILY]).
 */
internal val PIPELINE_CONTEXT: ClassValue<Boolean> = object : ClassValue<Boolean>() {
    override fun computeValue(type: Class<*>): Boolean =
        type.name == "io.netty.channel.AbstractChannelHandlerContext"
}

/**
 * Whether the (no-arg) inEventLoop call was made by Netty 4.2's
 * AbstractChannelHandlerContext.ensurePromiseUseCorrectExecutor - the promise-replacement check
 * on every outbound op. VirtualEventExecutor must answer it differently from pipeline DISPATCH
 * calls that come from the very same class (see its inEventLoop for why).
 */
internal fun calledFromEnsurePromiseExecutor(): Boolean = STACK_WALKER.walk { frames ->
    frames.skip(2).limit(3).anyMatch { f ->
        f.methodName == "ensurePromiseUseCorrectExecutor"
    }
}
