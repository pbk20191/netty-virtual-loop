package io.github.pbk20191.virtualloop

import io.netty.util.concurrent.EventExecutor
import io.netty.util.concurrent.FastThreadLocal
import io.netty.util.concurrent.FastThreadLocalThread
import io.netty.util.internal.InternalThreadLocalMap
import io.netty.util.internal.ThreadExecutorMap
import java.lang.reflect.Constructor
import java.lang.reflect.Method
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

// LOOP-LEVEL FastThreadLocal SHARING (opt-in via VirtualIoEventLoopGroup(sharedFastThreadLocals)).
//
// One InternalThreadLocalMap per VirtualIoEventLoop, injected into every virtual thread the loop
// creates (its JDK slow-path ThreadLocal holder). Because all of a loop's virtual threads are
// carried by ONE platform thread, accesses to the shared map are serialized by mounting - no data
// race is possible between them. What this buys (verified against Netty 4.2 sources):
//  - ThreadExecutorMap.setCurrentExecutor stamped ONCE into the shared map makes
//    currentExecutor() == the loop for every loop thread, which opens the allocator gates:
//    PooledByteBufAllocator's PoolThreadLocalCache and AdaptivePoolingAllocator's thread-local
//    magazine groups both grant a REAL cache when currentExecutor() != null - and since the
//    backing storage is the (now shared) InternalThreadLocalMap, that cache/magazine group is
//    genuinely SHARED BY THE WHOLE LOOP: the per-carrier buffer pooling that per-thread keying
//    otherwise makes impossible.
//  - Plain FastThreadLocal values (stringBuilder scratch, charset codecs, user FTLs) become
//    loop-scoped instead of dying with each task virtual thread.
//
// The sharp edges, and why they are accepted for an OPT-IN:
//  - BORROW-SEMANTICS scratch (e.g. InternalThreadLocalMap.stringBuilder()) assumes nothing can
//    interleave mid-borrow. Netty-internal borrows never park mid-use; user/third-party FTL code
//    that blocks while holding borrowed scratch WOULD interleave with another loop thread. That
//    is the inherent trade of sharing and the reason the default stays OFF.
//  - During teardown the scheduler's REE fallback may run a continuation off-carrier; the shared
//    map can then be touched from a foreign thread for that bounded window.
//  - Drain threads must NOT use FastThreadLocalThread.runWithFastThreadLocal here: its exit
//    removeAll() would wipe the SHARED map under every other thread. Instead the drain's thread
//    id is registered in FastThreadLocalThread's fallback set (Recycler's gate) reflectively and
//    unregistered WITHOUT removeAll; the map is cleaned exactly once at loop termination.

/** Reflection bridge for the pieces Netty does not expose (see file doc). */
internal object SharedFtlSupport {

    private val failure: Throwable?
    private val slowThreadLocal: ThreadLocal<InternalThreadLocalMap>?
    private val mapConstructor: Constructor<InternalThreadLocalMap>?
    private val fallbackThreadsRef: AtomicReference<Any>?
    private val fallbackAdd: Method?
    private val fallbackRemove: Method?

    init {
        var failed: Throwable? = null
        var slow: ThreadLocal<InternalThreadLocalMap>? = null
        var ctor: Constructor<InternalThreadLocalMap>? = null
        var ref: AtomicReference<Any>? = null
        var add: Method? = null
        var remove: Method? = null
        try {
            // getDeclaredField, NOT getField: both members are private (getField sees public only).
            @Suppress("UNCHECKED_CAST")
            slow = InternalThreadLocalMap::class.java.getDeclaredField("slowThreadLocalMap")
                .apply { isAccessible = true }.get(null) as ThreadLocal<InternalThreadLocalMap>
            ctor = InternalThreadLocalMap::class.java.getDeclaredConstructor()
                .apply { isAccessible = true }
            @Suppress("UNCHECKED_CAST")
            ref = FastThreadLocalThread::class.java.getDeclaredField("fallbackThreads")
                .apply { isAccessible = true }.get(null) as AtomicReference<Any>
            val setClass = ref.get().javaClass // FastThreadLocalThread.FallbackThreadSet (COW)
            add = setClass.getDeclaredMethod("add", Long::class.javaPrimitiveType).apply { isAccessible = true }
            remove = setClass.getDeclaredMethod("remove", Long::class.javaPrimitiveType).apply { isAccessible = true }
        } catch (t: Throwable) {
            failed = t
        }
        failure = failed
        slowThreadLocal = slow
        mapConstructor = ctor
        fallbackThreadsRef = ref
        fallbackAdd = add
        fallbackRemove = remove
    }

    val isSupported: Boolean get() = failure == null

    fun newMap(): InternalThreadLocalMap = mapConstructor!!.newInstance()

    /** Binds [map] as the CURRENT thread's InternalThreadLocalMap (the non-FTLT slow path). */
    fun install(map: InternalThreadLocalMap) {
        slowThreadLocal!!.set(map)
    }

    /** Adds [threadId] to Netty's fallback set - Recycler's willCleanup gate - via CAS on the COW set. */
    fun registerFallback(threadId: Long) {
        val ref = fallbackThreadsRef!!
        while (true) {
            val cur = ref.get()
            val next = fallbackAdd!!.invoke(cur, threadId)
            if (ref.compareAndSet(cur, next)) return
        }
    }

    /** Removes [threadId] WITHOUT FastThreadLocal.removeAll - the shared map outlives the thread. */
    fun unregisterFallback(threadId: Long) {
        val ref = fallbackThreadsRef!!
        while (true) {
            val cur = ref.get()
            val next = fallbackRemove!!.invoke(cur, threadId)
            if (ref.compareAndSet(cur, next)) return
        }
    }
}

/** One loop's shared-FTL state: the map, the one-time executor stamp, and termination cleanup. */
internal class SharedFtlDomain(private val executor: EventExecutor) {

    private val map = SharedFtlSupport.newMap()
    private val executorStamped = AtomicBoolean()

    /** Called first thing on every loop virtual thread (the thread factory wraps bodies). */
    fun installOnCurrentThread() {
        SharedFtlSupport.install(map)
        if (!executorStamped.get() && executorStamped.compareAndSet(false, true)) {
            // Lands in the SHARED map: from now on every loop thread answers
            // ThreadExecutorMap.currentExecutor() == the loop - the allocator gate opener.
            ThreadExecutorMap.setCurrentExecutor(executor)
        }
    }

    fun registerDrain() = SharedFtlSupport.registerFallback(Thread.currentThread().threadId())

    fun unregisterDrain() = SharedFtlSupport.unregisterFallback(Thread.currentThread().threadId())

    /**
     * Runs FastThreadLocal.removeAll ONCE under the shared map so every onRemoval hook fires
     * (PoolThreadCache.free, magazine release, user hooks). A throwaway platform thread is used
     * because removeAll operates on "the current thread's map" - and every loop thread is gone by
     * termination time.
     */
    fun cleanup(factory: ThreadFactory) {
        factory.newThread {
            SharedFtlSupport.install(map)
            FastThreadLocal.removeAll()
        }.start()
    }
}
