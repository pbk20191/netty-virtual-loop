package io.github.pbk20191.virtualloop

import io.netty.util.internal.EmptyArrays
import java.lang.invoke.MethodHandle
import java.lang.invoke.MethodHandles
import java.lang.invoke.VarHandle
import java.lang.reflect.InvocationHandler
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

/**
 * Bridge to the JDK's OFFICIAL custom virtual-thread scheduler API - `Thread.VirtualThreadScheduler`
 * / `Thread.VirtualThreadTask` - which is the eventual, spec-clean replacement for the whole
 * [PrivateLoomSupport] reflection layer. It is DORMANT on every JDK that predates that API (both
 * JDK 25 GA and the 27-ea build this project develops on lack it), reporting [Status.API_ABSENT];
 * only a future loom build turns it [Status.READY].
 *
 * Everything here is expressed against LOCAL proxy interfaces ([VirtualThreadTaskProxy] /
 * [VirtualThreadSchedulerProxy]) so the module carries ZERO compile-time reference to the JDK
 * types - the classes are resolved by name at init and adapted through MethodHandles. Init is
 * fail-closed: any absence or shape mismatch leaves the object at a non-READY status with a live
 * [failure] cause, never an ExceptionInInitializerError.
 *
 * Design mirrors Micronaut's / franz1981's LoomBranchSupport: a scheduler injected into a
 * `Thread.Builder.OfVirtual` (via the builder's `scheduler` field, reachable through
 * [LookupUnsafe]) receives onStart/onContinue callbacks carrying a VirtualThreadTask whose run()
 * IS the continuation - so forwarding those to a Netty loop makes the loop carry the VTs, exactly
 * like the reflection path does today, but through supported API.
 */
object VirtualThreadBranchingSupport {

    enum class Status { API_ABSENT, BUILDER_FIELD_ABSENT, NOT_OPENED, READY }

    /** Local mirror of `java.lang.Thread.VirtualThreadTask` (no compile-time JDK reference). */
    interface VirtualThreadTaskProxy : Runnable {
        fun thread(): Thread
        fun preferredCarrier(): Thread?
        fun attach(att: Any?): Any?
        fun attachment(): Any?
    }

    /** Local mirror of `java.lang.Thread.VirtualThreadScheduler`. */
    interface VirtualThreadSchedulerProxy {
        /** Continuation start; run() executes the continuation on the carrier this forwards to. */
        fun onStart(task: VirtualThreadTaskProxy)

        /** Continuation resume after an unpark. */
        fun onContinue(task: VirtualThreadTaskProxy)

        /** Timed park/wait delay; the returned Future must support cancel(false). */
        fun schedule(task: Runnable, delay: Long, unit: TimeUnit): Future<*>

        /** Creation hook (instrumentation). */
        fun newThread(builder: Thread.Builder.OfVirtual, preferredCarrier: Thread?, task: Runnable): VirtualThreadTaskProxy
    }

    private class VTaskSupport(
        val clazz: Class<*>,
        table: Map<Method, MethodHandle>,
    ) {
        // Pre-adapted to (task) -> * so callers invoke without re-adapting (see AbstractInterfaceMethodMap).
        val thread: MethodHandle = table.getValue(clazz.getMethod("thread"))
        val carrier: MethodHandle = table.getValue(clazz.getMethod("preferredCarrier"))
        val attach: MethodHandle = table.getValue(clazz.getMethod("attach", Any::class.java))
        val attachment: MethodHandle = table.getValue(clazz.getMethod("attachment"))
    }

    private class VSchedulerSupport(
        val clazz: Class<*>,
        val default: Any,
        val table: Map<Method, MethodHandle>,
    ) {
        /** Handles bound to the default scheduler, for forwarding un-overridden methods to it. */
        val boundToDefault: Map<Method, MethodHandle> = table.mapValues { it.value.bindTo(default) }
    }

    private class LoomBranchSupport(
        val vTask: VTaskSupport,
        val vScheduler: VSchedulerSupport,
        val builderSchedulerField: VarHandle,
    )

    val status: Status
    val failure: Throwable?
    private val env: LoomBranchSupport?

    init {
        var st = Status.API_ABSENT
        var e: LoomBranchSupport? = null
        var failed: Throwable? = null
        try {
            val sched = Class.forName("java.lang.Thread\$VirtualThreadScheduler")
            val task = Class.forName("java.lang.Thread\$VirtualThreadTask")
            require(Runnable::class.java.isAssignableFrom(task)) { "VirtualThreadTask is not Runnable" }

            val vThreadClass = Thread.ofVirtual().unstarted { }.javaClass
            val defaultScheduler = LookupUnsafe.lookupIn(vThreadClass)
                .unreflect(vThreadClass.getDeclaredMethod("defaultScheduler"))
                .invoke()!!

            st = Status.BUILDER_FIELD_ABSENT
            // getDeclaredField needs no opens; the field's type confirms the expected snapshot.
            val field = Thread.ofVirtual().javaClass.getDeclaredField("scheduler")
            require(field.type == sched) { "builder scheduler field is not VirtualThreadScheduler" }

            st = Status.NOT_OPENED
            val lk = MethodHandles.publicLookup()
            e = LoomBranchSupport(
                builderSchedulerField = LookupUnsafe.lookupIn(field.declaringClass).unreflectVarHandle(field),
                vScheduler = VSchedulerSupport(
                    clazz = sched,
                    default = defaultScheduler,
                    table = sched.methods.associateWith { lk.unreflect(it) },
                ),
                vTask = VTaskSupport(
                    clazz = task,
                    table = task.methods.associateWith { lk.unreflect(it) },
                ),
            )
            st = Status.READY
        } catch (t: Throwable) {
            failed = t // fail-closed: any snapshot drift leaves a non-READY status with a cause
        }
        status = st
        failure = failed
        env = e
    }

    val isSupported: Boolean get() = status == Status.READY

    /**
     * Install [service] as the scheduler for a fresh virtual-thread builder and return the builder;
     * threads it creates are carried by [service]. Only valid when [isSupported]; the returned
     * builder is single-use per the Thread.Builder contract (share its factory(), not the builder).
     */
    fun install(service: ScheduledExecutorService): Thread.Builder.OfVirtual {
        val env = requireNotNull(env) { "VirtualThreadBranchingSupport not READY: $status" }
        val handler = SchedulerInvocationHandler(service, env)
        val schedulerProxy = Proxy.newProxyInstance(
            env.vScheduler.clazz.classLoader,
            arrayOf(env.vScheduler.clazz),
            handler,
        )
        val builder = Thread.ofVirtual()
        env.builderSchedulerField.set(builder, schedulerProxy)
        return builder
    }

    /** Adapts a JDK VirtualThreadTask to our local proxy interface (run() forwards the continuation). */
    private class TaskAdapter(private val task: Runnable, private val env: VTaskSupport) : VirtualThreadTaskProxy {
        init {
            require(env.clazz.isInstance(task)) { "not a VirtualThreadTask" }
        }
        override fun run() = task.run()
        override fun thread(): Thread = env.thread.invoke(task) as Thread
        override fun preferredCarrier(): Thread? = env.carrier.invoke(task) as Thread?
        override fun attach(att: Any?): Any? = env.attach.invoke(task, att)
        override fun attachment(): Any? = env.attachment.invoke(task)
    }

    /**
     * JDK -> our loop. The JDK calls onStart/onContinue with a VirtualThreadTask (run() = the
     * continuation); we forward it to [service]. schedule() forwards timed parks. Everything else
     * (newThread and any future method) falls through to the default scheduler, so the bridge stays
     * correct even if the interface grows.
     */
    private class SchedulerInvocationHandler(
        private val service: ScheduledExecutorService,
        private val env: LoomBranchSupport,
    ) : InvocationHandler {

        override fun invoke(proxy: Any, method: Method, args: Array<out Any?>?): Any? {
            when (method.name) {
                "hashCode" -> if (method.parameterCount == 0) return System.identityHashCode(proxy)
                "equals" -> if (method.parameterCount == 1) return proxy === args!![0]
                "toString" -> if (method.parameterCount == 0) return "VirtualThreadSchedulerProxy@${System.identityHashCode(proxy)}"
                "onStart", "onContinue" -> {
                    // Signature: (VirtualThreadScheduler is the receiver; arg0 = VirtualThreadTask).
                    val task = args!![0] as Runnable
                    service.execute(TaskAdapter(task, env.vTask))
                    return null
                }
                "schedule" -> {
                    val task = args!![0] as Runnable
                    val delay = args[1] as Long
                    val unit = args[2] as TimeUnit
                    return service.schedule(task, delay, unit)
                }
            }
            // Fallthrough (newThread, future additions): defer to the default scheduler's own
            // implementation - handles bound to it, so a plain invoke with the proxy args.
            val bound = env.vScheduler.boundToDefault[method]
            return try {
                if (bound != null) {
                    if (args == null) bound.invoke() else bound.invokeWithArguments(*args)
                } else {
                    method.invoke(env.vScheduler.default, *(args ?: EmptyArrays.EMPTY_OBJECTS))
                }
            } catch (e: InvocationTargetException) {
                throw e.targetException
            }
        }
    }
}
