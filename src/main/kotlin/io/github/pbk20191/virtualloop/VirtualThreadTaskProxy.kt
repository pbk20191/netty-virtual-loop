package io.github.pbk20191.virtualloop

import io.netty.channel.EventLoop
import io.netty.util.internal.EmptyArrays
import java.lang.invoke.MethodHandle
import java.lang.invoke.MethodHandles
import java.lang.invoke.VarHandle
import java.lang.reflect.InvocationHandler
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.util.concurrent.Future
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit



object VirtualThreadBridge {
    enum class Status { API_ABSENT, BUILDER_FIELD_ABSENT, NOT_OPENED, READY, INJECTION_INEFFECTIVE, UNSUPPORTED_PLATFORM }
    /** JDK Thread.VirtualThreadTask 1:1 (컴파일 타임 JDK 타입 참조 0) */
    interface VirtualThreadTaskProxy : Runnable {
        fun thread(): Thread
        fun preferredCarrier(): Thread?
        fun attach(att: Any?): Any?
        fun attachment(): Any?
//    fun unwrap(): Any
    }

    /** JDK Thread.VirtualThreadScheduler 1:1 */
    interface VirtualThreadSchedulerProxy {
        /** 가상 스레드에서 호출될 땐 pinned → 논블로킹 enqueue만. task.run()은 플랫폼 스레드에서(WrongThreadException). */
        fun onStart(task: VirtualThreadTaskProxy)
        fun onContinue(task: VirtualThreadTaskProxy)
        /** 타임드 park/wait 지연 태스크. null = JDK 기본. USE_STPE(CPU<4 기본)면 호출되지 않음(-Djdk.virtualThreadScheduler.useSTPE=false로 강제).
         *  Future는 cancel(false) 지원, task는 플랫폼 스레드에서 실행(다시 onContinue로 들어옴). */
        fun schedule(task: Runnable, delay: Long, unit: TimeUnit): Future<*>
        /** 생성 훅(계측용). null = JDK 기본. 오버라이드는 드물어야 함(@implSpec). */
        fun newThread(builder: Thread.Builder.OfVirtual, preferredCarrier: Thread?, task: Runnable): VirtualThreadTaskProxy
    }

    private class VTaskSupport(
        val clazz:Class<Runnable>,
        val table:Map<Method, MethodHandle>,
    ) {
        val thread: MethodHandle by lazy(LazyThreadSafetyMode.PUBLICATION) {
            table[clazz.getMethod("thread")]!!
        }
        val carrier: MethodHandle by lazy(LazyThreadSafetyMode.PUBLICATION) {
            table[clazz.getMethod("preferredCarrier")]!!
        }
        val attach: MethodHandle by lazy(LazyThreadSafetyMode.PUBLICATION) {
            table[clazz.getMethod("attach")]!!
        }
        val attachment: MethodHandle by lazy(LazyThreadSafetyMode.PUBLICATION) {
            table[clazz.getMethod("attachment")]!!
        }
    }

    private class VSchedularSupport(
        val clazz: Class<*>,
        val default:Any,
        val table:Map<Method, MethodHandle>,

    ) {
        val newThread:MethodHandle by lazy(LazyThreadSafetyMode.PUBLICATION) {
            table[clazz.getMethod("newThread")]!!
        }
        val schedule:MethodHandle by lazy(LazyThreadSafetyMode.PUBLICATION) {
            table[clazz.getMethod("schedule")]!!
        }
        val onStart:MethodHandle by lazy(LazyThreadSafetyMode.PUBLICATION) {
            table[clazz.getMethod("onStart")]!!
        }
        val onContinue:MethodHandle by lazy(LazyThreadSafetyMode.PUBLICATION) {
            table[clazz.getMethod("onContinue")]!!
        }
    }

    private class LoomBranchSupport(
        val vTask:VTaskSupport,
        val vScheduler: VSchedularSupport,
        val factoryField: VarHandle,
        ) {

        fun invokeNewThreadFromScheduler(
            instance:Any,
            builder: Thread.Builder.OfVirtual,
            preferredCarrier: Thread?,
            task: Runnable,
        ): Runnable {
            check(vScheduler.clazz.isInstance(instance))
            return vScheduler.newThread.invoke(instance, builder, preferredCarrier, task) as Runnable
        }

        fun invokeSchedule(
            instance: Any,
            task: Runnable,
            delay: Long,
            unit: TimeUnit,
        ): Future<*> {
            check(vScheduler.clazz.isInstance(instance))
            return vScheduler.schedule.invoke(instance, task, delay, unit) as Future<*>
        }

        fun setScheduler(
            instance: Thread.Builder.OfVirtual,
            other:Any
        ) {
            check(vScheduler.clazz.isInstance(other))
            factoryField.set(instance, other)
        }

    }

    val status: Status
    private val env: LoomBranchSupport?

    init {
        var st = Status.API_ABSENT
        var e: LoomBranchSupport? = null
        try {
            val sched = Class.forName("java.lang.Thread\$VirtualThreadScheduler")
            val task  = Class.forName("java.lang.Thread\$VirtualThreadTask")
            val vThreadClass = Thread.ofVirtual().unstarted {  }.javaClass
            val defaultSchedulerMethod = vThreadClass.getDeclaredMethod("defaultScheduler")

            val defaultScheduler = LookupUnsafe.lookupIn(vThreadClass).unreflect(defaultSchedulerMethod).invoke()
            require(Runnable::class.java.isAssignableFrom(task))                 // run()을 캐스팅으로 부르는 전제
            st = Status.BUILDER_FIELD_ABSENT
            val f = Thread.ofVirtual().javaClass
                .getDeclaredField("scheduler")                                   // getDeclaredField는 opens 불필요
            require(f.type == sched)
            st = Status.NOT_OPENED
            val lk = MethodHandles.publicLookup()
            // 핸들을 Kotlin 호출부가 뽑는 디스크립터로 미리 asType → invokeExact 가능

            e = LoomBranchSupport(
                factoryField = LookupUnsafe.lookupIn(f.declaringClass).unreflectVarHandle(f),
                vScheduler = VSchedularSupport(
                    clazz = sched,
                    default = defaultScheduler,
                    table = sched.methods.associateWith { lk.unreflect(it) }
                ),
                vTask = VTaskSupport(
                    clazz = task as Class<Runnable>,
                    table = task.methods.associateWith { lk.unreflect(it) }
                ),
            )
            st = Status.READY
        } catch (_: Throwable) { }                                               // 스냅샷 변경(@since 99) 등 → fail-closed
        status = st
        env = e
    }

    // ── 내 코드 → JDK: 태스크 어댑터 (JDK attach 슬롯에 자기 자신을 캐시 → 태스크당 1회 할당) ──
    private class TaskAdapter(private val runnable: Runnable, private val env: VTaskSupport) : VirtualThreadTaskProxy {

        init {
            require(env.clazz.isInstance(runnable))
        }

        override fun run() = runnable.run()
        override fun thread(): Thread = env.thread.invoke(runnable) as Thread
        override fun preferredCarrier(): Thread? = env.carrier.invoke(runnable) as Thread?
        override fun attach(att: Any?): Any? = env.attach.invoke(runnable, att)
        override fun attachment(): Any? = env.attachment.invoke(runnable)
//        override fun unwrap(): Any = task
    }

//    private fun wrap(task: Any): VirtualThreadTaskProxy {
//        val ev = env!!
//        val cached = ev.attachment.invokeExact(task) as Any?
//        if (cached is TaskAdapter) return cached
//        val a: Any = TaskAdapter(task)                                           // Any로 받아야 디스크립터가 Object
//        ev.attach.invokeExact(task, a) as Any?                                   // CustomVThreadTask.attach = getAndSet(원자적)
//        return a as TaskAdapter
//    }

    // ── JDK → 내 코드: Proxy (빌더 주입 경로는 loadCustomScheduler를 거치지 않아 Proxy 허용) ──

    private class VirtualThreadSchedulerProxyImp(
        val service: EventLoop,
        val env:LoomBranchSupport
    ):  InvocationHandler {

        override fun invoke(
            proxy: Any,
            method: Method,
            args: Array<out Any?>?
        ): Any? {
            when (method.name) {
                "hashCode" -> {

                    return System.identityHashCode(proxy)
                }
                "equals" -> {
                    val other = args?.getOrNull(0)
                    return proxy === other
                }
                "toString" -> {
                    return "VirtualThreadSchedulerProxyImp(${hashCode()})"
                }
                "newThread" -> {

                    return InvocationHandler.invokeDefault(proxy, method, args)
                }
                "schedule" -> {
                    return try {
                        service.schedule(args!![0] as Runnable, args!![1] as Long, args!![2] as TimeUnit)
                    } catch (_: RejectedExecutionException) {
                        InvocationHandler.invokeDefault(proxy, method, args)
                    }
                }
                "onStart" -> {
                    val t = TaskAdapter(args!![0] as Runnable, env.vTask)

                    return service.execute(t)
                }
                "onContinue" -> {
                    val t = TaskAdapter(args!![0] as Runnable, env.vTask)

                    return service.execute(t)
                }
            }
            try {
                return method.invoke(env.vScheduler.default, *(args ?: EmptyArrays.EMPTY_OBJECTS))
            } catch (e: InvocationTargetException) {
                throw e.targetException
            }
        }


        companion object {
//            fun asdf() {
//                Proxy.newProxyInstance(javaClass.classLoader, listOf(env!!.sched), VirtualThreadSchedulerProxyImp(
//
//                ))
//            }
        }


    }

//
//    // ── 주입 결과 핸들 ──
//    class Installed internal constructor(val builder: Thread.Builder.OfVirtual, private val jdkScheduler: Any) {
//        /** 스레드 안전 — Executors.newThreadPerTaskExecutor(factory()) 등에 공유 */
//        fun factory(): ThreadFactory = builder.factory()
//        /** 시작 전 태스크 핸들 → attach(ctx) → thread().start(). 주입 빌더를 고정해 넘기므로 항상 우리 스케줄러를 탄다. */
//        fun newThread(preferredCarrier: Thread?, task: Runnable): VirtualThreadTaskProxy =
//            wrap(env!!.newThread.invokeExact(jdkScheduler, builder, preferredCarrier, task) as Any)
//    }
//
//    /** 빌더 하나에 스케줄러 주입. Builder는 비스레드안전 → 반환값의 factory()를 공유할 것. */
//    fun install(impl: VirtualThreadSchedulerProxy): Installed {
//        check(status == Status.READY) { "VirtualThreadBridge not ready: $status" }
//        val b = Thread.ofVirtual()
//        val s = jdkScheduler(impl)
//        env!!.schedField.set(b, s)                                               // VirtualThreadBuilder.scheduler ← Proxy
//        return Installed(b, s)
//    }

}