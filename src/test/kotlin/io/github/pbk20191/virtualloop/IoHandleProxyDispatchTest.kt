package io.github.pbk20191.virtualloop

import io.netty.channel.IoEvent
import io.netty.channel.IoHandle
import io.netty.channel.IoRegistration
import java.lang.reflect.Proxy
import java.util.concurrent.LinkedTransferQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test

/**
 * Pins the proxy dispatch rule of [DelegatedHandle]: lifecycle methods must route through the
 * WRAPPER (serial queue + closed/cancelled flags) even when the concrete handle implements a
 * SUB-interface that REDECLARES them. The JDK hands the InvocationHandler a [java.lang.reflect.Method]
 * resolved against the proxy's interface list - its declaringClass can be the sub-interface, so
 * Method-set equality (which includes declaringClass) silently misroutes; matching must be by
 * name+arity. This test both asserts the behavioral contract and documents what the JDK passes.
 */
class IoHandleProxyDispatchTest {

    /** A transport-style sub-interface that REDECLARES lifecycle methods. */
    interface RedeclaringHandle : IoHandle {
        override fun close()
        override fun handle(registration: IoRegistration, ioEvent: IoEvent)
    }

    /** An unrelated mixin whose handle() OVERLOAD must NOT be captured by the wrapper. */
    interface OverloadMixin {
        fun handle(a: String, b: String): String
    }

    // Closeable FIRST: the proxy passes the FOREMOST interface's Method, so close() arrives with
    // declaringClass=Closeable - pinning that declaringClass-based gating in either direction
    // would lose the lifetime flag.
    private class FakeHandle : java.io.Closeable, RedeclaringHandle, OverloadMixin {
        val directClose = AtomicBoolean()
        val directHandle = AtomicBoolean()
        val overloadCalled = AtomicBoolean()
        override fun close() { directClose.set(true) }
        override fun handle(registration: IoRegistration, ioEvent: IoEvent) { directHandle.set(true) }
        override fun handle(a: String, b: String): String { overloadCalled.set(true); return a + b }
    }

    private object FakeRegistration : IoRegistration {
        override fun <T> attachment(): T = throw UnsupportedOperationException()
        override fun submit(ops: io.netty.channel.IoOps): Long = 0
        override fun isValid(): Boolean = true
        override fun cancel(): Boolean = true
    }

    @Test
    fun lifecycleMethodsRouteThroughWrapperEvenWhenRedeclared() {
        val fake = FakeHandle()
        val queue = LinkedTransferQueue<Runnable>()
        val delegated = DelegatedHandle(fake, queue, AtomicReference())
        val interfaces = InterfaceCache().get(fake.javaClass)
        val proxy = Proxy.newProxyInstance(fake.javaClass.classLoader, interfaces, delegated) as IoHandle

        println("proxy interfaces: ${interfaces.map { it.simpleName }}")

        // Behavioral contract, independent of the above: the wrapper must intercept.
        proxy.handle(FakeRegistration, object : IoEvent {})
        check(!fake.directHandle.get()) { "handle() bypassed the wrapper (went straight to actual)" }
        check(queue.isNotEmpty()) { "handle() was not serialized onto the drain queue" }

        // An unrelated arity-2 handle() OVERLOAD must go straight to actual, not be cast-crashed
        // by the wrapper's handle(IoRegistration, IoEvent).
        val asMixin = proxy as OverloadMixin
        check(asMixin.handle("a", "b") == "ab") { "overloaded handle() misrouted" }
        check(fake.overloadCalled.get()) { "overloaded handle() did not reach actual" }

        // close() through the Closeable interface view (declaringClass = Closeable, a SUPER-type
        // direction mismatch) must still set the lifetime flag.
        (proxy as java.io.Closeable).close()
        check(delegated.closed) { "close() via Closeable view bypassed the wrapper: lifetime signal lost" }

        println("RESULT: redeclared/overloaded/foreign-view lifecycle dispatch all route correctly.")
    }
}
