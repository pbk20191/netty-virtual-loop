package io.github.pbk20191.virtualloop.proxy

import io.netty.channel.IoHandle
import io.netty.channel.IoHandler
import io.netty.channel.IoHandlerContext
import io.netty.channel.IoRegistration

internal class IoHandlerInvocation(
    actual: IoHandler,
    val executor: ThreadAwareExecutorProxy,
) : CanonicalInvocationHandler<IoHandler>(actual), IoHandler {

    override val methodMap: AbstractInterfaceMethodMap<IoHandler> get() = IoHandlerProxyMethodCache


    override fun initialize() {
        delegate.initialize()
    }

    override fun prepareToDestroy() {
        delegate.prepareToDestroy()
    }

    override fun wakeup() {
        delegate.wakeup()
    }

    override fun destroy() {
        delegate.destroy()
    }

    override fun isCompatible(handleType: Class<out IoHandle>): Boolean {
        return delegate.isCompatible(handleType)
    }

    override fun register(handle: IoHandle): IoRegistration {
        val inner = delegate.register(handle)
        return IoRegistrationWrapper(inner)
    }

    override fun run(context: IoHandlerContext): Int {
        return delegate.run(IoHandlerContextWrapper(context))
    }

    internal data object IoHandlerProxyMethodCache : AbstractInterfaceMethodMap<IoHandler>() {
        override val clazz: Class<IoHandler> = IoHandler::class.java
    }

    internal class IoRegistrationWrapper(private val delegate: IoRegistration): IoRegistration by delegate {

        override fun cancel(): Boolean {
            val t = delegate.cancel()
            return t
        }
    }
    internal class IoHandlerContextWrapper(private val delegate: IoHandlerContext): IoHandlerContext by delegate {
        override fun reportActiveIoTime(activeNanos: Long) {
            delegate.reportActiveIoTime(activeNanos)
        }

        override fun shouldReportActiveIoTime(): Boolean {
            return delegate.shouldReportActiveIoTime()
        }
    }
}
