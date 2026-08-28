package io.github.pbk20191.virtualloop.proxy

import io.netty.channel.*
import io.netty.util.concurrent.ThreadAwareExecutor
import java.lang.reflect.Proxy

internal class IoHandlerProxyFactory(
    private val delegate: IoHandlerFactory,
): IoHandlerFactory {

    private val classCache = InterfaceCache()



    override fun newHandler(
        ioExecutor: ThreadAwareExecutor
    ): IoHandler {
        val executorInvocation = ThreadAwareExecutorProxy(ioExecutor)
        val proxyExecutor = Proxy.newProxyInstance(ioExecutor.javaClass.classLoader, classCache.get(ioExecutor.javaClass), executorInvocation)
        val inner = delegate.newHandler(proxyExecutor as ThreadAwareExecutor)
        val iArray = classCache.get(inner.javaClass)
        val invoker = IoHandlerInvocation(inner, executorInvocation)
        val proxy = Proxy.newProxyInstance(inner.javaClass.classLoader, iArray, invoker)
        return proxy as IoHandler
    }


}


