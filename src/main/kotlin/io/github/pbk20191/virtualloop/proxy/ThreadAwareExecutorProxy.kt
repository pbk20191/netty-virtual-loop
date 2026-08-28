package io.github.pbk20191.virtualloop.proxy

import io.netty.util.concurrent.ThreadAwareExecutor


internal class ThreadAwareExecutorProxy(delegate: ThreadAwareExecutor): CanonicalInvocationHandler<ThreadAwareExecutor>(delegate), ThreadAwareExecutor {

    override val methodMap: AbstractInterfaceMethodMap<ThreadAwareExecutor> get() = ClassCache

    override fun isExecutorThread(thread: Thread): Boolean {
        return delegate.isExecutorThread(thread)
    }

    override fun execute(command: Runnable) {
        return delegate.execute(command)
    }



    internal object ClassCache: AbstractInterfaceMethodMap<ThreadAwareExecutor>() {
        override val clazz: Class<out ThreadAwareExecutor> = ThreadAwareExecutor::class.java
    }


}