package io.github.pbk20191.virtualloop

import io.netty.util.concurrent.Future
import java.util.concurrent.RunnableFuture

interface RunnableNettyFuture<V>: RunnableFuture<V>, Future<V>