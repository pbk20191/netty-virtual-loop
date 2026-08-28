package io.github.pbk20191.virtualloop.proxy

import io.netty.channel.IoOps
import io.netty.channel.IoRegistration

internal class DelegatedRegistration(
    private val inner: IoRegistration,
    private val handle: DelegatedHandle,
) : IoRegistration {
    override fun <T> attachment(): T = inner.attachment()
    override fun submit(ops: IoOps): Long = inner.submit(ops)
    override fun isValid(): Boolean = inner.isValid
    override fun cancel(): Boolean {
        val cancelled = inner.cancel()
        if (cancelled) {
            handle.markCancelled()
        }
        return cancelled
    }
}
