package io.github.pbk20191.virtualloop.proxy

import java.lang.reflect.InvocationHandler
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method

// Shared dynamic-proxy infrastructure: the covariance-aware canonical-method map and the general
// InvocationHandler built on it. Used by IoHandleProxy (per-registration drain wrapper) and
// IOHandlerProxy (IoHandler wrapper).


/**
 * General canonical-translating [InvocationHandler]: methods of the canonical interface route to
 * THIS wrapper (which must implement `T` - the canonical Method makes that reflective call
 * legal regardless of which interface view the proxy resolved the incoming Method from, see
 * [AbstractInterfaceMethodMap]); everything else goes straight to [delegate]. Object identity
 * methods are answered by the proxy itself so it never masquerades as the raw delegate in maps.
 *
 * [InvocationTargetException] is unwrapped: without it, exceptions thrown by the delegate (or
 * the wrapper) would surface to proxy callers as UndeclaredThrowableException instead of the
 * original throwable.
 */
internal abstract class CanonicalInvocationHandler<T : Any>(
    @JvmField
    protected val delegate: T,
) : InvocationHandler {

    /** The canonical-method map for the interface this wrapper implements. */
    protected abstract val methodMap: AbstractInterfaceMethodMap<T>

    override fun invoke(proxy: Any, method: Method, args: Array<out Any?>?): Any? {
        if (method.declaringClass === Any::class.java) {
            return when (method.name) {
                "hashCode" -> System.identityHashCode(proxy)
                "equals" -> proxy === args!![0]
                else -> "${javaClass.simpleName}($delegate)"
            }
        }
        val canonical = methodMap.get(method.declaringClass)[method]
        var resolved: Method = method
        var target:Any = delegate
        if (canonical != null) {
            resolved = canonical
            target = this
        }
        // The InvocationHandler contract passes args == null (not an empty array) for no-arg
        // methods; a non-null array is required for the spread below.
        return try {
            if (args == null) {
                resolved.invoke(target)
            } else {
                resolved.invoke(target, *args)
            }
        } catch (e: InvocationTargetException) {
            throw e.targetException
        }
    }

}
