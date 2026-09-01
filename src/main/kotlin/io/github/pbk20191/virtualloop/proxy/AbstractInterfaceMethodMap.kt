package io.github.pbk20191.virtualloop.proxy

import java.lang.reflect.Method

/**
 * Per-declaringClass map from "the Method the proxy may pass" to the CANONICAL [clazz]-declared
 * Method (safe to invoke on a wrapper that implements [clazz]).
 *
 * COVARIANCE-AWARE: matching scans [Class.getMethods] for every method with the canonical
 * name+parameterTypes instead of the single most-specific `getMethod` hit. A sub-interface that
 * covariantly NARROWS a return type compiles to TWO methods - the specific one and a BRIDGE with
 * the original return type - and the proxy passes the bridge when invoked through the supertype
 * view; keying only getMethod's most-specific pick would silently miss it (the same
 * bypass-by-view bug class as IoHandleProxyDispatchTest's cases). Parameter types stay an exact
 * match: overrides cannot change them, so foreign overloads never map. Known limit: generic
 * SPECIALIZATION bridges (erased vs specialized parameter types) are not resolved - fine for
 * non-generic canonical interfaces like IoHandle/IoHandler.
 *
 * Kotlin-variance note: `Class<out T>` keeps the type parameter itself covariant-friendly
 * (Class is invariant in Java, so `Class<T>` would pin it).
 */
internal abstract class AbstractInterfaceMethodMap<T> : ClassValue<Map<Method, Method>>() {

    abstract val clazz: Class<out T>

    override fun computeValue(type: Class<*>): Map<Method, Method> {
        val canonical = if (clazz.isInterface) clazz.methods.toList() else clazz.interfaces.flatMap { it.methods.toList() }
        val map = HashMap<Method, Method>()
        for (candidate in type.methods) {
            val match = canonical.firstOrNull {
                it.name == candidate.name && it.parameterTypes.contentEquals(candidate.parameterTypes)
            } ?: continue
            map[candidate] = match
        }
        return map
    }
}
