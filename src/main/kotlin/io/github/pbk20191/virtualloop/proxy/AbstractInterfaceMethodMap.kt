package io.github.pbk20191.virtualloop.proxy

import java.lang.invoke.MethodHandle
import java.lang.invoke.MethodHandles
import java.lang.invoke.MethodType
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
internal abstract class AbstractInterfaceMethodMap<T> : ClassValue<AbstractInterfaceMethodMap.MethodPayload>() {

    abstract val clazz: Class<out T>

    data class MethodPayload(
        val intercepted:Set<Method>,
        val interceptedHandles:Map<Method, MethodHandle>,
        val nativeHandles:Map<Method, MethodHandle>
    )

    override fun computeValue(type: Class<*>): MethodPayload {
        val canonical = canonicalSignatures(clazz)
        val intercepted = mutableSetOf<Method>()
        val map = HashMap<Method, MethodHandle>()
        val nativeHandles = mutableMapOf<Method, MethodHandle>()
        val lookup = MethodHandles.publicLookup()
        for (candidate in type.methods) {
            // Delegate-side handle for EVERY method of this interface view - the handler's
            // fallback path dereferences it unconditionally, so populating only matched
            // candidates would NPE on the first delegate-only method (e.g. selectableChannel()).
            nativeHandles[candidate] = lookup.unreflect(candidate)
                .withVarargs(false)
                .asSpreader(Array<Any>::class.java, candidate.parameterCount)
                .asType(INVOKER_TYPE)
            val match = canonical[SignatureKey(candidate)]
                ?.takeIf { returnCompatible(candidate, it) }
                ?: continue
            intercepted.add(candidate)
            // PRE-ADAPTED to the InvocationHandler's generic shape (receiver, Object[]) -> Object:
            // the adaptation work happens ONCE here, so dispatch is a plain MethodHandle.invoke
            // (~5ns). Never invokeWithArguments on the hot path - it re-derives the adaptation on
            // every call (~64ns, slower than Method.invoke's ~7ns; probed on JDK 25).
            // withVarargs(false) before spreading, as the JDK's own Proxy.defaultMethodHandle
            // does: unreflect gives a varargs-collecting handle for varargs methods, and stacking
            // asSpreader on top of that adaptation misbehaves.
            map[candidate] = lookup.unreflect(match)
                .withVarargs(false)
                .asSpreader(Array<Any>::class.java, match.parameterCount)
                .asType(INVOKER_TYPE)
        }
        return MethodPayload(intercepted.toSet(), map.toMap(), nativeHandles.toMap())
    }

    /** Erased signature (name + parameter types); return type deliberately excluded - see below. */
    private data class SignatureKey(val name: String, val parameterTypes: List<Class<*>>) {
        constructor(m: Method) : this(m.name, m.parameterTypes.asList())
    }

    private companion object {
        /** The generic dispatch shape every cached handle is adapted to. */
        val INVOKER_TYPE: MethodType =
            MethodType.methodType(Any::class.java, Any::class.java, Array<Any>::class.java)

        /**
         * The canonical set = methods invocable on the wrapper, ONE per signature. For an
         * interface, its own getMethods view (includes superinterfaces); for a CLASS the
         * hierarchy walk is [InterfaceCache]'s (shared cache, same public/not-hidden/not-sealed
         * filters as the proxies - direct interfaces alone would miss superclass interfaces).
         *
         * INHERITANCE DUPLICATES ARE COLLAPSED: the same signature reappears once per declaring
         * interface (AutoCloseable.close vs Closeable.close vs a transport redeclaration), and a
         * Method-identity distinct() cannot see that. Each signature keeps one DETERMINISTIC
         * winner - [moreSpecific], mirroring getMethods' own selection rule - instead of
         * whatever the walk order happened to produce.
         */
        fun canonicalSignatures(clazz: Class<*>): Map<SignatureKey, Method> {
            val source = if (clazz.isInterface) {
                clazz.methods.asSequence()
            } else {
                InterfaceCache.get(clazz).asSequence().flatMap { it.methods.asSequence() }
            }
            val winners = LinkedHashMap<SignatureKey, Method>()
            for (method in source) {
                winners.merge(SignatureKey(method), method, ::moreSpecific)
            }
            return winners
        }

        /**
         * getMethods' specificity rule, applied across interface roots: a SUBTYPE's declaration
         * beats its supertype's; between unrelated declarings the NARROWER return wins (the
         * covariant end of a redeclaration chain); otherwise keep the incumbent.
         */
        fun moreSpecific(a: Method, b: Method): Method = when {
            a.declaringClass !== b.declaringClass &&
                b.declaringClass.isAssignableFrom(a.declaringClass) -> a
            a.declaringClass !== b.declaringClass &&
                a.declaringClass.isAssignableFrom(b.declaringClass) -> b
            a.returnType !== b.returnType && b.returnType.isAssignableFrom(a.returnType) -> a
            else -> a
        }

        /**
         * Return-type sanity gate, the precision the name+parameters key alone cannot give:
         * covariant BRIDGES only ever WIDEN the return (bridge Object vs canonical String -
         * assignable), so requiring assignability in either direction keeps every bridge while
         * excluding an unrelated interface's same-name+parameters method with an incompatible
         * return, which would otherwise be misrouted to the wrapper and blow up with a
         * ClassCastException at the caller.
         */
        fun returnCompatible(candidate: Method, canonical: Method): Boolean =
            candidate.returnType.isAssignableFrom(canonical.returnType) ||
                canonical.returnType.isAssignableFrom(candidate.returnType)
    }
}
