package io.github.pbk20191.virtualloop

import java.lang.invoke.MethodHandles

/**
 * A trusted [MethodHandles.Lookup] provider with TWO strategies, tried in order:
 *
 *  1. **Opened module** - `MethodHandles.privateLookupIn(Thread.class, lookup())` succeeds when
 *     the JVM was started with `--add-opens=java.base/java.lang=ALL-UNNAMED`. The supported,
 *     spec-clean path; per-target lookups are minted with privateLookupIn so PRIVATE access is
 *     never lost to a teleport ([MethodHandles.Lookup.in] drops PRIVATE across classes).
 *  2. **sun.misc.Unsafe** - grab `MethodHandles.Lookup.IMPL_LOOKUP` (the trusted lookup) via
 *     staticFieldBase/staticFieldOffset. Works with NO JVM flags, but rides APIs deprecated for
 *     removal; when the JDK finally drops them this strategy dies and only (1) remains.
 *
 * Both failing leaves [isSupported] false with the causes retained - never an
 * ExceptionInInitializerError (an eager `data object` init would poison every consumer).
 */
internal object LookupUnsafe {

    private val opened: MethodHandles.Lookup?
    private val trusted: MethodHandles.Lookup?

    /** Which strategy is active: "add-opens", "unsafe", or "none". */
    val strategy: String

    val failure: Throwable?

    init {
        var openedLookup: MethodHandles.Lookup? = null
        var trustedLookup: MethodHandles.Lookup? = null
        var openFailure: Throwable? = null
        var unsafeFailure: Throwable? = null

        try {
            // Probe: java.lang is representative of the java.base packages we need opened.
            openedLookup = MethodHandles.privateLookupIn(Thread::class.java, MethodHandles.lookup())
        } catch (t: Throwable) {
            openFailure = t
        }
        if (openedLookup == null) {
            try {
                @Suppress("DEPRECATION")
                val unsafe = sun.misc.Unsafe::class.java.getDeclaredField("theUnsafe")
                    .apply { isAccessible = true } // sun.misc is opened by jdk.unsupported: no flags needed
                    .get(null) as sun.misc.Unsafe
                val implLookup = MethodHandles.Lookup::class.java.getDeclaredField("IMPL_LOOKUP")
                // staticFieldBase, NOT the Class object: passing the class as the base only works
                // by HotSpot coincidence; the Unsafe contract routes statics through the base.
                @Suppress("DEPRECATION")
                trustedLookup = unsafe.getObject(
                    unsafe.staticFieldBase(implLookup),
                    unsafe.staticFieldOffset(implLookup),
                ) as MethodHandles.Lookup
            } catch (t: Throwable) {
                unsafeFailure = t
            }
        }

        opened = openedLookup
        trusted = trustedLookup
        strategy = when {
            openedLookup != null -> "add-opens"
            trustedLookup != null -> "unsafe"
            else -> "none"
        }
        failure = if (openedLookup == null && trustedLookup == null) {
            (unsafeFailure ?: openFailure)?.also { primary ->
                openFailure?.takeIf { it !== primary }?.let(primary::addSuppressed)
            }
        } else {
            null
        }
    }

    val isSupported: Boolean get() = opened != null || trusted != null

    /**
     * A lookup with PRIVATE access in [target]. The IMPL_LOOKUP keeps its trust across [in];
     * the opened-module lookup must be re-minted per target class instead.
     */
    fun lookupIn(target: Class<*>): MethodHandles.Lookup {
        trusted?.let { return it.`in`(target) }
        opened?.let { return MethodHandles.privateLookupIn(target, MethodHandles.lookup()) }
        throw IllegalStateException(
            "No trusted lookup: start the JVM with --add-opens=java.base/java.lang=ALL-UNNAMED " +
                "or allow sun.misc.Unsafe (jdk.unsupported)",
            failure,
        )
    }
}
