package io.github.pbk20191.virtualloop

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.Test

/**
 * [VirtualThreadBranchingSupport] bridges the JDK's OFFICIAL `Thread.VirtualThreadScheduler` API,
 * which does not exist on any JDK this project can run on yet (25 GA and 27-ea both lack it). So
 * the contract to pin HERE is the fail-closed one: on such a JDK the object initializes cleanly to
 * a non-READY status (no ExceptionInInitializerError), reports it, and refuses install(). The
 * READY path is exercised only on a future loom build.
 */
class VirtualThreadBranchingSupportTest {
    @Test
    fun dormantAndFailClosedWhenApiAbsent() {
        val status = VirtualThreadBranchingSupport.status
        val supported = VirtualThreadBranchingSupport.isSupported
        println("branching status   : $status (supported=$supported)")
        println("branching failure  : ${VirtualThreadBranchingSupport.failure?.javaClass?.simpleName}")

        // The object must have initialized without throwing (reaching here proves that), and status
        // must be one of the defined states.
        check(status in VirtualThreadBranchingSupport.Status.entries) { "unexpected status $status" }
        check(supported == (status == VirtualThreadBranchingSupport.Status.READY)) {
            "isSupported must track READY"
        }

        if (!supported) {
            // API absent on this JDK: install() must refuse rather than NPE or half-install.
            val svc = Executors.newSingleThreadScheduledExecutor()
            try {
                val ex = runCatching { VirtualThreadBranchingSupport.install(svc) }.exceptionOrNull()
                check(ex is IllegalArgumentException || ex is IllegalStateException) {
                    "install() on a non-READY bridge must fail fast, got: $ex"
                }
                println("install() refused  : ${ex!!.message}")
            } finally {
                svc.shutdownNow()
            }
            check(VirtualThreadBranchingSupport.Status.API_ABSENT in VirtualThreadBranchingSupport.Status.entries)
            println("RESULT: bridge is dormant and fail-closed on this JDK (no official API).")
        } else {
            // Future loom build: a real install must carry a virtual thread and run its task.
            val svc = Executors.newSingleThreadScheduledExecutor()
            try {
                val factory = VirtualThreadBranchingSupport.install(svc).factory()
                val ran = java.util.concurrent.CountDownLatch(1)
                factory.newThread { ran.countDown() }.start()
                check(ran.await(5, TimeUnit.SECONDS)) { "installed scheduler never ran its virtual thread" }
                println("RESULT: bridge READY - installed scheduler carried a virtual thread.")
            } finally {
                svc.shutdownNow()
            }
        }
    }
}
