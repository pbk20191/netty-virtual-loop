package io.github.pbk20191.virtualloop

import kotlin.test.Test

import io.netty.channel.MultiThreadIoEventLoopGroup
import io.netty.channel.nio.NioIoHandler
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Verifies VirtualIoEventLoopGroup's carrier-ownership modes:
 * - consume (default): shutting the virtual group down also shuts the carrier group down.
 * - guest: shutting the virtual group down leaves the carrier running and usable.
 */
class OwnershipTest {
    @Test
    fun consumeAndGuestCarrierLifecycles() {
        check(PrivateLoomSupport.isSupported)

        // --- consume mode (default) ---
        run {
            val carrier = MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory())
            val vgroup = VirtualIoEventLoopGroup(carrier)
            val ran = CountDownLatch(1)
            vgroup.next().execute { ran.countDown() }
            check(ran.await(5, TimeUnit.SECONDS))

            // Children quiesce first; the carrier is shut only once the virtual group has
            // terminated - so await the group future, then the carrier must be going down.
            check(vgroup.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(10, TimeUnit.SECONDS)) {
                "consume mode: virtual group did not terminate"
            }
            // The carrier-shutdown listener fires asynchronously once the group future completes,
            // so don't sample isShuttingDown immediately - await the carrier's termination instead.
            check(carrier.terminationFuture().await(15, TimeUnit.SECONDS)) { "consume mode: carrier did not terminate" }
            println("consume mode: carrier shut down after virtual group  -> OK (isShuttingDown=${carrier.isShuttingDown}, terminated=${carrier.isTerminated})")
        }

        // --- guest mode ---
        run {
            val carrier = MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory())
            val vgroup = VirtualIoEventLoopGroup(carrier, ownsCarrier = false)
            val ran = CountDownLatch(1)
            vgroup.next().execute { ran.countDown() }
            check(ran.await(5, TimeUnit.SECONDS))

            check(vgroup.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(10, TimeUnit.SECONDS)) {
                "guest mode: virtual group did not terminate"
            }
            check(!carrier.isShuttingDown) { "guest mode: carrier must NOT be shut down" }

            // the carrier must still be fully usable after the guest's shutdown
            val stillWorks = CountDownLatch(1)
            carrier.next().execute { stillWorks.countDown() }
            check(stillWorks.await(5, TimeUnit.SECONDS)) { "guest mode: carrier no longer usable" }
            println("guest mode  : carrier survived virtual group shutdown -> OK (isShuttingDown=${carrier.isShuttingDown}, still executes tasks)")

            carrier.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(15, TimeUnit.SECONDS)
        }

        println("RESULT: both carrier-ownership modes behave as specified.")
    }
}
