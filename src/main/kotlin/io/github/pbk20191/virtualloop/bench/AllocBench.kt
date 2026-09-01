package io.github.pbk20191.virtualloop.bench

import io.github.pbk20191.virtualloop.VirtualIoEventLoopGroup
import io.netty.buffer.AdaptiveByteBufAllocator
import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.PooledByteBufAllocator
import io.netty.buffer.UnpooledByteBufAllocator
import io.netty.channel.MultiThreadIoEventLoopGroup
import io.netty.channel.nio.NioIoHandler
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Measures the allocator cost profile this architecture actually produces: many virtual threads
 * (one per IoHandle/registration) sharing ONE carrier, none of them FastThreadLocalThreads - so
 * PooledByteBufAllocator's PoolThreadCache never attaches and every alloc/free takes the arena
 * path, while stock Netty's single FTL loop thread aggregates hundreds of channels into one hot
 * thread cache. This bench quantifies that gap to decide whether a carrier-local buffer pool
 * (CarrierLocal) is worth building.
 *
 * Modes per allocator:
 *  - virtual : K long-lived VTs on one VirtualIoEventLoop, each doing M alloc/write/release
 *              cycles (mixed 256B/8KiB, direct) - mimics K drain VTs of K busy channels.
 *  - vanilla : the same total work on a stock single-thread Netty loop (FastThreadLocalThread,
 *              thread cache attaches) - what the pooling infrastructure was designed for.
 */
object AllocBench {

    private const val WORKERS = 8
    private const val CYCLES = 150_000
    private const val WARMUP = 30_000

    @JvmStatic
    fun main(args: Array<String>) {
        val allocators = linkedMapOf<String, ByteBufAllocator>(
            "adaptive (4.2 default)" to AdaptiveByteBufAllocator(),
            "pooled, no thread cache" to PooledByteBufAllocator(
                true,
                PooledByteBufAllocator.defaultNumHeapArena(),
                PooledByteBufAllocator.defaultNumDirectArena(),
                PooledByteBufAllocator.defaultPageSize(),
                PooledByteBufAllocator.defaultMaxOrder(),
                PooledByteBufAllocator.defaultSmallCacheSize(),
                PooledByteBufAllocator.defaultNormalCacheSize(),
                false,
            ),
            "pooled, cacheForAllThreads" to PooledByteBufAllocator(
                true,
                PooledByteBufAllocator.defaultNumHeapArena(),
                PooledByteBufAllocator.defaultNumDirectArena(),
                PooledByteBufAllocator.defaultPageSize(),
                PooledByteBufAllocator.defaultMaxOrder(),
                PooledByteBufAllocator.defaultSmallCacheSize(),
                PooledByteBufAllocator.defaultNormalCacheSize(),
                true,
            ),
            "unpooled (floor)" to UnpooledByteBufAllocator(true),
        )

        val virtualGroup = VirtualIoEventLoopGroup(nThreads = 0)
        val sharedGroup = VirtualIoEventLoopGroup(nThreads = 0, sharedFastThreadLocals = true)
        val vanillaGroup = MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory())
        try {
            val virtualLoop = virtualGroup.next()
            val sharedLoop = sharedGroup.next()
            val vanillaLoop = vanillaGroup.next()
            System.out.printf("%-28s %14s %14s %14s%n", "allocator", "virtual ns/op", "sharedFTL ns/op", "vanilla ns/op")
            for ((name, alloc) in allocators) {
                val virtualNs = onExecutor(alloc) { work -> // K parallel VTs, one carrier
                    val done = CountDownLatch(WORKERS)
                    repeat(WORKERS) { virtualLoop.execute { work(); done.countDown() } }
                    check(done.await(120, TimeUnit.SECONDS)) { "virtual mode timed out" }
                }
                val sharedNs = onExecutor(alloc) { work -> // K VTs sharing ONE InternalThreadLocalMap
                    val done = CountDownLatch(WORKERS)
                    repeat(WORKERS) { sharedLoop.execute { work(); done.countDown() } }
                    check(done.await(120, TimeUnit.SECONDS)) { "shared mode timed out" }
                }
                val vanillaNs = onExecutor(alloc) { work -> // same total work, one FTL thread
                    val done = CountDownLatch(WORKERS)
                    repeat(WORKERS) { vanillaLoop.execute { work(); done.countDown() } }
                    check(done.await(120, TimeUnit.SECONDS)) { "vanilla mode timed out" }
                }
                System.out.printf("%-28s %11.1f ns %14.1f ns %11.1f ns%n", name, virtualNs, sharedNs, vanillaNs)
            }
        } finally {
            virtualGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS)
            sharedGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS)
            vanillaGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS)
        }
    }

    /** Runs warmup + timed cycles through [submit] and returns ns per alloc/release cycle. */
    private inline fun onExecutor(
        alloc: ByteBufAllocator,
        submit: (work: () -> Unit) -> Unit,
    ): Double {
        submit { cycles(alloc, WARMUP) }
        val t0 = System.nanoTime()
        submit { cycles(alloc, CYCLES) }
        val elapsed = System.nanoTime() - t0
        return elapsed.toDouble() / (WORKERS.toLong() * CYCLES)
    }

    private fun cycles(alloc: ByteBufAllocator, count: Int) {
        var sink = 0L
        repeat(count) { i ->
            val size = if (i and 1 == 0) 256 else 8192
            val buf = alloc.directBuffer(size)
            buf.writeLong(i.toLong())
            sink += buf.getLong(0)
            buf.release()
        }
        if (sink == Long.MIN_VALUE) println("impossible $sink") // keep the work observable
    }
}
