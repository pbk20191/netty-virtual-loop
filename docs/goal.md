──────────────────────────────────────────────────────────────────────────────────────────────────────────────
https://github.com/franz1981/Netty-VirtualThread-Scheduler

https://github.com/micronaut-projects/micronaut-core
-> micronaut-core/http-netty/src/main/java/io/micronaut/http/netty/channel/loom

two project is sample target for our goal I'm writing my goal

my goal is two create VirtualIoEventLoopGroup which is internally backed by
MultiThreadedIoEventLoopGroup backed by java NIO

VirtualIOEventLoopGroup has VirtualIOEventLoop which is backed by normal children of
MultiThreadedIOEventLoopGroup's children one by one

however every task executed by VirtualIOEventLoop must be executed by starting fresh VirtualThread

so which means each VirtualIOEventLoop has a ThreadPerTaskExecutor which is created by VirtualThreadFactory and that 
VirtualThreadFactory's executor property is mapped to SingleThreadIOEventLoop which is children of MultiThreadedIOEventLoopGRoup


also any IOHandle registered to this VirtualIOEventLoop should not see the actual SingleThreadIOEventLoop

SingleThreadIoEventLoop.ioHandler is not the good way to go I think?.. I think we need have Our own delegated IoHandler for VirtualThreadIOEventLoop
->
maybe we just implement our VirtualThreadIOHandler as an IOHandle for carrier IOHandle and bridge back and forth?..


-> target jvm is 25 and platform is linux or mac 
-> so Selector.java calls in VirtualThread is mapped to Virtual Threads system Poller so no blocking happens
-> jvm monitor is fully supported for this runtime for virtual Thread so synchonization block is safe for virtual Thread