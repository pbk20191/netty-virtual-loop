plugins {
    kotlin("jvm") version "2.4.0"
}

group = "io.github.pbk20191"
version = "0.1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))
    // Source: https://mvnrepository.com/artifact/io.netty/netty-all
    implementation("io.netty:netty-all:4.2.16.Final")
}

kotlin {
    jvmToolchain(25)
    sourceSets {
        getByName("main") {
            resources.srcDirs("resources")
        }

    }

}

tasks.test {
    useJUnitPlatform()
    jvmArgs("--add-opens=java.base/java.lang=ALL-UNNAMED")
    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = true
    }
}

// Custom virtual-thread schedulers require reflective access to java.base internals
// (ThreadBuilders$VirtualThreadBuilder.scheduler, VirtualThread.scheduler/carrierThread).
tasks.withType<JavaExec>().configureEach {
    jvmArgs("--add-opens=java.base/java.lang=ALL-UNNAMED")
}

tasks.register<JavaExec>("runPingPong") {
    group = "application"
    description = "Ping-pong latency bench for the per-IO-event dispatch path."
    mainClass.set("io.github.pbk20191.virtualloop.bench.PingPongBench")
    classpath = sourceSets["main"].runtimeClasspath
}

tasks.register<JavaExec>("runPingPongVanilla") {
    group = "application"
    description = "Ping-pong latency bench on vanilla Netty (reference)."
    mainClass.set("io.github.pbk20191.virtualloop.bench.PingPongBench")
    args("vanilla")
    classpath = sourceSets["main"].runtimeClasspath
}

tasks.register<JavaExec>("runHandoff") {
    group = "application"
    description = "Same-carrier handoff micro-bench (spawn child task + await completion)."
    mainClass.set("io.github.pbk20191.virtualloop.bench.HandoffBench")
    classpath = sourceSets["main"].runtimeClasspath
}

tasks.register<JavaExec>("runBench") {
    group = "application"
    description = "Micro-bench for the inEventLoop() disguise overhead."
    mainClass.set("io.github.pbk20191.virtualloop.bench.DisguiseBench")
    classpath = sourceSets["main"].runtimeClasspath
}

