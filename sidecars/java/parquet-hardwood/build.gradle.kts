// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
//
// `parquet@hardwood` WRITE + READ conformance lanes via Hardwood (dev.hardwood:hardwood-core,
// pinned as hardwoodVersion in gradle.properties), a Parquet reader and writer with no
// Hadoop, Avro or parquet-java underneath. ONE subproject, TWO launch scripts:
// raincloud-export-parquet-hardwood (write) + raincloud-read-parquet-hardwood (read),
// sharing one lib/.
//
// Hardwood has no Arrow API, so both directions bridge Arrow <-> Hardwood's columnar
// writer/reader here (HardwoodWriter / HardwoodReader); the Parquet bytes are Hardwood's.
//
// Hardwood's jar targets Java 21, so this subproject builds and runs on a Java 21
// toolchain (Gradle's toolchain resolver provisions one when none is installed, which can
// mean a download). Its launchers default to that JDK: see `defaultJavaHome` below.
plugins {
    application
}

repositories {
    mavenCentral()
}

val arrowVersion: String by project
val hardwoodVersion: String by project
val junitVersion: String by project
val slf4jVersion: String by project

dependencies {
    implementation(project(":conformance-common"))

    // Hardwood's BOM pins its optional codec libraries, which a consumer must declare to
    // get them: zstd (this lane writes zstd), and snappy/lz4 so the reader opens Parquet
    // other writers compress that way (pyarrow's default is snappy), and brotli4j, with its
    // native library for the platforms raincloud builds on, since RAINCLOUD_PARQUET_COMPRESSION
    // can ask for Brotli.
    implementation(platform("dev.hardwood:hardwood-bom:$hardwoodVersion"))
    implementation("dev.hardwood:hardwood-core")
    runtimeOnly("com.github.luben:zstd-jni")
    runtimeOnly("org.xerial.snappy:snappy-java")
    runtimeOnly("at.yawk.lz4:lz4-java")
    runtimeOnly("com.aayushatharva.brotli4j:brotli4j")
    for (platform in listOf("linux-x86_64", "linux-aarch64", "osx-x86_64", "osx-aarch64")) {
        runtimeOnly("com.aayushatharva.brotli4j:native-$platform:1.23.0")
    }

    // Off-heap allocator impl for the Arrow side + silence Arrow's SLF4J warning.
    runtimeOnly("org.apache.arrow:arrow-memory-netty:$arrowVersion")
    runtimeOnly("org.slf4j:slf4j-nop:$slf4jVersion")

    testImplementation(platform("org.junit:junit-bom:$junitVersion"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

java {
    toolchain { languageVersion.set(JavaLanguageVersion.of(21)) }
}

tasks.test {
    useJUnitPlatform()
    jvmArgs("--add-opens=java.base/java.nio=ALL-UNNAMED")
}

val sidecarJvmArgs = listOf(
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--enable-native-access=ALL-UNNAMED",
)

application {
    // Primary entry point: the WRITE lane.
    mainClass.set("dev.raincloud.sidecar.hardwood.ConformanceWriter")
    applicationName = "raincloud-export-parquet-hardwood"
    applicationDefaultJvmArgs = sidecarJvmArgs
}

// Second entry point: the READ lane, added into the same distribution's bin/.
val readerStartScripts = tasks.register<CreateStartScripts>("readerStartScripts") {
    dependsOn(tasks.named("jar"))
    mainClass.set("dev.raincloud.sidecar.hardwood.ConformanceReader")
    applicationName = "raincloud-read-parquet-hardwood"
    outputDir = layout.buildDirectory.dir("scriptsReader").get().asFile
    classpath = files(tasks.named("jar")) + configurations.runtimeClasspath.get()
    defaultJvmOpts = sidecarJvmArgs
}

distributions {
    named("main") {
        contents {
            from(readerStartScripts) { into("bin") }
        }
    }
}

// The machine's `java` is often older than 21 (the other JVM lanes run on 17), so the POSIX
// launchers keep JAVA_HOME only when it names Java 21 or newer, and otherwise use the Java 21
// toolchain this build ran on, when that JDK is still installed. A copied install on a
// machine without it needs JAVA_HOME set to a JDK 21+.
val defaultJavaHome = javaToolchains.launcherFor(java.toolchain)
    .map { it.metadata.installationPath.asFile.absolutePath }

tasks.withType<CreateStartScripts>().configureEach {
    inputs.property("defaultJavaHome", defaultJavaHome)
    val home = defaultJavaHome
    doLast {
        val jdk = home.get()
        require(!jdk.contains("'")) { "toolchain path $jdk contains a quote" }
        val marker = "# Determine the Java command to use to start the JVM."
        val script = unixScript
        val text = script.readText()
        require(text.contains(marker)) { "$script: Gradle's start script no longer has \"$marker\"" }
        script.writeText(text.replace(marker, """
            |# raincloud: Hardwood needs Java 21 or newer. Keep JAVA_HOME when it names one;
            |# otherwise use the JDK this launcher was built with, when it is still installed.
            |raincloud_java_major=
            |if [ -n "${'$'}JAVA_HOME" ] && [ -r "${'$'}JAVA_HOME/release" ] ; then
            |    raincloud_java_major=${'$'}( sed -n 's/^JAVA_VERSION="\([0-9]*\).*/\1/p' "${'$'}JAVA_HOME/release" )
            |fi
            |if [ "${'$'}{raincloud_java_major:-0}" -lt 21 ] && [ -x '$jdk/bin/java' ] ; then
            |    JAVA_HOME='$jdk'
            |fi
            |
            |$marker""".trimMargin()))
    }
}
