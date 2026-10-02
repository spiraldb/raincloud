// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
//
// `avro@java` WRITE + READ conformance lanes via Arrow Java's own Avro adapter
// (org.apache.arrow:arrow-avro, the same arrowVersion as every JVM lane) over Apache
// Avro's Java implementation. ONE subproject, TWO launch scripts sharing one lib/:
// raincloud-export-avro-java (write the canonical as an Avro object container file,
// then self-verify by reading it back) and raincloud-read-avro-java (read an Avro file,
// compare LOGICALLY to the canonical).
plugins {
    application
}

repositories {
    mavenCentral()
}

val arrowVersion: String by project
val junitVersion: String by project
val slf4jVersion: String by project
val zstdJniVersion: String by project

dependencies {
    implementation(project(":conformance-common"))

    // Arrow Java's Avro adapter; it brings Apache Avro (1.12.1 with Arrow 19.0.0).
    implementation("org.apache.arrow:arrow-avro:$arrowVersion")
    // Avro's zstandard codec, which Avro declares optional.
    runtimeOnly("com.github.luben:zstd-jni:$zstdJniVersion")

    // Off-heap allocator impl + silence SLF4J.
    runtimeOnly("org.apache.arrow:arrow-memory-netty:$arrowVersion")
    runtimeOnly("org.slf4j:slf4j-nop:$slf4jVersion")

    testImplementation(platform("org.junit:junit-bom:$junitVersion"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

java {
    toolchain { languageVersion.set(JavaLanguageVersion.of(17)) }
}

val sidecarJvmArgs = listOf(
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--enable-native-access=ALL-UNNAMED",
)

tasks.test {
    useJUnitPlatform()
    jvmArgs(sidecarJvmArgs)
}

application {
    mainClass.set("dev.raincloud.sidecar.avrojava.ConformanceWriter")
    applicationName = "raincloud-export-avro-java"
    applicationDefaultJvmArgs = sidecarJvmArgs
}

// Second entry point: the READ lane, added into the same distribution's bin/.
val readerStartScripts = tasks.register<CreateStartScripts>("readerStartScripts") {
    dependsOn(tasks.named("jar"))
    mainClass.set("dev.raincloud.sidecar.avrojava.ConformanceReader")
    applicationName = "raincloud-read-avro-java"
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
