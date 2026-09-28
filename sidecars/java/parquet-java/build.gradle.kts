// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
//
// `parquet@java` WRITE + READ conformance lanes via Apache parquet-java 1.17.1
// (Iceberg 1.11's pin — the Apache reference encoder). ONE subproject, TWO launch
// scripts: raincloud-export-parquet-java (write) + raincloud-read-parquet-java
// (read), sharing one lib/.
//
// parquet-java cannot consume Arrow directly, so this lane hops
// arrow → parquet-arrow-java → parquet-java: parquet-arrow-java (a git submodule
// consumed as a composite build) is the Hadoop-free Arrow⇆Parquet adapter that
// drives parquet-java's ParquetFileWriter/RecordReader. It replaces the former
// parquet-avro + shaded-hadoop-client dep story — the lane is now Hadoop-free (the
// only org.apache.hadoop.* classes are the empty shims vendored inside
// parquet-arrow-core).
plugins {
    application
}

repositories {
    mavenCentral()
}

val arrowVersion: String by project
val junitVersion: String by project
val slf4jVersion: String by project

dependencies {
    implementation(project(":conformance-common"))

    // Composite-build substituted from ./parquet-arrow-java (see settings.gradle.kts),
    // so the submodule commit is the pin and this version is nominal.
    implementation("dev.spiraldb.parquet.arrow:parquet-arrow-core:0.2.0")

    // Off-heap allocator impl for reading the canonical + silence SLF4J.
    runtimeOnly("org.apache.arrow:arrow-memory-netty:$arrowVersion")
    runtimeOnly("org.slf4j:slf4j-nop:$slf4jVersion")

    // Tests: ParquetArrowIo round trips, the row-group knobs reaching the writer, and
    // ConformanceWriter's reports.
    testImplementation(platform("org.junit:junit-bom:$junitVersion"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.apache.arrow:arrow-memory-netty:$arrowVersion")
    testRuntimeOnly("org.slf4j:slf4j-nop:$slf4jVersion")
}

java {
    toolchain { languageVersion.set(JavaLanguageVersion.of(17)) }
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
    mainClass.set("dev.raincloud.sidecar.parquetjava.ConformanceWriter")
    applicationName = "raincloud-export-parquet-java"
    applicationDefaultJvmArgs = sidecarJvmArgs
}

// Second entry point: the READ lane, added into the same distribution's bin/.
val readerStartScripts = tasks.register<CreateStartScripts>("readerStartScripts") {
    dependsOn(tasks.named("jar"))
    mainClass.set("dev.raincloud.sidecar.parquetjava.ConformanceReader")
    applicationName = "raincloud-read-parquet-java"
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
