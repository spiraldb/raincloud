// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
//
// Shared conformance machinery for the JVM sidecar lanes: CLI parse, canonical
// Arrow IPC reader (zstd), the LOGICAL comparator (one copy, never duplicated
// per lane), the reader and writer mains, the row-group knob grammar, and the
// JSON report writers. Depended on by vortex-jni-reader, parquet-java and
// parquet-hardwood.
plugins {
    `java-library`
}

repositories {
    mavenCentral()
}

val arrowVersion: String by project
val junitVersion: String by project
val jacksonVersion: String by project

dependencies {
    // `api` so consumers inherit Arrow on their compile classpath.
    api("org.apache.arrow:arrow-vector:$arrowVersion")
    api("org.apache.arrow:arrow-compression:$arrowVersion")
    api("org.apache.arrow:arrow-memory-core:$arrowVersion")

    // The knob grammar is tested against the shared sidecars/knob_cases.json, hence Jackson.
    testImplementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    testImplementation(platform("org.junit:junit-bom:$junitVersion"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    // Off-heap allocator impl so tests can build real Arrow vectors (e.g. dictionary decoding).
    testRuntimeOnly("org.apache.arrow:arrow-memory-netty:$arrowVersion")
}

java {
    toolchain { languageVersion.set(JavaLanguageVersion.of(17)) }
}

val knobCases = rootDir.resolve("../knob_cases.json")

tasks.test {
    useJUnitPlatform()
    // Arrow's off-heap memory needs java.nio opened on JDK 17.
    jvmArgs("--add-opens=java.base/java.nio=ALL-UNNAMED")
    inputs.file(knobCases)
    systemProperty("raincloud.knobCases", knobCases.absolutePath)
}
