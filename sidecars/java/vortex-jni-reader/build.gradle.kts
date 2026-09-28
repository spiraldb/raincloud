// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
//
// `vortex@jni` READ + WRITE conformance lanes via the published Vortex JNI bindings. ONE
// subproject, TWO launch scripts sharing one lib/: raincloud-read-vortex-jni (read a
// .vortex artifact, compare LOGICALLY to the canonical) and raincloud-export-vortex-jni
// (write the canonical through vortex-jni's VortexWriter, then self-verify by reading it
// back through the reader). `installDist` produces both under
// build/install/raincloud-read-vortex-jni/bin/, which RAINCLOUD_READER_VORTEX_JNI and
// RAINCLOUD_SIDECAR_VORTEX_JNI point at.
plugins {
    application
}

repositories {
    mavenCentral()
}

val arrowVersion: String by project
val vortexJniVersion: String by project
val junitVersion: String by project
val slf4jVersion: String by project

dependencies {
    implementation(project(":conformance-common"))

    // Published Vortex JNI bindings (native lib bundled in the jar). vortex-jni pins
    // its own Arrow; arrowVersion in gradle.properties must match it.
    implementation("dev.vortex:vortex-jni:$vortexJniVersion")
    // Arrow C Data Interface (the vortex-jni <-> arrow-java handoff, both directions).
    implementation("org.apache.arrow:arrow-c-data:$arrowVersion")

    // Off-heap allocator impl + silence Arrow's SLF4J "no providers" warning.
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
    // Primary entry point: the READ lane (this subproject's first lane).
    mainClass.set("dev.raincloud.sidecar.vortexjni.ConformanceReader")
    applicationName = "raincloud-read-vortex-jni"
    applicationDefaultJvmArgs = sidecarJvmArgs
}

// Second entry point: the WRITE lane, added into the same distribution's bin/.
val writerStartScripts = tasks.register<CreateStartScripts>("writerStartScripts") {
    dependsOn(tasks.named("jar"))
    mainClass.set("dev.raincloud.sidecar.vortexjni.ConformanceWriter")
    applicationName = "raincloud-export-vortex-jni"
    outputDir = layout.buildDirectory.dir("scriptsWriter").get().asFile
    classpath = files(tasks.named("jar")) + configurations.runtimeClasspath.get()
    defaultJvmOpts = sidecarJvmArgs
}

distributions {
    named("main") {
        contents {
            from(writerStartScripts) { into("bin") }
        }
    }
}
