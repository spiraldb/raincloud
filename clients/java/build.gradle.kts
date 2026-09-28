// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
plugins { `java-library`; distribution }
repositories { mavenCentral() }
group = "dev.raincloud"
// Derived, not declared: the release version has one authority, and a stale
// copy here would ship a jar whose name disagrees with the wheel beside it.
version = rootDir.resolve("../../raincloud/__init__.py").readLines()
    .firstNotNullOfOrNull { Regex("""^__version__ = "(.+)"$""").find(it)?.groupValues?.get(1) }
    ?: error("no __version__ in raincloud/__init__.py")
java { toolchain { languageVersion.set(JavaLanguageVersion.of(17)) }; withSourcesJar() }
// This library's own pins, deliberately not read from sidecars/java/gradle.properties:
// Arrow here is the client's public API (vectors imported over the C stream), while the
// sidecars' Arrow must match the one vortex-jni ships against. The two may move apart.
val arrowVersion = "19.0.0"
dependencies {
    api("org.apache.arrow:arrow-vector:$arrowVersion")
    implementation("org.apache.arrow:arrow-c-data:$arrowVersion")
    implementation("net.java.dev.jna:jna:5.17.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.21.0")
    runtimeOnly("org.apache.arrow:arrow-memory-netty:$arrowVersion")
    testImplementation(platform("org.junit:junit-bom:5.10.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.slf4j:slf4j-nop:2.0.18")
}
tasks.test {
    useJUnitPlatform()
    testLogging { showStandardStreams = true }
    jvmArgs("--add-opens=java.base/java.nio=ALL-UNNAMED")
    systemProperty("jna.library.path", System.getProperty("raincloud.native.path", ""))
    systemProperty("raincloud.fixture", System.getProperty("raincloud.fixture", ""))
}

// A local install carries the runtime classpath; no Maven publication required.
val nativeLibrary = providers.gradleProperty("raincloudNativeLibrary")
distributions {
    main {
        contents {
            into("lib") {
                from(tasks.jar)
                from(configurations.runtimeClasspath)
            }
            into("native") {
                if (nativeLibrary.isPresent) from(file(nativeLibrary.get()))
            }
            from("../../LICENSE")
        }
    }
}
