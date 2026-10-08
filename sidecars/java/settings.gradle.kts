// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
//
// Standalone Gradle multi-project for raincloud's JVM conformance sidecars —
// the JVM counterpart of `sidecars/rust/` (its own Cargo workspace). NOT part of any
// parent build. See sidecars/README.md for installation and discovery.
plugins {
    // Uses an installed JDK 17 toolchain, or provisions one if none is found,
    // which can mean a network download of a JDK on first build. 1.0.0 is the
    // first release compatible with Gradle 9 (0.8.0 references a JvmVendorSpec
    // constant Gradle 9 removed and fails with NoSuchFieldError).
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "raincloud-sidecars-java"

include("conformance-common")
include("vortex-jni-reader")
include("parquet-java")
include("parquet-hardwood")
include("avro-java")

// parquet-arrow-java (git submodule) supplies the Hadoop-free Arrow⇆Parquet bridge the
// parquet@java lane hops through (arrow → parquet-arrow-java → parquet-java). Consumed as a
// composite build so `dev.spiraldb.parquet.arrow:parquet-arrow-core` resolves from source — no
// Maven publication, pinned by the submodule commit.
includeBuild("parquet-arrow-java")
