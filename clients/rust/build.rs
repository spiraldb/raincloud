// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
//! Give the C library an install-independent name. Without a SONAME a linker
//! records the absolute path it linked against in each consumer, and moving
//! the installation breaks them.
fn main() {
    match std::env::var("CARGO_CFG_TARGET_OS").as_deref() {
        Ok("linux" | "android" | "freebsd" | "netbsd" | "openbsd" | "dragonfly") => {
            println!("cargo:rustc-cdylib-link-arg=-Wl,-soname,libraincloud_reader.so")
        }
        Ok("macos" | "ios") => {
            println!(
                "cargo:rustc-cdylib-link-arg=-Wl,-install_name,@rpath/libraincloud_reader.dylib"
            )
        }
        _ => {}
    }
}
