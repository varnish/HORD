//! Build script for hord-core.
//!
//! Compiles the C shim (`csrc/shim.c`) into a static archive and links it
//! together with the system `librdmacm` and `libibverbs`. We invoke `cc`/`ar`
//! directly rather than pulling in the `cc` crate so that the whole workspace
//! has zero third-party build dependencies — handy on an isolated RDMA host.

use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"));
    let obj = out_dir.join("shim.o");
    let archive = out_dir.join("libhordshim.a");

    // Compile the shim. -D_GNU_SOURCE so the rdma-core headers expose
    // everything; -fPIC because it gets linked into a Rust dylib/staticlib.
    let cc = env::var("CC").unwrap_or_else(|_| "cc".to_string());
    run(Command::new(&cc).args([
        "-c",
        "-O2",
        "-fPIC",
        "-Wall",
        "-Wextra",
        "-D_GNU_SOURCE",
        "csrc/shim.c",
        "-o",
    ]).arg(&obj));

    // Bundle the object into a static archive that Cargo will link.
    let ar = env::var("AR").unwrap_or_else(|_| "ar".to_string());
    let _ = std::fs::remove_file(&archive);
    run(Command::new(&ar).arg("crs").arg(&archive).arg(&obj));

    println!("cargo:rustc-link-search=native={}", out_dir.display());
    println!("cargo:rustc-link-lib=static=hordshim");
    // System RDMA libraries (the -dev packages provide the .so symlinks).
    println!("cargo:rustc-link-lib=dylib=rdmacm");
    println!("cargo:rustc-link-lib=dylib=ibverbs");

    println!("cargo:rerun-if-changed=csrc/shim.c");
    println!("cargo:rerun-if-changed=csrc/shim.h");
    println!("cargo:rerun-if-env-changed=CC");
    println!("cargo:rerun-if-env-changed=AR");
}

fn run(cmd: &mut Command) {
    let rendered = format!("{cmd:?}");
    let status = cmd
        .status()
        .unwrap_or_else(|e| panic!("failed to spawn {rendered}: {e}"));
    assert!(status.success(), "command failed ({status}): {rendered}");
}
