#[cfg(feature = "ffi")]
fn main() {
    use std::env;
    use std::path::PathBuf;

    const LIB_NAME: &str = "libcqlite.dylib";

    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    cbindgen::Builder::new()
        .with_crate(&manifest_dir)
        .with_language(cbindgen::Language::C)
        .with_no_includes()
        .with_sys_include("stdint.h")
        .with_sys_include("stdbool.h")
        .with_include_guard("CQLITE_H")
        .generate()
        .unwrap()
        .write_to_file("cqlite.h");

    let include_dir = manifest_dir.clone();
    let mut shared_object_dir = PathBuf::from(manifest_dir);
    shared_object_dir.push("target");
    shared_object_dir.push(env::var("PROFILE").unwrap());
    let shared_object_dir = shared_object_dir.as_path().to_string_lossy();

    println!(
        "cargo:rustc-env=INLINE_C_RS_CFLAGS=-I{I} -L{L} -D_DEBUG -D_CRT_SECURE_NO_WARNINGS",
        I = include_dir,
        L = shared_object_dir,
    );

    println!(
        "cargo:rustc-env=INLINE_C_RS_LDFLAGS={shared_object_dir}/{lib}",
        shared_object_dir = shared_object_dir,
        lib = LIB_NAME,
    );
}

#[cfg(not(feature = "ffi"))]
fn main() {}
