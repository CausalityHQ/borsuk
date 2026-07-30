//! Platform linker configuration for the Python extension module.

fn main() {
    // PyO3 extension modules resolve CPython symbols from the importing
    // interpreter. Direct `cargo build --workspace --all-targets` therefore
    // needs the platform-specific extension-module linker flags that maturin
    // normally supplies for wheel builds.
    pyo3_build_config::add_extension_module_link_args();
}
