//! Generates the test gRPC service from `proto/`, with a vendored `protoc`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/k9/test/echo.proto";
    println!("cargo:rerun-if-changed={proto}");
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    let descriptors = std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("echo.bin");
    tonic_prost_build::configure()
        .build_client(false)
        .file_descriptor_set_path(descriptors)
        .compile_with_config(config, &[proto], &["proto"])?;
    Ok(())
}
