//! Generates the gRPC code from `proto/`, with a vendored `protoc` so building
//! k9 needs no protobuf install.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/k9/worker/v1/worker.proto";
    println!("cargo:rerun-if-changed={proto}");
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    tonic_prost_build::configure().compile_with_config(config, &[proto], &["proto"])?;
    Ok(())
}
