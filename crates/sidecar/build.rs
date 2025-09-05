fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile protobuf files for gRPC transport
    tonic_build::configure()
        .build_server(true)
        .build_client(false) // We only need server for the sidecar
        .out_dir("src/transport/grpc")
        .compile(
            &["src/transport/grpc/sidecar.proto"],
            &["src/transport/grpc"],
        )?;

    Ok(())
}
