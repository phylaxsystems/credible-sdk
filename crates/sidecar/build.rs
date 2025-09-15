fn main() {
    // Generate gRPC bindings for the sidecar transport service
    println!("cargo:rerun-if-changed=src/transport/grpc/sidecar.proto");
    // Use a vendored `protoc` to avoid requiring it on the system
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("protoc not found (vendored)");

    // Setting an env var for build tooling is safe; this is a build script.
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &["src/transport/grpc/sidecar.proto"],
            &["src/transport/grpc"],
        )
        .expect("Failed to compile gRPC protos");

    // Unset var after being set
    unsafe {
        std::env::remove_var("PROTOC");
    }
}
