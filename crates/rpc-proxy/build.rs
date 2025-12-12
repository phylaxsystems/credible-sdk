fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("failed to find protoc");
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }

    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .compile_protos(&["proto/heuristics.proto"], &["proto"])
        .expect("compile proto");
}
