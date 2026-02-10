use std::process::Command;

fn main() {
    emit_build_metadata();

    // Generate gRPC bindings for the sidecar transport service
    println!("cargo:rerun-if-changed=src/transport/grpc/sidecar.proto");
    // Use a vendored `protoc` to avoid requiring it on the system
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("protoc not found (vendored)");

    // Setting an env var for build tooling is safe; this is a build script.
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }

    let rax = tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &["src/transport/grpc/sidecar.proto"],
            &["src/transport/grpc"],
        );

    // Unset var after being set
    unsafe {
        std::env::remove_var("PROTOC");
    }

    assert!(rax.is_ok(), "Failed to compile gRPC protos");
}

fn emit_build_metadata() {
    // Rebuild metadata when HEAD changes. On non-git checkouts this is harmless.
    println!("cargo:rerun-if-changed=.git/HEAD");

    let git_sha = command_output("git", &["rev-parse", "--short", "HEAD"])
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=SIDECAR_GIT_SHA={git_sha}");

    let rustc_version =
        command_output("rustc", &["--version"]).unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=SIDECAR_RUSTC_VERSION={rustc_version}");
}

fn command_output(command: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(command).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8(output.stdout).ok()?;
    let trimmed = stdout.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.to_string())
}
