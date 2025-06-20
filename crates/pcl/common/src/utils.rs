use std::path::Path;

use crate::Assertion;

#[derive(Debug)]
pub struct BuildInfo {
    pub compiler_version: String,
    pub compilation_target: String,
    pub bytecode: String,
}

/// Reads a contract artifact
/// Input can be specified in two patterns
/// 1. ${file_name[.sol, .a.sol]}:${contract_name}
/// 2. ${contract_name} (file_name is assumed to be the same as contract_name, with .sol extension)
///
/// out_dir is the output directory of the build artifact
pub fn read_artifact(input: &Assertion, out_dir: &Path) -> serde_json::Value {
    let file_names = input.get_paths();
    // Try each file name until we find one that exists
    for file_name in &file_names {
        let path = out_dir.join(format!("{}/{}.json", file_name, input.contract_name()));
        if path.exists() {
            let file = std::fs::File::open(&path).expect("Failed to open file");
            return serde_json::from_reader(file).expect("Failed to parse JSON");
        }
    }
    panic!("Failed to find artifact for {}", input.contract_name());
}

/// Reads deployment bytecode from a contract artifact
/// Input can be specified in two patterns
/// 1. ${file_name[.sol, .a.sol]}:${contract_name}
/// 2. ${contract_name} (file_name is assumed to be the same as contract_name, with .sol extension)
///
/// out_dir is the output directory of the build artifact
pub fn bytecode(artifact: &serde_json::Value) -> String {
    let bytecode = artifact["bytecode"]["object"]
        .as_str()
        .expect("Failed to read bytecode");
    bytecode.to_string()
}

pub fn compilation_target(input: &Assertion, artifact: &serde_json::Value) -> String {
    // The compilationTarget is a map with a single key-value pair where the key is the file path
    // and the value is the contract name. We need to extract the file path (key).
    let compilation_target = artifact["metadata"]["settings"]["compilationTarget"]
        .as_object()
        .expect("Failed to read compilation target as object");
    // Get the compilation target of the contract with name contract_name
    compilation_target
        .iter()
        .find_map(|(key, value)| {
            if value.as_str() == Some(input.contract_name()) {
                Some(key.to_string())
            } else {
                None
            }
        })
        .expect("Failed to find contract in compilation target")
}

pub fn compiler_version(artifact: &serde_json::Value) -> String {
    let compiler_version = artifact["metadata"]["compiler"]["version"]
        .as_str()
        .expect("failed to read compiler version");
    compiler_version.to_string()
}

pub fn get_build_info(input: &Assertion, out_dir: &Path) -> BuildInfo {
    let artifact = read_artifact(input, out_dir);
    BuildInfo {
        compiler_version: compiler_version(&artifact),
        compilation_target: compilation_target(input, &artifact),
        bytecode: bytecode(&artifact),
    }
}
