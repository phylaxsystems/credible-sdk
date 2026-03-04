use serde_json::json;
use std::{
    env,
    fs,
    path::PathBuf,
};

fn main() {
    emit_rerun_hints();
    let output_file = output_file_path();
    let document = openapi_document();
    let json = serde_json::to_string_pretty(&document).expect("failed to serialize openapi");
    fs::write(output_file, format!("{json}\n")).expect("failed to write openapi");
}

fn emit_rerun_hints() {
    for path in [
        "build.rs",
        "src/server/handlers/health.rs",
        "src/server/handlers/start_block.rs",
        "src/server/handlers/replay.rs",
        "src/server/models/replay.rs",
        "src/server/models/error.rs",
    ] {
        println!("cargo:rerun-if-changed={path}");
    }
}

fn output_file_path() -> PathBuf {
    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR should be set"));
    manifest_dir.join("openapi.json")
}

fn openapi_document() -> serde_json::Value {
    json!({
        "openapi": "3.1.0",
        "info": openapi_info(),
        "paths": openapi_paths(),
        "components": openapi_components(),
        "tags": openapi_tags()
    })
}

fn openapi_info() -> serde_json::Value {
    json!({
        "title": env!("CARGO_PKG_NAME"),
        "description": "",
        "contact": { "name": "Phylax Systems" },
        "license": {
            "name": "BSL 1.1",
            "identifier": "BSL 1.1"
        },
        "version": env!("CARGO_PKG_VERSION")
    })
}

fn openapi_paths() -> serde_json::Value {
    json!({
        "/health": {
            "get": {
                "tags": ["assertion-replay"],
                "operationId": "health_handler",
                "responses": {
                    "200": { "description": "Service is healthy" }
                }
            }
        },
        "/replay/start-block": {
            "get": {
                "tags": ["assertion-replay"],
                "operationId": "replay_start_block_handler",
                "responses": {
                    "200": {
                        "description": "Replay start-block preview",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "$ref": "#/components/schemas/ReplayStartBlockResponse"
                                }
                            }
                        }
                    },
                    "500": {
                        "description": "Failed to compute replay start-block",
                        "content": {
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                            }
                        }
                    }
                }
            }
        },
        "/replay": {
            "post": {
                "tags": ["assertion-replay"],
                "operationId": "replay_handler",
                "requestBody": {
                    "required": true,
                    "content": {
                        "application/json": {
                            "schema": { "$ref": "#/components/schemas/ReplayRequest" }
                        }
                    }
                },
                "responses": {
                    "200": { "description": "Replay completed" },
                    "400": {
                        "description": "Invalid payload",
                        "content": {
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                            }
                        }
                    },
                    "500": {
                        "description": "Replay failed",
                        "content": {
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/ErrorResponse" }
                            }
                        }
                    }
                }
            }
        }
    })
}

fn openapi_components() -> serde_json::Value {
    json!({
        "schemas": {
            "ReplayRequest": {
                "type": "object",
                "properties": {
                    "assertions": {
                        "type": "array",
                        "items": { "$ref": "#/components/schemas/Assertion" },
                        "default": []
                    }
                }
            },
            "Assertion": {
                "type": "object",
                "required": ["adopter", "deployment_bytecode", "id"],
                "properties": {
                    "adopter": {
                        "type": "string",
                        "pattern": "^0x[a-fA-F0-9]{40}$"
                    },
                    "deployment_bytecode": {
                        "type": "string",
                        "pattern": "^0x[a-fA-F0-9]*$"
                    },
                    "id": {
                        "type": "string",
                        "pattern": "^0x[a-fA-F0-9]{64}$"
                    }
                }
            },
            "ReplayStartBlockResponse": {
                "type": "object",
                "required": ["start_block", "head_block", "replay_window"],
                "properties": {
                    "start_block": { "type": "integer", "format": "int64", "minimum": 0 },
                    "head_block": { "type": "integer", "format": "int64", "minimum": 0 },
                    "replay_window": { "type": "integer", "format": "int64", "minimum": 0 }
                }
            },
            "ErrorResponse": {
                "type": "object",
                "required": ["error"],
                "properties": { "error": { "type": "string" } }
            }
        }
    })
}

fn openapi_tags() -> serde_json::Value {
    json!([
        {
            "name": "assertion-replay",
            "description": "Replay service API"
        }
    ])
}
