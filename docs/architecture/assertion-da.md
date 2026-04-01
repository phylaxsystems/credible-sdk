# Assertion DA

This file covers `crates/assertion-da/da-client`, `crates/assertion-da/da-core`, and `crates/assertion-da/da-server`.

## Assertion DA schema

```text
sidecar indexer
  |
  v
assertion-da-client
  |
  v
assertion-da-server (JSON-RPC over HTTP)
  |
  +--> verify JSON-RPC envelope + method + body size
  |
  +--> da_submit_solidity_assertion
  |      |
  |      +--> compile Solidity in Dockerized solc
  |      +--> ABI-encode constructor args
  |      +--> keccak256(deployment bytecode + args) => assertion id
  |      +--> sign id with DA private key
  |      +--> persist StoredAssertion
  |
  +--> da_get_assertion
  |      |
  |      +--> fetch StoredAssertion by id
  |
  v
db task over mpsc
  |
  +--> Sled
  |
  +--> Redis
```

## What it is

The Assertion DA stack is a JSON-RPC service plus client and shared request types.

- `da-core` defines the request and response contract used by both sides.
- `da-client` issues JSON-RPC requests and validates response envelopes.
- `da-server` compiles, stores, signs, and serves assertion artifacts.

Its role is narrow but important: it is the artifact handoff point between assertion authoring and later assertion indexing. The service does not execute assertions. It only turns a Solidity assertion submission into a durable, fetchable artifact bundle.

## Shared request model

`da-core` defines three core payloads:

- `DaSubmission`: Solidity source, compiler version, assertion contract name, constructor args, and constructor ABI signature.
- `DaSubmissionResponse`: assertion `id` plus `prover_signature`.
- `DaFetchResponse`: original source, creation bytecode without constructor args, encoded constructor args, constructor ABI signature, and the same prover signature.

The important behavioral detail is that fetch returns `bytecode` and `encoded_constructor_args` separately. Consumers are expected to append the encoded constructor args to the creation bytecode when reconstructing deployment data.

## Client behavior

`da-client` exposes two JSON-RPC methods:

- `da_submit_solidity_assertion`
- `da_get_assertion`

It is intentionally strict:

- it always sends JSON-RPC 2.0 envelopes,
- it checks that the response is JSON-RPC 2.0,
- it verifies that the response `id` matches the request `id`,
- it treats transport failures and malformed envelopes as client errors.

`DaClientError::is_reachable_da_error()` is used by other components, especially `sidecar`'s DA reachability monitor, to distinguish a DA application error from DA unreachability.

## Server startup and process model

`da-server` startup does the following:

1. binds a TCP listener from config,
2. connects to Docker,
3. chooses a storage backend,
4. loads the DA signing private key,
5. constructs a `DaServer`.

The storage backend selection is dynamic:

- if `DA_REDIS_URL` is configured and reachable, Redis is used,
- otherwise the server falls back to a local Sled database under the configured or XDG-derived path.

Runtime execution splits into two cooperating tasks:

- an API task that accepts HTTP connections and processes JSON-RPC requests,
- a DB task that listens on an in-process channel and serializes `Get` and `Insert` operations against the chosen backend.

That split keeps the API layer backend-agnostic. API handlers never talk to Sled or Redis directly.

## HTTP, readiness, and request validation

The server exposes:

- `/health`
- `/ready`

Readiness is not a trivial always-true probe. It verifies:

- the cancellation token has not already been triggered,
- Docker is reachable,
- the DB task can round-trip a request successfully.

The JSON-RPC path is served over Hyper. Before method dispatch, request processing validates:

- the request body exists and parses as JSON,
- the JSON-RPC version is correct,
- the method shape is valid,
- the body stays under the configured size limit,
- the expected params are present for the selected method.

Invalid envelopes are rejected before they reach business logic.

## Submission behavior

The primary write path is `da_submit_solidity_assertion`.

Its behavior is:

1. parse `DaSubmission`,
2. compile the Solidity source in a Dockerized `solc`,
3. ABI-encode constructor args using the provided ABI signature,
4. append encoded constructor args to the creation bytecode,
5. compute `keccak256(deployment_data)` as the assertion id,
6. sign that id with the DA private key,
7. persist a `StoredAssertion`,
8. return the id and signature.

This means the DA identity is bound to deployable creation data, not just to source text. Changing constructor args changes the encoded deployment data and therefore changes the assertion id.

## Compilation model

Compilation is delegated to Docker rather than to an in-process Solidity compiler.

`source_compilation.rs` is responsible for:

- selecting the correct `solc` image,
- respecting optional platform overrides,
- ensuring the image exists locally,
- pulling it when necessary,
- creating and later removing the compiler container,
- extracting compilation outputs needed by the API layer.

The operational consequence is that DA readiness and submission success both depend on Docker being healthy and on the selected compiler image being runnable.

## Fetch behavior

The primary read path is `da_get_assertion`.

Given an assertion id, the server returns:

- original Solidity source,
- creation bytecode without constructor args appended,
- ABI-encoded constructor args,
- constructor ABI signature,
- DA prover signature.

Consumers can therefore reproduce the exact deployable creation payload and independently verify the signed assertion id.

## Storage model

`da-server` treats persistence as a key-value store over assertion id.

The currently supported backends are:

- Redis
- Sled

Both are hidden behind the same `Database` trait. All API writes and reads pass through `listen_for_db()`, which processes:

- `DbOperation::Insert`
- `DbOperation::Get`

Backend choice changes durability and operations characteristics, but not API semantics.
