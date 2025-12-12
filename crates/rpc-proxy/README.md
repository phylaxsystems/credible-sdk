# rpc-proxy

A placeholder for the JSON-RPC proxy that will sit in front of the sequencer. Future work will:

- accept standard Ethereum RPC calls and forward them to the sequencer
- maintain a gRPC channel to the sidecar for invalidation fingerprints
- enforce fingerprint bans and assertion cool-down heuristics before forwarding transactions

Implementation will start once the heuristics are finalized.
