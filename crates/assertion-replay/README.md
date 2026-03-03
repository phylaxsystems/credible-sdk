# assertion-replay

`assertion-replay` is an HTTP service that replays blockchain blocks with given assertions through the sidecar's core
engine and assertion executor.
This way, we can observe the effects of assertions on the chain.

It is intended for:

- replaying from a configured `start_block` up to current head (timeout 15 mins)
- observing assertion failures during replay
- optionally stopping early when a watched assertion id is observed

## How it works

For each `POST /replay` request, the service:

1. Connects to archive RPC (`ws` + `http`)
2. Initializes an in-process sidecar's core engine
3. Replays blocks in `[REPLAY_START_BLOCK, current_head]` with 15 mins timeout
4. If request includes `assertion_ids`, it watches incident reports and can stop early on first match
5. Shuts down runtime/engine cleanly

## Configuration

Configuration is loaded from CLI args and/or environment variables.

- `REPLAY_BIND_ADDR` (default: `0.0.0.0:8080`)
- `REPLAY_ARCHIVE_WS_URL` (required)
- `REPLAY_ARCHIVE_HTTP_URL` (required)
- `REPLAY_START_BLOCK` (required)
- `REPLAY_CHAIN_ID` (default: `1`)
- `REPLAY_ASSERTION_GAS_LIMIT` (default: `1000000000`)

## HTTP API

### `GET /health`

Health probe endpoint.

- Response: `200 OK`
- Body: empty

### `POST /replay`

Starts one replay run.

Request body:

```json
{
  "assertion_ids": [
    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  ]
}
```

`assertion_ids`:

- replay watches incidents and may stop early when one watched assertion ID is observed

Responses:

- `200 OK` on successful replay execution
- `400 Bad Request` for invalid JSON payload
- `500 Internal Server Error` for runtime/replay failures

Error response body:

```json
{
  "error": "human readable message"
}
```