# assertion-replay

`assertion-replay` is an HTTP service that backtests recent blocks through sidecar's core engine and assertion executor.

## What It Does

For each `POST /replay`:

1. Reads the current head from archive WS.
2. Computes replay start as `head - replay_window`.
3. Replays `[start_block, head]`.
4. Measures execution time.
5. Auto-tunes `replay_window` for the next run to keep replay duration near target.

If request contains watched assertion IDs, replay may stop early on first matched incident.

## Adaptive Window Tuning

The service targets one replay run around **12.5 minutes**.

- if elapsed > `max` (default 15 min), it reduces window
- if elapsed < `min` (default 10 min), it increases window
- otherwise keeps window unchanged

Adjustment is proportional:

`next_window = current_window * (target_duration / actual_duration)`

Defaults:

- min: `10.0` minutes
- target: `12.5` minutes
- max: `15.0` minutes

## Configuration

Configuration is loaded from CLI args and/or env vars.

- `REPLAY_BIND_ADDR` (default: `0.0.0.0:8080`)
- `REPLAY_ARCHIVE_WS_URL` (required)
- `REPLAY_ARCHIVE_HTTP_URL` (required)
- `REPLAY_WINDOW` (required; initial replay window in blocks)
- `REPLAY_CHAIN_ID` (default: `1`)
- `REPLAY_ASSERTION_GAS_LIMIT` (default: `1000000000`)
- `REPLAY_DURATION_MIN_MINUTES` (default: `10.0`)
- `REPLAY_DURATION_TARGET_MINUTES` (default: `12.5`)
- `REPLAY_DURATION_MAX_MINUTES` (default: `15.0`)

## Endpoints

### `GET /health`

Returns:

- `200 OK`

### `GET /replay/start-block`

Returns the current replay start preview, computed as:

`start_block = current_head - current_replay_window`

Response (`200 OK`):

```json
{
  "start_block": 12345000,
  "head_block": 12348000,
  "replay_window": 3000
}
```

Failures:

- `500 Internal Server Error` when head block cannot be queried

### `POST /replay`

Starts one replay/backtest run.

Request body:

```json
{
  "assertions": [
    {
      "adopter": "0x1111111111111111111111111111111111111111",
      "deployment_bytecode": "0x6001600055",
      "id": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    }
  ]
}
```

`assertions` is optional:

- empty/missing: replay full computed range
- non-empty: replay watches incidents and can stop early on first watched ID match

Responses:

- `200 OK` on success
- `400 Bad Request` for invalid JSON payload
- `500 Internal Server Error` for runtime/replay failures

Error body:

```json
{
  "error": "human readable message"
}
```
