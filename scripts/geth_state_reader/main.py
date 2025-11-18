from __future__ import annotations

import argparse
import json
import subprocess
import sys
import threading
from dataclasses import dataclass
import re
from typing import Callable, Dict, Iterable, List, Optional, Tuple

import redis


def _normalize_hex(value: Optional[object], *, pad_to: Optional[int] = None) -> Optional[str]:
    """Return a lowercase 0x-prefixed hex string (or None)."""
    if value is None:
        return None
    if isinstance(value, bytes):
        body = value.hex()
    elif isinstance(value, int):
        body = f"{value:x}"
    elif isinstance(value, str):
        body = value.strip()
        if body.startswith("0x") or body.startswith("0X"):
            body = body[2:]
    else:
        return None

    if pad_to:
        body = body.rjust(pad_to * 2, "0")
    if not body:
        body = "0"
    return "0x" + body.lower()


def _parse_int(value: object) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        trimmed = value.strip()
        if trimmed.startswith(("0x", "0X")):
            return int(trimmed, 16)
        return int(trimmed, 10)
    raise TypeError(f"Unsupported numeric type: {type(value).__name__}")


def _strip_hex_prefix(value: str) -> str:
    return value[2:] if value.startswith(("0x", "0X")) else value


def _parse_storage(raw: object) -> Dict[str, str]:
    storage: Dict[str, str] = {}
    if not raw:
        return storage

    def insert(slot_key: Optional[str], slot_value: Optional[str]) -> None:
        key_hex = _normalize_hex(slot_key, pad_to=32)
        value_hex = _normalize_hex(slot_value, pad_to=32)
        if key_hex is None or value_hex is None:
            return
        storage[key_hex] = value_hex

    if isinstance(raw, dict):
        for hashed_slot, value in raw.items():
            if isinstance(value, dict):
                insert(value.get("key") or hashed_slot, value.get("value"))
            else:
                insert(hashed_slot, value)
    elif isinstance(raw, list):
        for entry in raw:
            if isinstance(entry, dict):
                insert(entry.get("key") or entry.get("hash"), entry.get("value"))
    return storage


@dataclass
class AccountRecord:
    address_hash: str
    balance: int
    nonce: int
    code_hash: str
    code: Optional[str]
    storage: Dict[str, str]

    def as_dict(self) -> Dict[str, object]:
        payload = {
            "address_hash": self.address_hash,
            "balance": str(self.balance),
            "nonce": self.nonce,
            "code_hash": self.code_hash,
        }
        if self.code is not None:
            payload["code"] = self.code
        if self.storage:
            payload["storage"] = self.storage
        return payload


@dataclass
class DumpMetadata:
    block_number: Optional[int]
    block_hash: Optional[str]
    state_root: Optional[str]
    total_accounts: int = 0


class GethDumpError(RuntimeError):
    def __init__(self, command: str, returncode: int, stderr: str) -> None:
        self.command = command
        self.returncode = returncode
        self.stderr = stderr
        super().__init__(f"geth {command} exited with {returncode}: {stderr}")


class JsonSink:
    def __init__(self, path: Optional[str]):
        self._path = path
        self._stream = open(path, "w") if path else sys.stdout

    def handle(self, account: AccountRecord) -> None:
        json.dump(account.as_dict(), self._stream)
        self._stream.write("\n")
        self._stream.flush()

    def finalize(self, _: DumpMetadata) -> None:
        if self._path:
            self._stream.close()


class RedisSink:
    def __init__(
        self,
        url: str,
        namespace: str,
        pipeline_size: int,
        buffer_size: int,
        block_number: int,
    ) -> None:
        if block_number is None:
            raise ValueError("--block-number is required when writing to Redis")
        if buffer_size <= 0:
            raise ValueError("--redis-buffer-size must be positive")

        self._base_namespace = namespace.rstrip(":")
        self._buffer_size = buffer_size
        self._block_number = block_number
        namespace_idx = block_number % buffer_size
        self._namespace = f"{self._base_namespace}:{namespace_idx}"
        self._client = redis.Redis.from_url(url)
        self._pipeline_size = max(1, pipeline_size)
        self._pipeline = (
            self._client.pipeline(transaction=False) if self._pipeline_size > 1 else None
        )
        self._pending = 0
        self._clear_namespace()

    def _clear_namespace(self) -> None:
        patterns = [
            f"{self._namespace}:account:*",
            f"{self._namespace}:storage:*",
            f"{self._namespace}:code:*",
        ]
        for pattern in patterns:
            cursor = 0
            while True:
                cursor, keys = self._client.scan(cursor=cursor, match=pattern, count=1000)
                if keys:
                    self._client.delete(*keys)
                if cursor == 0:
                    break
        self._client.delete(f"{self._namespace}:block")

    def handle(self, account: AccountRecord) -> None:
        address_hex = _strip_hex_prefix(account.address_hash)
        account_key = f"{self._namespace}:account:{address_hex}"
        mapping = {
            "balance": str(account.balance),
            "nonce": str(account.nonce),
            "code_hash": account.code_hash.lower(),
        }
        target = self._pipeline or self._client
        target.hset(account_key, mapping=mapping)

        if account.storage:
            storage_key = f"{self._namespace}:storage:{address_hex}"
            target.hset(storage_key, mapping=account.storage)

        if account.code and account.code_hash:
            code_key = f"{self._namespace}:code:{_strip_hex_prefix(account.code_hash)}"
            target.set(code_key, account.code.lower())

        if self._pipeline is not None:
            self._pending += 1
            if self._pending >= self._pipeline_size:
                self._pipeline.execute()
                self._pending = 0

    def finalize(self, metadata: DumpMetadata) -> None:
        if self._pipeline is not None and self._pending:
            self._pipeline.execute()
            self._pending = 0

        meta_pipe = self._client.pipeline(transaction=False)
        namespace_block_key = f"{self._namespace}:block"
        meta_pipe.set(namespace_block_key, str(self._block_number))

        block_hash_key = f"{self._base_namespace}:block_hash:{self._block_number}"
        state_root_key = f"{self._base_namespace}:state_root:{self._block_number}"
        latest_block_key = f"{self._base_namespace}:meta:latest_block"
        indices_key = f"{self._base_namespace}:state_dump_indices"

        meta_pipe.set(latest_block_key, str(self._block_number))
        meta_pipe.set(indices_key, str(self._buffer_size))

        if metadata.block_hash:
            meta_pipe.set(block_hash_key, metadata.block_hash.lower())
        if metadata.state_root:
            meta_pipe.set(state_root_key, metadata.state_root.lower())

        meta_pipe.execute()


def _spawn_reader(stream, collector: List[str]) -> threading.Thread:
    def _reader() -> None:
        try:
            for line in stream:
                collector.append(line)
        finally:
            stream.close()

    thread = threading.Thread(target=_reader, daemon=True)
    thread.start()
    return thread


def _parse_account(entry: Dict[str, object]) -> AccountRecord:
    address_hash = _normalize_hex(entry["key"], pad_to=32)
    if address_hash is None:
        raise ValueError("Account entry missing hashed address")

    code_hash = _normalize_hex(entry.get("codeHash"), pad_to=32) or "0x"
    code = _normalize_hex(entry.get("code"))
    if code == "0x":
        code = None

    storage = _parse_storage(entry.get("storage"))

    return AccountRecord(
        address_hash=address_hash,
        balance=_parse_int(entry.get("balance", 0)),
        nonce=_parse_int(entry.get("nonce", 0)),
        code_hash=code_hash,
        code=code,
        storage=storage,
    )


def _extract_block_info(stderr_lines: Iterable[str]) -> DumpMetadata:
    block_number: Optional[int] = None
    block_hash: Optional[str] = None
    for line in stderr_lines:
        if "State dump configured" in line:
            parts = line.strip().split()
            for part in parts:
                if part.startswith("block="):
                    try:
                        block_number = int(part.split("=", 1)[1], 10)
                    except ValueError:
                        pass
                elif part.startswith("hash="):
                    block_hash = part.split("=", 1)[1].lower()
    return DumpMetadata(block_number, block_hash, None)


def _build_dump_command(
    *,
    mode: str,
    geth_bin: str,
    datadir: str,
    block_argument: Optional[str],
    start_key: Optional[str],
    limit: Optional[int],
) -> List[str]:
    cmd: List[str] = [geth_bin]
    if mode == "snapshot":
        cmd.extend(["snapshot", "dump"])
    else:
        cmd.append("dump")
        cmd.extend(["--iterative", "--incompletes"])

    cmd.extend(["--datadir", datadir])
    if limit is not None:
        cmd.extend(["--limit", str(limit)])
    if start_key:
        cmd.extend(["--start", start_key])
    if block_argument:
        cmd.append(block_argument)
    return cmd


def _invoke_geth_dump(
    *,
    mode: str,
    geth_bin: str,
    datadir: str,
    block_argument: Optional[str],
    start_key: Optional[str],
    limit: Optional[int],
    on_account: Callable[[AccountRecord], None],
) -> DumpMetadata:
    cmd = _build_dump_command(
        mode=mode,
        geth_bin=geth_bin,
        datadir=datadir,
        block_argument=block_argument,
        start_key=start_key,
        limit=limit,
    )

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(f"Failed to execute '{geth_bin}': {exc}") from exc

    stderr_lines: List[str] = []
    stderr_thread = _spawn_reader(proc.stderr, stderr_lines)

    metadata = DumpMetadata(block_number=None, block_hash=None, state_root=None)

    assert proc.stdout is not None
    for raw_line in proc.stdout:
        line = raw_line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue

        if "key" not in payload and "root" in payload and metadata.state_root is None:
            metadata.state_root = _normalize_hex(payload["root"], pad_to=32)
            continue
        if "key" not in payload:
            continue

        account = _parse_account(payload)
        on_account(account)
        metadata.total_accounts += 1

    proc.stdout.close()
    proc.wait()
    stderr_thread.join()

    stderr_text = "".join(stderr_lines)
    if proc.returncode != 0:
        raise GethDumpError(
            command="snapshot dump" if mode == "snapshot" else "dump",
            returncode=proc.returncode,
            stderr=stderr_text,
        )

    info = _extract_block_info(stderr_lines)
    if metadata.block_number is None:
        metadata.block_number = info.block_number
    if metadata.block_hash is None:
        metadata.block_hash = info.block_hash

    return metadata


def _should_retry_snapshot(stderr_text: str) -> bool:
    lowered = stderr_text.lower()
    retry_tokens = [
        "head doesn't match snapshot",
        "snapshot not found",
        "snapshot storage not ready",
        "loaded snapshot journal",
        "failed to load snapshot",
    ]
    return any(token in lowered for token in retry_tokens)


def _should_retry_trie(stderr_text: str) -> bool:
    lowered = stderr_text.lower()
    return "missing trie node" in lowered or "state is not available" in lowered


def _parse_snapshot_head_mismatch(stderr_text: str) -> Optional[Tuple[str, str]]:
    match = re.search(
        r"head doesn't match snapshot: have (0x[a-f0-9]+), want (0x[a-f0-9]+)",
        stderr_text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    have_raw, want_raw = match.groups()
    have = _normalize_hex(have_raw, pad_to=32)
    want = _normalize_hex(want_raw, pad_to=32)
    if have and want:
        return have, want
    return None


def _parse_missing_trie_node(stderr_text: str) -> Optional[str]:
    match = re.search(r"missing trie node ([0-9a-fx]+)", stderr_text, flags=re.IGNORECASE)
    if match:
        return _normalize_hex(match.group(1), pad_to=32)
    match = re.search(r"state (0x[a-f0-9]+) is not available", stderr_text, flags=re.IGNORECASE)
    if match:
        return _normalize_hex(match.group(1), pad_to=32)
    return None


def _maybe_raise_pruned_state_error(errors: List[GethDumpError], block_argument: Optional[str]) -> None:
    snapshot_error = next((err for err in errors if err.command == "snapshot dump"), None)
    trie_error = next((err for err in errors if err.command == "dump"), None)
    if not snapshot_error or not trie_error:
        return

    mismatch = _parse_snapshot_head_mismatch(snapshot_error.stderr)
    missing_node = _parse_missing_trie_node(trie_error.stderr)
    if not mismatch or not missing_node:
        return

    head_root, requested_root = mismatch
    if requested_root != missing_node:
        return

    block_label = block_argument or "the requested block"
    raise RuntimeError(
        "Geth pruned the historical state needed for "
        f"{block_label}: snapshot data only exists for head root {head_root}, "
        f"while the requested block requires state root {requested_root} "
        f"and the trie backend reported missing node {missing_node}. "
        "Re-sync the datadir with --gcmode=archive or select a block within the "
        "snapshot horizon (typically HEAD-127 or newer)."
    )


def run_geth_dump(
    *,
    geth_bin: str,
    datadir: str,
    block_argument: Optional[str],
    start_key: Optional[str],
    limit: Optional[int],
    on_account: Callable[[AccountRecord], None],
    dump_backend: str,
    verbose: bool,
) -> DumpMetadata:
    def _backend_order() -> List[str]:
        if dump_backend == "snapshot":
            return ["snapshot"]
        if dump_backend == "trie":
            return ["trie"]
        return ["snapshot", "trie"]

    errors: List[GethDumpError] = []
    for mode in _backend_order():
        if verbose:
            print(f"Invoking geth {mode} dump …", file=sys.stderr)
        try:
            return _invoke_geth_dump(
                mode=mode,
                geth_bin=geth_bin,
                datadir=datadir,
                block_argument=block_argument,
                start_key=start_key,
                limit=limit,
                on_account=on_account,
            )
        except GethDumpError as exc:
            errors.append(exc)
            if dump_backend != "auto":
                raise
            if mode == "snapshot" and _should_retry_snapshot(exc.stderr):
                if verbose:
                    print(
                        "Snapshot backend unavailable, falling back to trie dump.",
                        file=sys.stderr,
                    )
                continue
            if mode == "trie" and _should_retry_trie(exc.stderr):
                if verbose:
                    print(
                        "Trie backend missing node data, falling back to snapshot dump.",
                        file=sys.stderr,
                    )
                continue
            raise

    _maybe_raise_pruned_state_error(errors, block_argument)
    error_messages = "\n---\n".join(str(err) for err in errors)
    raise RuntimeError(f"All geth dump attempts failed:\n{error_messages}")


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Dump Geth state into a Redis instance compatible with StateWorker."
    )
    parser.add_argument(
        "--datadir",
        required=True,
        help="Path to the Geth data directory (the folder containing chaindata).",
    )
    parser.add_argument(
        "--geth-bin",
        default="geth",
        help="Path to the geth binary (defaults to 'geth' in $PATH).",
    )
    parser.add_argument(
        "--geth-dump-backend",
        choices=("auto", "snapshot", "trie"),
        default="auto",
        help=(
            "State dump implementation to invoke: 'snapshot' (preferred for path scheme), "
            "'trie' (legacy geth dump), or 'auto' to try snapshot then trie."
        ),
    )
    parser.add_argument(
        "--block-number",
        type=int,
        help="Specific block number to dump. Defaults to the latest available block.",
    )
    parser.add_argument(
        "--block-hash",
        help="Specific block hash to dump instead of a block number.",
    )
    parser.add_argument(
        "--start-key",
        help="Optional hashed address key (0x…) to start from when iterating accounts.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit the number of accounts returned by geth dump.",
    )
    parser.add_argument(
        "--redis-url",
        help="Redis connection URL (e.g. redis://localhost:6379/0).",
    )
    parser.add_argument(
        "--redis-namespace",
        default="state",
        help="Redis namespace/prefix for the state (default: state).",
    )
    parser.add_argument(
        "--redis-pipeline-size",
        type=int,
        default=1,
        help="Number of accounts to buffer before flushing Redis pipelines (default: 1).",
    )
    parser.add_argument(
        "--redis-buffer-size",
        type=int,
        default=3,
        help="Number of circular buffer namespaces to rotate (default: 3).",
    )
    parser.add_argument(
        "--json-output",
        help="When set, write newline-delimited JSON to this file.",
    )
    parser.add_argument(
        "--json-output-enabled",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Emit JSON lines (defaults to disabled).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging from this script.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    if args.block_number is not None and args.block_hash is not None:
        print("Specify either --block-number or --block-hash, not both.", file=sys.stderr)
        return 2

    if args.redis_url and args.block_number is None:
        print("--block-number is required when writing to Redis.", file=sys.stderr)
        return 2

    if args.redis_buffer_size <= 0:
        print("--redis-buffer-size must be positive.", file=sys.stderr)
        return 2

    sinks: List[object] = []

    if args.json_output_enabled or args.json_output:
        sinks.append(JsonSink(args.json_output))

    if args.redis_url:
        assert args.block_number is not None  # for type checkers
        sinks.append(
            RedisSink(
                args.redis_url,
                args.redis_namespace,
                args.redis_pipeline_size,
                args.redis_buffer_size,
                args.block_number,
            )
        )

    if not sinks:
        print("Nothing to do: enable JSON output and/or provide --redis-url.", file=sys.stderr)
        return 1

    def dispatch(account: AccountRecord) -> None:
        for sink in sinks:
            sink.handle(account)

    block_argument: Optional[str] = None
    if args.block_hash:
        block_argument = args.block_hash
    elif args.block_number is not None:
        block_argument = str(args.block_number)

    metadata = run_geth_dump(
        geth_bin=args.geth_bin,
        datadir=args.datadir,
        block_argument=block_argument,
        start_key=args.start_key,
        limit=args.limit,
        on_account=dispatch,
        dump_backend=args.geth_dump_backend,
        verbose=args.verbose,
    )

    if metadata.block_number is None:
        metadata.block_number = args.block_number

    if args.block_number is not None and metadata.block_number is not None:
        if metadata.block_number != args.block_number:
            print(
                "Mismatch between geth dump block number and --block-number.",
                file=sys.stderr,
            )
            return 1

    for sink in sinks:
        sink.finalize(metadata)

    if args.verbose:
        print(
            f"Synced {metadata.total_accounts} accounts "
            f"for block {metadata.block_number} "
            f"(state_root={metadata.state_root}, block_hash={metadata.block_hash})",
            file=sys.stderr,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
