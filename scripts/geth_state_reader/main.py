from __future__ import annotations

import argparse
import json
import subprocess
import sys
import threading
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Optional

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
    def __init__(self, url: str, namespace: str, pipeline_size: int) -> None:
        self._namespace = namespace.rstrip(":")
        self._client = redis.Redis.from_url(url)
        self._pipeline = self._client.pipeline(transaction=False)
        self._pipeline_size = max(1, pipeline_size)
        self._pending = 0

    def handle(self, account: AccountRecord) -> None:
        address_hex = _strip_hex_prefix(account.address_hash)
        account_key = f"{self._namespace}:account:{address_hex}"
        mapping = {
            "balance": str(account.balance),
            "nonce": str(account.nonce),
            "code_hash": account.code_hash.lower(),
        }
        self._pipeline.hset(account_key, mapping=mapping)

        if account.storage:
            storage_key = f"{self._namespace}:storage:{address_hex}"
            self._pipeline.hset(storage_key, mapping=account.storage)

        if account.code and account.code_hash:
            code_key = f"{self._namespace}:code:{_strip_hex_prefix(account.code_hash)}"
            self._pipeline.set(code_key, account.code.lower())

        self._pending += 1
        if self._pending >= self._pipeline_size:
            self._pipeline.execute()
            self._pending = 0

    def finalize(self, metadata: DumpMetadata) -> None:
        if self._pending:
            self._pipeline.execute()
            self._pending = 0

        meta_pipe = self._client.pipeline(transaction=False)
        if metadata.block_number is not None:
            block_number_str = str(metadata.block_number)
            meta_pipe.set(f"{self._namespace}:current_block", block_number_str)
            if metadata.block_hash:
                meta_pipe.set(
                    f"{self._namespace}:block_hash:{metadata.block_number}",
                    metadata.block_hash.lower(),
                )
            if metadata.state_root:
                meta_pipe.set(
                    f"{self._namespace}:state_root:{metadata.block_number}",
                    metadata.state_root.lower(),
                )
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


def run_geth_dump(
    *,
    geth_bin: str,
    datadir: str,
    block_argument: Optional[str],
    start_key: Optional[str],
    limit: Optional[int],
    on_account: Callable[[AccountRecord], None],
) -> DumpMetadata:
    cmd = [
        geth_bin,
        "dump",
        "--datadir",
        datadir,
        "--iterative",
        "--incompletes",
    ]

    if limit is not None:
        cmd.extend(["--limit", str(limit)])
    if start_key:
        cmd.extend(["--start", start_key])

    if block_argument:
        cmd.append(block_argument)

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

    if proc.returncode != 0:
        raise RuntimeError(
            f"geth dump exited with {proc.returncode}: {''.join(stderr_lines)}"
        )

    info = _extract_block_info(stderr_lines)
    if metadata.block_number is None:
        metadata.block_number = info.block_number
    if metadata.block_hash is None:
        metadata.block_hash = info.block_hash

    return metadata


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
        default=1000,
        help="Number of commands to batch per Redis execute call.",
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

    sinks: List[object] = []

    if args.json_output_enabled or args.json_output:
        sinks.append(JsonSink(args.json_output))

    if args.redis_url:
        sinks.append(RedisSink(args.redis_url, args.redis_namespace, args.redis_pipeline_size))

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
    )

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
