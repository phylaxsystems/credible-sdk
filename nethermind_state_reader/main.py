from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from typing import Dict, Iterable, Iterator, List, Optional

import rlp
from eth_hash.auto import keccak
from rocksdict import AccessType, Options, Rdict

EMPTY_TRIE_HASH = bytes.fromhex(
    "56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"
)

_EOA_SUFFIX = bytes(
    [
        160,
        86,
        232,
        31,
        23,
        27,
        204,
        85,
        166,
        255,
        131,
        69,
        230,
        146,
        192,
        248,
        110,
        91,
        72,
        224,
        27,
        153,
        108,
        173,
        192,
        1,
        98,
        47,
        181,
        227,
        99,
        180,
        33,
        160,
        197,
        210,
        70,
        1,
        134,
        247,
        35,
        60,
        146,
        126,
        125,
        178,
        220,
        199,
        3,
        192,
        229,
        0,
        182,
        83,
        202,
        130,
        39,
        59,
        123,
        250,
        216,
        4,
        93,
        133,
        164,
        112,
    ]
)


def _decompress(value: bytes) -> bytes:
    """Undo Nethermind's EOA compression (0x00 + payload without code/storage suffix)."""
    if value and value[0] == 0:
        return value[1:] + _EOA_SUFFIX
    return value


def _decode_hex_prefix(encoded: bytes) -> tuple[List[int], bool]:
    if not encoded:
        return [], False
    flags = encoded[0] >> 4
    is_leaf = (flags & 0b10) != 0
    is_odd = (flags & 0b01) != 0

    nibbles: List[int] = []
    idx = 1
    if is_odd:
        nibbles.append(encoded[0] & 0x0F)
    for byte in encoded[idx:]:
        nibbles.append(byte >> 4)
        nibbles.append(byte & 0x0F)
    return nibbles, is_leaf


def _nibbles_to_bytes(nibbles: List[int]) -> bytes:
    if len(nibbles) % 2 != 0:
        raise ValueError("Received odd-length nibble sequence")
    out = bytearray(len(nibbles) // 2)
    for i in range(0, len(nibbles), 2):
        out[i // 2] = (nibbles[i] << 4) | nibbles[i + 1]
    return bytes(out)


def _resolve_ref(ref: bytes, nodes: Dict[bytes, bytes]) -> Optional[bytes]:
    if len(ref) == 0:
        return None
    if len(ref) == 32:
        value = nodes.get(ref)
        if value is None:
            raise KeyError(f"Missing trie node with hash {ref.hex()}")
        return value
    return _decompress(ref)


def build_node_index(state_path: str, verify: bool = True, verbose: bool = False) -> Dict[bytes, bytes]:
    options = Options(raw_mode=True)
    nodes: Dict[bytes, bytes] = {}

    with Rdict(state_path, options=options, access_type=AccessType.read_only()) as db:
        iterator = db.iter()
        iterator.seek_to_first()
        total = 0
        mismatches = 0

        while iterator.valid():
            key = iterator.key()
            value = _decompress(iterator.value())
            node_hash = keccak(value)

            if verify:
                expected = key[-32:]
                if expected != node_hash:
                    mismatches += 1
                    if verbose:
                        print(
                            f"[warn] hash mismatch for key {key.hex()}",
                            file=sys.stderr,
                        )
                    iterator.next()
                    continue

            # keep the latest observation for a hash (pruning may leave duplicates)
            nodes[node_hash] = value
            iterator.next()
            total += 1

        if verbose:
            print(
                f"[info] indexed {total} entries from state DB (unique hashes: {len(nodes)}, mismatches: {mismatches})",
                file=sys.stderr,
            )

    return nodes


@dataclass
class ResolvedRoot:
    block_number: int
    state_root: bytes


def find_state_root(
    headers_path: str,
    nodes: Dict[bytes, bytes],
    block_number: Optional[int] = None,
    verbose: bool = False,
) -> ResolvedRoot:
    options = Options(raw_mode=True)
    with Rdict(headers_path, options=options, access_type=AccessType.read_only()) as db:
        iterator = db.iter()

        if block_number is None:
            iterator.seek_to_last()
            while iterator.valid():
                header = rlp.decode(iterator.value())
                candidate = header[3]
                if candidate in nodes:
                    number = int.from_bytes(header[8], "big")
                    if verbose:
                        print(
                            f"[info] using block {number} with state root {candidate.hex()}",
                            file=sys.stderr,
                        )
                    return ResolvedRoot(number, candidate)
                iterator.prev()
            raise RuntimeError("Failed to locate any header whose state root is present in the state DB")

        # explicit block lookup
        seek_bytes = block_number.to_bytes(8, "big")
        iterator.seek(seek_bytes)
        while iterator.valid():
            key = iterator.key()
            number = int.from_bytes(key[:8], "big")
            if number == block_number:
                header = rlp.decode(iterator.value())
                state_root = header[3]
                if state_root not in nodes:
                    raise RuntimeError(
                        f"Requested block {block_number} has state root {state_root.hex()}, "
                        "but the node is not present in the state DB."
                    )
                if verbose:
                    print(
                        f"[info] using block {block_number} with state root {state_root.hex()}",
                        file=sys.stderr,
                    )
                return ResolvedRoot(block_number, state_root)
            iterator.next()

    raise RuntimeError(f"Header for block {block_number} not found")


def iter_storage_slots(
    nodes: Dict[bytes, bytes],
    storage_root: bytes,
    limit: Optional[int] = None,
) -> Iterator[Dict[str, object]]:
    if storage_root == EMPTY_TRIE_HASH:
        return

    root_bytes = nodes.get(storage_root)
    if root_bytes is None:
        raise KeyError(f"Storage root {storage_root.hex()} missing from node index")

    produced = 0

    def walk(node_bytes: bytes, path: List[int]) -> Iterator[Dict[str, object]]:
        nonlocal produced
        node = rlp.decode(node_bytes)

        if isinstance(node, list):
            if len(node) == 17:
                value_ref = node[16]
                if value_ref:
                    slot_value = rlp.decode(_decompress(value_ref))
                    slot_hash = _nibbles_to_bytes(path)
                    yield {
                        "slot_hash": "0x" + slot_hash.hex(),
                        "value": "0x" + slot_value.hex(),
                        "value_int": int.from_bytes(slot_value, "big") if slot_value else 0,
                    }
                    produced += 1
                    if limit is not None and produced >= limit:
                        return

                for index, child in enumerate(node[:16]):
                    if child:
                        child_bytes = _resolve_ref(child, nodes)
                        if child_bytes is None:
                            continue
                        yield from walk(child_bytes, path + [index])
                        if limit is not None and produced >= limit:
                            return

            elif len(node) == 2:
                suffix, is_leaf = _decode_hex_prefix(node[0])
                new_path = path + suffix
                child = node[1]
                if is_leaf:
                    slot_value = rlp.decode(_decompress(child))
                    slot_hash = _nibbles_to_bytes(new_path)
                    yield {
                        "slot_hash": "0x" + slot_hash.hex(),
                        "value": "0x" + slot_value.hex(),
                        "value_int": int.from_bytes(slot_value, "big") if slot_value else 0,
                    }
                    produced += 1
                else:
                    child_bytes = _resolve_ref(child, nodes)
                    if child_bytes is not None:
                        yield from walk(child_bytes, new_path)
                if limit is not None and produced >= limit:
                    return

    yield from walk(root_bytes, [])


def iter_accounts(
    nodes: Dict[bytes, bytes],
    state_root: bytes,
    include_storage: bool,
    storage_limit: Optional[int],
) -> Iterator[Dict[str, object]]:
    root_bytes = nodes.get(state_root)
    if root_bytes is None:
        raise KeyError(f"State root {state_root.hex()} missing from node index")

    def walk(node_bytes: bytes, path: List[int]) -> Iterator[Dict[str, object]]:
        node = rlp.decode(node_bytes)

        if isinstance(node, list):
            if len(node) == 17:
                value_ref = node[16]
                if value_ref:
                    account_rlp = rlp.decode(_decompress(value_ref))
                    if len(path) == 64 and isinstance(account_rlp, list) and len(account_rlp) == 4:
                        yield _build_account(path, account_rlp)
                for index, child in enumerate(node[:16]):
                    if child:
                        child_bytes = _resolve_ref(child, nodes)
                        if child_bytes is None:
                            continue
                        yield from walk(child_bytes, path + [index])
            elif len(node) == 2:
                suffix, is_leaf = _decode_hex_prefix(node[0])
                new_path = path + suffix
                child = node[1]
                if is_leaf:
                    account_rlp = rlp.decode(_decompress(child))
                    if len(new_path) == 64 and isinstance(account_rlp, list) and len(account_rlp) == 4:
                        yield _build_account(new_path, account_rlp)
                else:
                    child_bytes = _resolve_ref(child, nodes)
                    if child_bytes is not None:
                        yield from walk(child_bytes, new_path)

    def _build_account(path_nibbles: List[int], account_rlp: List[bytes]) -> Dict[str, object]:
        address_hash = _nibbles_to_bytes(path_nibbles)
        nonce = int.from_bytes(account_rlp[0], "big")
        balance = int.from_bytes(account_rlp[1], "big")
        storage_root = account_rlp[2]
        code_hash = account_rlp[3]

        account: Dict[str, object] = {
            "address_hash": "0x" + address_hash.hex(),
            "nonce": nonce,
            "balance": str(balance),
            "storage_root": "0x" + storage_root.hex(),
            "code_hash": "0x" + code_hash.hex(),
        }

        if include_storage and storage_root != EMPTY_TRIE_HASH:
            try:
                slots = list(iter_storage_slots(nodes, storage_root, storage_limit))
            except KeyError:
                slots = []
            if slots:
                account["storage"] = slots

        return account

    yield from walk(root_bytes, [])


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract accounts and storage slots from a Nethermind RocksDB state database."
    )
    parser.add_argument(
        "--state-db",
        required=True,
        help="Path to the Nethermind state RocksDB column family (e.g. .../state/0)",
    )
    parser.add_argument(
        "--headers-db",
        required=True,
        help="Path to the Nethermind headers RocksDB (used to locate the latest state root).",
    )
    parser.add_argument(
        "--block-number",
        type=int,
        help="Optional block number to pin the state root. If omitted, the latest available root is used.",
    )
    parser.add_argument(
        "--include-storage",
        action="store_true",
        help="Traverse per-account storage tries and include slot values.",
    )
    parser.add_argument(
        "--storage-limit",
        type=int,
        default=None,
        help="Maximum number of storage slots to collect per account (omit for all).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of accounts emitted.",
    )
    parser.add_argument(
        "--output",
        help="Optional path to write JSON Lines output. Defaults to stdout.",
    )
    parser.add_argument(
        "--no-verify",
        action="store_true",
        help="Skip verifying that node hashes match the key suffix (faster but unsafe).",
    )
    parser.add_argument("--verbose", action="store_true", help="Emit progress information to stderr.")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)

    nodes = build_node_index(
        state_path=args.state_db,
        verify=not args.no_verify,
        verbose=args.verbose,
    )

    resolved_root = find_state_root(
        headers_path=args.headers_db,
        nodes=nodes,
        block_number=args.block_number,
        verbose=args.verbose,
    )

    if args.verbose:
        print(
            f"[info] extracting accounts from block {resolved_root.block_number}",
            file=sys.stderr,
        )

    sys.setrecursionlimit(max(10_000, sys.getrecursionlimit()))

    account_iter = iter_accounts(
        nodes=nodes,
        state_root=resolved_root.state_root,
        include_storage=args.include_storage,
        storage_limit=args.storage_limit,
    )

    output_handle = open(args.output, "w") if args.output else sys.stdout
    emitted = 0
    try:
        for account in account_iter:
            if args.limit is not None and emitted >= args.limit:
                break
            json.dump(account, output_handle)
            output_handle.write("\n")
            emitted += 1
    finally:
        if output_handle is not sys.stdout:
            output_handle.close()

    if args.verbose:
        print(f"[info] emitted {emitted} accounts", file=sys.stderr)


if __name__ == "__main__":
    main()
