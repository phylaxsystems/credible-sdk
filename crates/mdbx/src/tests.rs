//! Integration tests for MDBX state storage.

use crate::{
    AccountState,
    AddressHash,
    BlockStateUpdate,
    CommitStats,
    Reader,
    Writer,
    common::CircularBufferConfig,
    reader::StateReader,
    writer::StateWriter,
};
use alloy::primitives::{
    Address,
    B256,
    Bytes,
    U256,
    keccak256,
};
use std::{
    collections::HashMap,
    time::Duration,
};
use tempfile::TempDir;

fn u256(v: u64) -> U256 {
    U256::from(v)
}

fn slot_b256(s: u64) -> B256 {
    B256::from(U256::from(s).to_be_bytes::<32>())
}

fn hash_slot(s: B256) -> B256 {
    keccak256(s)
}

fn addr(byte: u8) -> Address {
    Address::repeat_byte(byte)
}

fn simple_account(addr: Address, bal: u64, nonce: u64) -> AccountState {
    AccountState {
        address_hash: AddressHash(keccak256(addr)),
        balance: u256(bal),
        nonce,
        code_hash: B256::ZERO,
        code: None,
        storage: HashMap::new(),
        deleted: false,
    }
}

fn hash_storage(storage: HashMap<B256, U256>) -> HashMap<B256, U256> {
    storage
        .into_iter()
        .map(|(k, v)| (hash_slot(k), v))
        .collect()
}

/// Storage builder for cleaner test setup
fn storage(slots: impl IntoIterator<Item = (u64, u64)>) -> HashMap<B256, U256> {
    slots
        .into_iter()
        .map(|(k, v)| (slot_b256(k), u256(v)))
        .collect()
}

/// Simple update with no storage or code
fn simple_update(block: u64, addr: Address, bal: u64, nonce: u64) -> BlockStateUpdate {
    test_update(block, B256::ZERO, addr, bal, nonce, HashMap::new(), None)
}

fn test_update(
    block: u64,
    root: B256,
    addr: Address,
    bal: u64,
    nonce: u64,
    storage: HashMap<B256, U256>,
    code: Option<Vec<u8>>,
) -> BlockStateUpdate {
    let code_hash = code.as_ref().map_or(B256::ZERO, keccak256);
    BlockStateUpdate {
        block_number: block,
        state_root: root,
        block_hash: B256::repeat_byte(u8::try_from(block).unwrap_or(0xff)),
        accounts: vec![AccountState {
            address_hash: AddressHash(keccak256(addr)),
            balance: u256(bal),
            nonce,
            code_hash,
            code: code.map(Bytes::from),
            storage: hash_storage(storage),
            deleted: false,
        }],
    }
}

fn update_with_storage(
    block: u64,
    addr: Address,
    bal: u64,
    nonce: u64,
    slots: impl IntoIterator<Item = (u64, u64)>,
) -> BlockStateUpdate {
    test_update(block, B256::ZERO, addr, bal, nonce, storage(slots), None)
}

fn account_state(addr: Address, bal: u64, nonce: u64) -> AccountState {
    AccountState {
        address_hash: AddressHash(keccak256(addr)),
        balance: u256(bal),
        nonce,
        code_hash: B256::ZERO,
        code: None,
        storage: HashMap::new(),
        deleted: false,
    }
}

fn block_update(block: u64, accounts: Vec<AccountState>) -> BlockStateUpdate {
    BlockStateUpdate {
        block_number: block,
        block_hash: B256::ZERO,
        state_root: B256::ZERO,
        accounts,
    }
}

/// Create writer only - reader created separately after writes
fn writer_env(buf: u8) -> (StateWriter, TempDir) {
    let tmp = TempDir::new().unwrap();
    let cfg = CircularBufferConfig::new(buf).unwrap();
    let w = StateWriter::new(tmp.path().join("state"), cfg).unwrap();
    (w, tmp)
}

/// Create reader for existing database (read-only)
fn reader_for(tmp: &TempDir, buf: u8) -> StateReader {
    let cfg = CircularBufferConfig::new(buf).unwrap();
    StateReader::new(tmp.path().join("state"), cfg).unwrap()
}

#[test]
fn test_cumulative_state_with_different_accounts() {
    let (w, tmp) = writer_env(3);

    let a1 = addr(0x11);
    w.commit_block(&simple_update(0, a1, 1000, 5)).unwrap();
    let a2 = addr(0x22);
    w.commit_block(&simple_update(1, a2, 2000, 10)).unwrap();
    let a3 = addr(0x33);
    w.commit_block(&simple_update(2, a3, 3000, 15)).unwrap();
    let a4 = addr(0x44);
    w.commit_block(&simple_update(3, a4, 4000, 20)).unwrap();
    drop(w);

    let r = reader_for(&tmp, 3);
    assert_eq!(
        r.get_account(a1.into(), 3).unwrap().unwrap().balance,
        u256(1000)
    );
    assert_eq!(
        r.get_account(a2.into(), 3).unwrap().unwrap().balance,
        u256(2000)
    );
    assert_eq!(
        r.get_account(a3.into(), 3).unwrap().unwrap().balance,
        u256(3000)
    );
    assert_eq!(
        r.get_account(a4.into(), 3).unwrap().unwrap().balance,
        u256(4000)
    );
    assert_eq!(r.latest_block_number().unwrap(), Some(3));
}

#[test]
fn test_cumulative_state_with_account_updates() {
    let (w, tmp) = writer_env(3);
    let a = addr(0x55);

    w.commit_block(&simple_update(0, a, 1000, 0)).unwrap();
    w.commit_block(&simple_update(1, a, 1500, 1)).unwrap();
    w.commit_block(&simple_update(2, a, 1200, 2)).unwrap();
    w.commit_block(&simple_update(3, a, 2000, 3)).unwrap();
    drop(w);

    let r = reader_for(&tmp, 3);
    let acc = r.get_account(a.into(), 3).unwrap().unwrap();
    assert_eq!(acc.balance, u256(2000));
    assert_eq!(acc.nonce, 3);
}

#[test]
fn test_cumulative_storage_updates() {
    let (w, tmp) = writer_env(3);
    let a = addr(0x66);

    w.commit_block(&update_with_storage(0, a, 1000, 0, [(1, 100)]))
        .unwrap();
    w.commit_block(&update_with_storage(1, a, 1000, 1, [(2, 200)]))
        .unwrap();
    w.commit_block(&update_with_storage(2, a, 1000, 2, [(1, 150)]))
        .unwrap();
    w.commit_block(&update_with_storage(3, a, 1000, 3, [(3, 300)]))
        .unwrap();
    drop(w);

    let r = reader_for(&tmp, 3);
    let s = r.get_all_storage(a.into(), 3).unwrap();
    assert_eq!(s.len(), 3);
    assert_eq!(s.get(&hash_slot(slot_b256(1))), Some(&u256(150)));
    assert_eq!(s.get(&hash_slot(slot_b256(2))), Some(&u256(200)));
    assert_eq!(s.get(&hash_slot(slot_b256(3))), Some(&u256(300)));
}

#[test]
fn test_single_block() {
    let (w, tmp) = writer_env(3);
    let a = addr(0x11);

    assert_eq!(w.latest_block_number().unwrap(), None);
    w.commit_block(&simple_update(0, a, 1000, 5)).unwrap();
    assert_eq!(w.latest_block_number().unwrap(), Some(0));
    drop(w);

    let r = reader_for(&tmp, 3);
    let acc = r.get_account(a.into(), 0).unwrap().unwrap();
    assert_eq!(acc.balance, u256(1000));
    assert_eq!(acc.nonce, 5);
}

#[test]
fn test_large_scale_rotation() {
    let (w, tmp) = writer_env(5);
    let a = addr(0xcc);

    for b in 0..20 {
        w.commit_block(&simple_update(b, a, b * 10, b)).unwrap();
    }
    drop(w);

    let r = reader_for(&tmp, 5);
    assert_eq!(r.latest_block_number().unwrap(), Some(19));
    assert_eq!(
        r.get_account(a.into(), 15).unwrap().unwrap().balance,
        u256(150)
    );
    assert_eq!(
        r.get_account(a.into(), 19).unwrap().unwrap().balance,
        u256(190)
    );
}

#[test]
fn test_zero_storage_deleted() {
    let (w, tmp) = writer_env(3);
    let a = addr(0xaa);

    w.commit_block(&update_with_storage(
        0,
        a,
        1000,
        0,
        [(1, 100), (2, 200), (3, 300)],
    ))
    .unwrap();
    w.commit_block(&test_update(
        1,
        B256::ZERO,
        a,
        1000,
        1,
        HashMap::from([(slot_b256(2), U256::ZERO)]),
        None,
    ))
    .unwrap();
    w.commit_block(&test_update(
        2,
        B256::ZERO,
        a,
        1000,
        2,
        HashMap::from([(slot_b256(1), U256::ZERO), (slot_b256(3), U256::ZERO)]),
        None,
    ))
    .unwrap();
    drop(w);

    let r = reader_for(&tmp, 3);
    let s1 = r.get_all_storage(a.into(), 1).unwrap();
    assert_eq!(s1.len(), 2);
    assert!(!s1.contains_key(&hash_slot(slot_b256(2))));
    assert_eq!(r.get_all_storage(a.into(), 2).unwrap().len(), 0);
}

#[test]
fn test_roundtrip_basic() {
    let (w, tmp) = writer_env(3);
    let a = addr(0xaa);

    w.commit_block(&simple_update(0, a, 1000, 5)).unwrap();
    drop(w);

    let r = reader_for(&tmp, 3);
    let acc = r.get_full_account(a.into(), 0).unwrap().unwrap();
    assert_eq!(acc.address_hash, AddressHash(keccak256(a)));
    assert_eq!(acc.balance, u256(1000));
    assert_eq!(acc.nonce, 5);
    assert!(acc.storage.is_empty());
}

#[test]
fn test_roundtrip_with_storage() {
    let (w, tmp) = writer_env(3);
    let a = addr(0xbb);

    w.commit_block(&update_with_storage(
        0,
        a,
        5000,
        10,
        [(1, 100), (2, 200), (3, 300)],
    ))
    .unwrap();
    drop(w);

    let r = reader_for(&tmp, 3);
    let acc = r.get_full_account(a.into(), 0).unwrap().unwrap();
    assert_eq!(acc.balance, u256(5000));
    assert_eq!(acc.storage.len(), 3);
    assert_eq!(
        r.get_storage(a.into(), hash_slot(slot_b256(2)), 0).unwrap(),
        Some(u256(200))
    );
}

#[test]
fn test_roundtrip_with_code() {
    let (w, tmp) = writer_env(3);
    let a = addr(0xcc);
    let code = vec![0x60, 0x80, 0x60, 0x40, 0x52];

    w.commit_block(&test_update(
        0,
        B256::ZERO,
        a,
        0,
        1,
        HashMap::new(),
        Some(code.clone()),
    ))
    .unwrap();
    drop(w);

    let r = reader_for(&tmp, 3);
    let acc = r.get_account(a.into(), 0).unwrap().unwrap();
    assert_eq!(acc.code_hash, keccak256(&code));
    assert_eq!(
        r.get_code(keccak256(&code), 0).unwrap(),
        Some(Bytes::from(code))
    );
}

#[test]
fn test_circular_buffer_rotation() {
    let (w, tmp) = writer_env(3);
    let a = addr(0xdd);

    for b in 0..6 {
        w.commit_block(&simple_update(b, a, b * 100, b)).unwrap();
    }
    drop(w);

    let r = reader_for(&tmp, 3);
    assert_eq!(r.latest_block_number().unwrap(), Some(5));
    assert!(!r.is_block_available(2).unwrap());
    assert!(r.is_block_available(3).unwrap());
    assert_eq!(
        r.get_account(a.into(), 3).unwrap().unwrap().balance,
        u256(300)
    );
    assert_eq!(
        r.get_account(a.into(), 5).unwrap().unwrap().balance,
        u256(500)
    );
    assert!(r.get_account(a.into(), 0).is_err());
}

#[test]
fn test_cumulative_state_reads() {
    let (writer, temp_dir) = writer_env(3);
    let (addr_a, addr_b, addr_c) = (addr(0xa1), addr(0xb1), addr(0xc1));

    writer
        .commit_block(&simple_update(0, addr_a, 1000, 1))
        .unwrap();
    writer
        .commit_block(&simple_update(1, addr_b, 2000, 2))
        .unwrap();
    writer
        .commit_block(&simple_update(2, addr_c, 3000, 3))
        .unwrap();
    writer
        .commit_block(&block_update(3, vec![account_state(addr_a, 1500, 5)]))
        .unwrap();
    drop(writer);

    let reader = reader_for(&temp_dir, 3);
    assert_eq!(
        reader
            .get_account(addr_a.into(), 3)
            .unwrap()
            .unwrap()
            .balance,
        u256(1500)
    );
    assert_eq!(
        reader
            .get_account(addr_b.into(), 3)
            .unwrap()
            .unwrap()
            .balance,
        u256(2000)
    );
    assert_eq!(
        reader
            .get_account(addr_c.into(), 3)
            .unwrap()
            .unwrap()
            .balance,
        u256(3000)
    );
}

#[test]
fn test_block_metadata() {
    let (w, tmp) = writer_env(5);
    let a = addr(0xee);

    for b in 0..5u64 {
        let block_hash_byte = u8::try_from(b * 10).expect("block hash byte fits in u8");
        let state_root_byte = u8::try_from(b * 20).expect("state root byte fits in u8");
        w.commit_block(&BlockStateUpdate {
            block_number: b,
            block_hash: B256::repeat_byte(block_hash_byte),
            state_root: B256::repeat_byte(state_root_byte),
            accounts: vec![account_state(a, b * 100, b)],
        })
        .unwrap();
    }
    drop(w);

    let r = reader_for(&tmp, 5);
    for b in 0..5u64 {
        let meta = r.get_block_metadata(b).unwrap().unwrap();
        let block_hash_byte = u8::try_from(b * 10).expect("block hash byte fits in u8");
        let state_root_byte = u8::try_from(b * 20).expect("state root byte fits in u8");
        assert_eq!(meta.block_hash, B256::repeat_byte(block_hash_byte));
        assert_eq!(meta.state_root, B256::repeat_byte(state_root_byte));
    }
}

#[test]
fn test_storage_evolution() {
    let (w, tmp) = writer_env(4);
    let a = addr(0xff);

    w.commit_block(&update_with_storage(
        0,
        a,
        1000,
        0,
        [(1, 100), (2, 200), (3, 300)],
    ))
    .unwrap();
    w.commit_block(&update_with_storage(1, a, 1000, 1, [(1, 150), (4, 400)]))
        .unwrap();
    w.commit_block(&test_update(
        2,
        B256::ZERO,
        a,
        1000,
        2,
        HashMap::from([(slot_b256(2), U256::ZERO), (slot_b256(3), u256(350))]),
        None,
    ))
    .unwrap();
    w.commit_block(&update_with_storage(3, a, 1000, 3, [(5, 500)]))
        .unwrap();
    drop(w);

    let r = reader_for(&tmp, 4);
    let s = r.get_all_storage(a.into(), 3).unwrap();
    assert_eq!(s.len(), 4);
    assert_eq!(s.get(&hash_slot(slot_b256(1))), Some(&u256(150)));
    assert_eq!(s.get(&hash_slot(slot_b256(2))), None);
    assert_eq!(s.get(&hash_slot(slot_b256(3))), Some(&u256(350)));
    assert_eq!(s.get(&hash_slot(slot_b256(4))), Some(&u256(400)));
    assert_eq!(s.get(&hash_slot(slot_b256(5))), Some(&u256(500)));
}

#[test]
fn test_multiple_accounts_per_block() {
    let (w, tmp) = writer_env(3);

    let accounts = (1..=3)
        .map(|i| account_state(addr(i), u64::from(i) * 1000, u64::from(i)))
        .collect();
    w.commit_block(&block_update(0, accounts)).unwrap();
    drop(w);

    let r = reader_for(&tmp, 3);
    for i in 1..=3u8 {
        let acc = r.get_account(addr(i).into(), 0).unwrap().unwrap();
        assert_eq!(acc.balance, u256(u64::from(i) * 1000));
    }
}

#[test]
fn test_available_block_range() {
    let (w, tmp) = writer_env(3);

    for b in 0..6 {
        let addr_byte = u8::try_from(b).expect("address byte fits in u8");
        w.commit_block(&simple_update(b, addr(addr_byte), 1000, 1))
            .unwrap();
    }
    drop(w);

    let r = reader_for(&tmp, 3);
    assert_eq!(r.get_available_block_range().unwrap(), Some((3, 5)));
}

#[test]
fn test_scan_account_hashes() {
    let (w, tmp) = writer_env(3);

    let accounts = (0..5)
        .map(|i| account_state(addr(i), u64::from(i) * 100, u64::from(i)))
        .collect();
    w.commit_block(&block_update(0, accounts)).unwrap();
    drop(w);

    let r = reader_for(&tmp, 3);
    assert_eq!(r.scan_account_hashes(0).unwrap().len(), 5);
}

#[test]
fn test_account_deletion() {
    let (w, tmp) = writer_env(3);
    let a = addr(0xaa);

    w.commit_block(&update_with_storage(0, a, 1000, 1, [(1, 100)]))
        .unwrap();
    w.commit_block(&BlockStateUpdate {
        block_number: 1,
        block_hash: B256::ZERO,
        state_root: B256::ZERO,
        accounts: vec![AccountState {
            address_hash: AddressHash(keccak256(a)),
            balance: U256::ZERO,
            nonce: 0,
            code_hash: B256::ZERO,
            code: None,
            storage: HashMap::new(),
            deleted: true,
        }],
    })
    .unwrap();
    drop(w);

    let r = reader_for(&tmp, 3);
    assert!(r.get_account(a.into(), 1).unwrap().is_none());
    assert!(r.get_all_storage(a.into(), 1).unwrap().is_empty());
}

#[test]
fn test_empty_database() {
    let (w, tmp) = writer_env(3);
    assert_eq!(w.latest_block_number().unwrap(), None);
    drop(w);

    let r = reader_for(&tmp, 3);
    assert_eq!(r.latest_block_number().unwrap(), None);
    assert_eq!(r.get_available_block_range().unwrap(), None);
    assert!(!r.is_block_available(0).unwrap());
}

#[test]
fn test_buffer_size() {
    let (w, tmp) = writer_env(5);
    drop(w);
    let r = reader_for(&tmp, 5);
    assert_eq!(r.buffer_size(), 5);
}

#[test]
fn test_consecutive_blocks() {
    let (w, tmp) = writer_env(3);
    let a = addr(0xdd);

    for b in 0..=6 {
        w.commit_block(&simple_update(b, a, b * 100, b)).unwrap();
    }
    drop(w);

    let r = reader_for(&tmp, 3);
    for b in 4..=6 {
        let acc = r.get_account(a.into(), b).unwrap().unwrap();
        assert_eq!(acc.balance, u256(b * 100));
    }
}

#[test]
fn test_namespace_isolation() {
    let (w, tmp) = writer_env(3);

    for i in 0..3u64 {
        let addr_byte = u8::try_from(i).expect("address byte fits in u8");
        w.commit_block(&simple_update(i, addr(addr_byte), 1000 * (i + 1), 1))
            .unwrap();
    }
    drop(w);

    let r = reader_for(&tmp, 3);
    for i in 0..3u64 {
        let addr_byte = u8::try_from(i).expect("address byte fits in u8");
        assert_eq!(
            r.get_account(addr(addr_byte).into(), i)
                .unwrap()
                .unwrap()
                .balance,
            u256(1000 * (i + 1))
        );
    }
}

#[test]
fn test_code_deduplication() {
    let (w, tmp) = writer_env(3);
    let code = vec![0x60, 0x80, 0x60, 0x40, 0x52];
    let code_hash = keccak256(&code);

    w.commit_block(&block_update(
        0,
        vec![
            AccountState {
                address_hash: AddressHash(keccak256(addr(0x01))),
                balance: u256(1000),
                nonce: 1,
                code_hash,
                code: Some(Bytes::from(code.clone())),
                storage: HashMap::new(),
                deleted: false,
            },
            AccountState {
                address_hash: AddressHash(keccak256(addr(0x02))),
                balance: u256(2000),
                nonce: 2,
                code_hash,
                code: Some(Bytes::from(code.clone())),
                storage: HashMap::new(),
                deleted: false,
            },
        ],
    ))
    .unwrap();
    drop(w);

    let r = reader_for(&tmp, 3);
    assert_eq!(r.get_code(code_hash, 0).unwrap(), Some(Bytes::from(code)));
    assert_eq!(
        r.get_account(addr(0x01).into(), 0)
            .unwrap()
            .unwrap()
            .code_hash,
        code_hash
    );
    assert_eq!(
        r.get_account(addr(0x02).into(), 0)
            .unwrap()
            .unwrap()
            .code_hash,
        code_hash
    );
}

#[test]
fn test_large_storage() {
    let (w, tmp) = writer_env(3);
    let a = addr(0xaa);
    // Use i+1 to avoid zero value (zero = delete in Ethereum semantics)
    let slots: Vec<_> = (0..100u64).map(|i| (i, i + 1)).collect();

    w.commit_block(&update_with_storage(0, a, 1000, 1, slots))
        .unwrap();
    drop(w);

    let r = reader_for(&tmp, 3);
    let all = r.get_all_storage(a.into(), 0).unwrap();
    assert_eq!(all.len(), 100);
    for i in 0..100u64 {
        assert_eq!(
            r.get_storage(a.into(), hash_slot(slot_b256(i)), 0).unwrap(),
            Some(u256(i + 1))
        );
    }
}

#[test]
fn test_block_not_found_error() {
    let (w, tmp) = writer_env(3);
    let a = addr(0xaa);

    w.commit_block(&simple_update(0, a, 1000, 1)).unwrap();
    drop(w);

    let r = reader_for(&tmp, 3);
    match r.get_account(a.into(), 100) {
        Err(crate::common::error::StateError::BlockNotFound { block_number, .. }) => {
            assert_eq!(block_number, 100);
        }
        _ => panic!("Expected BlockNotFound error"),
    }
}

fn account_state_with_storage(
    addr: Address,
    bal: u64,
    nonce: u64,
    slots: impl IntoIterator<Item = (u64, u64)>,
) -> AccountState {
    AccountState {
        address_hash: AddressHash(keccak256(addr)),
        balance: u256(bal),
        nonce,
        code_hash: B256::ZERO,
        code: None,
        storage: hash_storage(storage(slots)),
        deleted: false,
    }
}

fn deleted_account(addr: Address) -> AccountState {
    AccountState {
        address_hash: AddressHash(keccak256(addr)),
        balance: U256::ZERO,
        nonce: 0,
        code_hash: B256::ZERO,
        code: None,
        storage: HashMap::new(),
        deleted: true,
    }
}

#[test]
fn test_state_continuity_across_all_namespaces() {
    // Verifies state is correctly copied when writing to each new namespace
    let (w, tmp) = writer_env(3);
    let a = addr(0xaa);

    // Block 0 -> namespace 0
    w.commit_block(&update_with_storage(0, a, 1000, 1, [(1, 100), (2, 200)]))
        .unwrap();

    // Block 1 -> namespace 1 (must copy from ns 0)
    w.commit_block(&simple_update(1, a, 1100, 2)).unwrap();

    // Block 2 -> namespace 2 (must copy from ns 1)
    w.commit_block(&simple_update(2, a, 1200, 3)).unwrap();

    drop(w);
    let r = reader_for(&tmp, 3);

    // All blocks should have the storage from block 0
    for block in 0..3 {
        let s = r.get_all_storage(a.into(), block).unwrap();
        assert_eq!(s.len(), 2, "Block {block} should have 2 storage slots");
        assert_eq!(s.get(&hash_slot(slot_b256(1))), Some(&u256(100)));
        assert_eq!(s.get(&hash_slot(slot_b256(2))), Some(&u256(200)));
    }
}

#[test]
fn test_storage_delete_then_recreate_same_slot() {
    let (w, tmp) = writer_env(4);
    let a = addr(0xbb);

    // Block 0: Create slot 1 with value 100
    w.commit_block(&update_with_storage(0, a, 1000, 0, [(1, 100)]))
        .unwrap();

    // Block 1: Delete slot 1
    w.commit_block(&test_update(
        1,
        B256::ZERO,
        a,
        1000,
        1,
        HashMap::from([(slot_b256(1), U256::ZERO)]),
        None,
    ))
    .unwrap();

    // Block 2: Recreate slot 1 with different value
    w.commit_block(&update_with_storage(2, a, 1000, 2, [(1, 999)]))
        .unwrap();

    // Block 3: Just update balance to test persistence
    w.commit_block(&simple_update(3, a, 2000, 3)).unwrap();

    drop(w);
    let r = reader_for(&tmp, 4);

    // Block 0: slot exists
    assert_eq!(
        r.get_storage(a.into(), hash_slot(slot_b256(1)), 0).unwrap(),
        Some(u256(100))
    );

    // Block 1: slot deleted
    assert_eq!(
        r.get_storage(a.into(), hash_slot(slot_b256(1)), 1).unwrap(),
        None
    );

    // Block 2: slot recreated with new value
    assert_eq!(
        r.get_storage(a.into(), hash_slot(slot_b256(1)), 2).unwrap(),
        Some(u256(999))
    );

    // Block 3: slot persists
    assert_eq!(
        r.get_storage(a.into(), hash_slot(slot_b256(1)), 3).unwrap(),
        Some(u256(999))
    );
}

#[test]
fn test_account_delete_then_recreate() {
    let (w, tmp) = writer_env(4);
    let a = addr(0xcc);
    let code0 = vec![0x60, 0x80, 0x60, 0x40];
    let code0_hash = keccak256(&code0);
    let code2 = vec![0x61, 0x01, 0x60, 0x00];
    let code2_hash = keccak256(&code2);

    // Block 0: Create account with storage
    w.commit_block(&test_update(
        0,
        B256::ZERO,
        a,
        1000,
        1,
        storage([(1, 100), (2, 200)]),
        Some(code0.clone()),
    ))
    .unwrap();

    // Block 1: Delete account
    w.commit_block(&block_update(1, vec![deleted_account(a)]))
        .unwrap();

    // Block 2: Recreate account with different storage
    w.commit_block(&test_update(
        2,
        B256::ZERO,
        a,
        5000,
        0,
        storage([(3, 300)]),
        Some(code2.clone()),
    ))
    .unwrap();

    // Block 3: Update to test persistence
    w.commit_block(&test_update(
        3,
        B256::ZERO,
        a,
        5500,
        1,
        HashMap::new(),
        Some(code2.clone()),
    ))
    .unwrap();

    drop(w);
    let r = reader_for(&tmp, 4);

    // Block 0: original state
    assert_eq!(
        r.get_account(a.into(), 0).unwrap().unwrap().balance,
        u256(1000)
    );
    assert_eq!(
        r.get_account(a.into(), 0).unwrap().unwrap().code_hash,
        code0_hash
    );
    assert_eq!(r.get_code(code0_hash, 0).unwrap(), Some(Bytes::from(code0)));
    let s0 = r.get_all_storage(a.into(), 0).unwrap();
    assert_eq!(s0.len(), 2);

    // Block 1: deleted
    assert!(r.get_account(a.into(), 1).unwrap().is_none());
    assert!(r.get_all_storage(a.into(), 1).unwrap().is_empty());

    // Block 2: recreated - should NOT have old storage
    let acc2 = r.get_account(a.into(), 2).unwrap().unwrap();
    assert_eq!(acc2.balance, u256(5000));
    assert_eq!(acc2.nonce, 0);
    assert_eq!(acc2.code_hash, code2_hash);
    assert_eq!(
        r.get_code(code2_hash, 2).unwrap(),
        Some(Bytes::from(code2.clone()))
    );
    let s2 = r.get_all_storage(a.into(), 2).unwrap();
    assert_eq!(s2.len(), 1);
    assert_eq!(s2.get(&hash_slot(slot_b256(3))), Some(&u256(300)));
    assert_eq!(s2.get(&hash_slot(slot_b256(1))), None); // Old storage gone

    // Block 3: persists
    let acc3 = r.get_account(a.into(), 3).unwrap().unwrap();
    assert_eq!(acc3.balance, u256(5500));
    assert_eq!(acc3.code_hash, code2_hash);
    let s3 = r.get_all_storage(a.into(), 3).unwrap();
    assert_eq!(s3.len(), 1);
}

#[test]
fn test_multiple_accounts_interleaved_updates() {
    let (writer, temp_dir) = writer_env(4);
    let (addr_a, addr_b, addr_c) = (addr(0x11), addr(0x22), addr(0x33));

    // Block 0: Create A and B
    writer
        .commit_block(&block_update(
            0,
            vec![
                account_state_with_storage(addr_a, 1000, 0, [(1, 100)]),
                account_state_with_storage(addr_b, 2000, 0, [(1, 200)]),
            ],
        ))
        .unwrap();

    // Block 1: Update A, create C
    writer
        .commit_block(&block_update(
            1,
            vec![
                account_state_with_storage(addr_a, 1100, 1, [(2, 150)]),
                account_state_with_storage(addr_c, 3000, 0, [(1, 300)]),
            ],
        ))
        .unwrap();

    // Block 2: Delete B, update C
    writer
        .commit_block(&block_update(
            2,
            vec![
                deleted_account(addr_b),
                account_state_with_storage(addr_c, 3500, 1, [(2, 350)]),
            ],
        ))
        .unwrap();

    // Block 3: Recreate B, update A
    writer
        .commit_block(&block_update(
            3,
            vec![
                account_state_with_storage(addr_a, 1200, 2, [(1, 120)]),
                account_state_with_storage(addr_b, 9000, 0, [(5, 500)]),
            ],
        ))
        .unwrap();

    drop(writer);
    let reader = reader_for(&temp_dir, 4);

    // Verify final state at block 3
    // A: balance 1200, nonce 2, slots 1=120, 2=150
    let acc_a = reader.get_account(addr_a.into(), 3).unwrap().unwrap();
    assert_eq!(acc_a.balance, u256(1200));
    assert_eq!(acc_a.nonce, 2);
    let s_a = reader.get_all_storage(addr_a.into(), 3).unwrap();
    assert_eq!(s_a.len(), 2);
    assert_eq!(s_a.get(&hash_slot(slot_b256(1))), Some(&u256(120)));
    assert_eq!(s_a.get(&hash_slot(slot_b256(2))), Some(&u256(150)));

    // B: recreated with balance 9000, nonce 0, slot 5=500
    let acc_b = reader.get_account(addr_b.into(), 3).unwrap().unwrap();
    assert_eq!(acc_b.balance, u256(9000));
    assert_eq!(acc_b.nonce, 0);
    let s_b = reader.get_all_storage(addr_b.into(), 3).unwrap();
    assert_eq!(s_b.len(), 1);
    assert_eq!(s_b.get(&hash_slot(slot_b256(5))), Some(&u256(500)));
    // Old slot 1 should NOT exist
    assert_eq!(s_b.get(&hash_slot(slot_b256(1))), None);

    // C: balance 3500, nonce 1, slots 1=300, 2=350
    let acc_c = reader.get_account(addr_c.into(), 3).unwrap().unwrap();
    assert_eq!(acc_c.balance, u256(3500));
    let s_c = reader.get_all_storage(addr_c.into(), 3).unwrap();
    assert_eq!(s_c.len(), 2);
    assert_eq!(s_c.get(&hash_slot(slot_b256(1))), Some(&u256(300)));
    assert_eq!(s_c.get(&hash_slot(slot_b256(2))), Some(&u256(350)));
}

#[test]
fn test_rotation_with_intermediate_diffs() {
    // Tests that when namespace rotates, intermediate diffs are applied correctly
    let (w, tmp) = writer_env(3);
    let a = addr(0xdd);

    // Block 0 -> ns 0
    w.commit_block(&update_with_storage(0, a, 1000, 0, [(1, 100)]))
        .unwrap();

    // Block 1 -> ns 1
    w.commit_block(&update_with_storage(1, a, 1100, 1, [(2, 200)]))
        .unwrap();

    // Block 2 -> ns 2
    w.commit_block(&update_with_storage(2, a, 1200, 2, [(3, 300)]))
        .unwrap();

    // Block 3 -> ns 0 (rotation!)
    // Must apply diffs from blocks 1 and 2 to get correct cumulative state
    w.commit_block(&update_with_storage(3, a, 1300, 3, [(4, 400)]))
        .unwrap();

    drop(w);
    let r = reader_for(&tmp, 3);

    // Block 3 should have all slots: 1, 2, 3, 4
    let s = r.get_all_storage(a.into(), 3).unwrap();
    assert_eq!(s.len(), 4);
    assert_eq!(s.get(&hash_slot(slot_b256(1))), Some(&u256(100)));
    assert_eq!(s.get(&hash_slot(slot_b256(2))), Some(&u256(200)));
    assert_eq!(s.get(&hash_slot(slot_b256(3))), Some(&u256(300)));
    assert_eq!(s.get(&hash_slot(slot_b256(4))), Some(&u256(400)));
}

#[test]
fn test_rotation_with_deletes_in_intermediate() {
    // Tests that deletes in intermediate diffs are applied during rotation
    let (w, tmp) = writer_env(3);
    let a = addr(0xee);

    // Block 0 -> ns 0: create slots 1, 2, 3
    w.commit_block(&update_with_storage(
        0,
        a,
        1000,
        0,
        [(1, 100), (2, 200), (3, 300)],
    ))
    .unwrap();

    // Block 1 -> ns 1: delete slot 2
    w.commit_block(&test_update(
        1,
        B256::ZERO,
        a,
        1000,
        1,
        HashMap::from([(slot_b256(2), U256::ZERO)]),
        None,
    ))
    .unwrap();

    // Block 2 -> ns 2: update slot 1
    w.commit_block(&update_with_storage(2, a, 1000, 2, [(1, 150)]))
        .unwrap();

    // Block 3 -> ns 0 (rotation!)
    // Must apply: block 1 delete slot 2, block 2 update slot 1
    w.commit_block(&update_with_storage(3, a, 1000, 3, [(4, 400)]))
        .unwrap();

    drop(w);
    let r = reader_for(&tmp, 3);

    // Block 3: slots 1=150, 3=300, 4=400 (slot 2 deleted)
    let s = r.get_all_storage(a.into(), 3).unwrap();
    assert_eq!(s.len(), 3);
    assert_eq!(s.get(&hash_slot(slot_b256(1))), Some(&u256(150)));
    assert_eq!(s.get(&hash_slot(slot_b256(2))), None);
    assert_eq!(s.get(&hash_slot(slot_b256(3))), Some(&u256(300)));
    assert_eq!(s.get(&hash_slot(slot_b256(4))), Some(&u256(400)));
}

#[test]
fn test_rotation_with_account_delete_in_intermediate() {
    let (w, tmp) = writer_env(3);
    let a = addr(0xff);

    // Block 0 -> ns 0
    w.commit_block(&update_with_storage(0, a, 1000, 0, [(1, 100)]))
        .unwrap();

    // Block 1 -> ns 1: delete account
    w.commit_block(&block_update(1, vec![deleted_account(a)]))
        .unwrap();

    // Block 2 -> ns 2: recreate account
    w.commit_block(&update_with_storage(2, a, 5000, 0, [(2, 200)]))
        .unwrap();

    // Block 3 -> ns 0 (rotation!)
    w.commit_block(&simple_update(3, a, 5500, 1)).unwrap();

    drop(w);
    let r = reader_for(&tmp, 3);

    // Block 3: account exists with new state, old storage gone
    let acc = r.get_account(a.into(), 3).unwrap().unwrap();
    assert_eq!(acc.balance, u256(5500));
    assert_eq!(acc.nonce, 1);

    let s = r.get_all_storage(a.into(), 3).unwrap();
    assert_eq!(s.len(), 1);
    assert_eq!(s.get(&hash_slot(slot_b256(2))), Some(&u256(200)));
    assert_eq!(s.get(&hash_slot(slot_b256(1))), None); // Old slot gone
}

#[test]
fn test_complex_multi_rotation_scenario() {
    // Many rotations with various operations
    let (w, tmp) = writer_env(3);
    let a = addr(0x01);

    // Fill first rotation
    w.commit_block(&update_with_storage(0, a, 100, 0, [(1, 10)]))
        .unwrap();
    w.commit_block(&update_with_storage(1, a, 200, 1, [(2, 20)]))
        .unwrap();
    w.commit_block(&update_with_storage(2, a, 300, 2, [(3, 30)]))
        .unwrap();

    // Second rotation
    w.commit_block(&update_with_storage(3, a, 400, 3, [(1, 40)]))
        .unwrap(); // update slot 1
    w.commit_block(&test_update(
        4,
        B256::ZERO,
        a,
        500,
        4,
        HashMap::from([(slot_b256(2), U256::ZERO)]),
        None,
    ))
    .unwrap(); // delete slot 2
    w.commit_block(&update_with_storage(5, a, 600, 5, [(4, 50)]))
        .unwrap(); // add slot 4

    // Third rotation
    w.commit_block(&update_with_storage(6, a, 700, 6, [(5, 60)]))
        .unwrap();

    drop(w);
    let r = reader_for(&tmp, 3);

    // Final state at block 6: balance 700, nonce 6
    // Storage: slot 1=40, slot 3=30, slot 4=50, slot 5=60 (slot 2 deleted)
    let acc = r.get_account(a.into(), 6).unwrap().unwrap();
    assert_eq!(acc.balance, u256(700));
    assert_eq!(acc.nonce, 6);

    let s = r.get_all_storage(a.into(), 6).unwrap();
    assert_eq!(s.len(), 4);
    assert_eq!(s.get(&hash_slot(slot_b256(1))), Some(&u256(40)));
    assert_eq!(s.get(&hash_slot(slot_b256(2))), None);
    assert_eq!(s.get(&hash_slot(slot_b256(3))), Some(&u256(30)));
    assert_eq!(s.get(&hash_slot(slot_b256(4))), Some(&u256(50)));
    assert_eq!(s.get(&hash_slot(slot_b256(5))), Some(&u256(60)));
}

#[test]
fn test_delete_all_storage_one_by_one() {
    let (w, tmp) = writer_env(5);
    let a = addr(0x02);

    w.commit_block(&update_with_storage(
        0,
        a,
        1000,
        0,
        [(1, 100), (2, 200), (3, 300)],
    ))
    .unwrap();

    // Delete one slot per block
    w.commit_block(&test_update(
        1,
        B256::ZERO,
        a,
        1000,
        1,
        HashMap::from([(slot_b256(1), U256::ZERO)]),
        None,
    ))
    .unwrap();

    w.commit_block(&test_update(
        2,
        B256::ZERO,
        a,
        1000,
        2,
        HashMap::from([(slot_b256(2), U256::ZERO)]),
        None,
    ))
    .unwrap();

    w.commit_block(&test_update(
        3,
        B256::ZERO,
        a,
        1000,
        3,
        HashMap::from([(slot_b256(3), U256::ZERO)]),
        None,
    ))
    .unwrap();

    w.commit_block(&simple_update(4, a, 1000, 4)).unwrap();

    drop(w);
    let r = reader_for(&tmp, 5);

    assert_eq!(r.get_all_storage(a.into(), 0).unwrap().len(), 3);
    assert_eq!(r.get_all_storage(a.into(), 1).unwrap().len(), 2);
    assert_eq!(r.get_all_storage(a.into(), 2).unwrap().len(), 1);
    assert_eq!(r.get_all_storage(a.into(), 3).unwrap().len(), 0);
    assert_eq!(r.get_all_storage(a.into(), 4).unwrap().len(), 0);
}

#[test]
fn test_same_slot_modified_every_block() {
    let (w, tmp) = writer_env(5);
    let a = addr(0x03);

    for b in 0..10u64 {
        w.commit_block(&update_with_storage(b, a, 1000 + b, b, [(1, b * 100)]))
            .unwrap();
    }

    drop(w);
    let r = reader_for(&tmp, 5);

    // Only blocks 5-9 available
    for b in 5..10u64 {
        let s = r.get_all_storage(a.into(), b).unwrap();
        assert_eq!(
            s.get(&hash_slot(slot_b256(1))),
            Some(&u256(b * 100)),
            "Block {} should have slot 1 = {}",
            b,
            b * 100
        );
    }
}

#[test]
fn test_preserve_unmodified_accounts_across_rotation() {
    let (writer, temp_dir) = writer_env(3);
    let (addr_a, addr_b, addr_c) = (addr(0x10), addr(0x20), addr(0x30));

    // Block 0: create all three accounts
    writer
        .commit_block(&block_update(
            0,
            vec![
                account_state_with_storage(addr_a, 1000, 0, [(1, 100)]),
                account_state_with_storage(addr_b, 2000, 0, [(2, 200)]),
                account_state_with_storage(addr_c, 3000, 0, [(3, 300)]),
            ],
        ))
        .unwrap();

    // Block 1: only modify A
    writer
        .commit_block(&simple_update(1, addr_a, 1100, 1))
        .unwrap();

    // Block 2: only modify B
    writer
        .commit_block(&simple_update(2, addr_b, 2200, 1))
        .unwrap();

    // Block 3: rotation to ns 0, only modify C
    writer
        .commit_block(&simple_update(3, addr_c, 3300, 1))
        .unwrap();

    drop(writer);
    let reader = reader_for(&temp_dir, 3);

    // All accounts should exist at block 3 with their latest state
    assert_eq!(
        reader
            .get_account(addr_a.into(), 3)
            .unwrap()
            .unwrap()
            .balance,
        u256(1100)
    );
    assert_eq!(
        reader
            .get_account(addr_b.into(), 3)
            .unwrap()
            .unwrap()
            .balance,
        u256(2200)
    );
    assert_eq!(
        reader
            .get_account(addr_c.into(), 3)
            .unwrap()
            .unwrap()
            .balance,
        u256(3300)
    );

    // Storage should be preserved
    assert_eq!(
        reader
            .get_storage(addr_a.into(), hash_slot(slot_b256(1)), 3)
            .unwrap(),
        Some(u256(100))
    );
    assert_eq!(
        reader
            .get_storage(addr_b.into(), hash_slot(slot_b256(2)), 3)
            .unwrap(),
        Some(u256(200))
    );
    assert_eq!(
        reader
            .get_storage(addr_c.into(), hash_slot(slot_b256(3)), 3)
            .unwrap(),
        Some(u256(300))
    );
}

#[test]
fn test_code_persistence_across_rotation() {
    let (w, tmp) = writer_env(3);
    let a = addr(0x40);
    let code = vec![0x60, 0x80, 0x60, 0x40, 0x52, 0x60, 0x00];
    let code_hash = keccak256(&code);

    // Block 0: deploy contract
    w.commit_block(&block_update(
        0,
        vec![AccountState {
            address_hash: AddressHash(keccak256(a)),
            balance: u256(0),
            nonce: 1,
            code_hash,
            code: Some(Bytes::from(code.clone())),
            storage: hash_storage(storage([(1, 100)])),
            deleted: false,
        }],
    ))
    .unwrap();

    // Blocks 1-3: just update storage, no code changes
    // Must preserve code_hash when updating contract accounts
    w.commit_block(&block_update(
        1,
        vec![AccountState {
            address_hash: AddressHash(keccak256(a)),
            balance: u256(0),
            nonce: 2,
            code_hash,
            code: None,
            storage: hash_storage(storage([(2, 200)])),
            deleted: false,
        }],
    ))
    .unwrap();
    w.commit_block(&block_update(
        2,
        vec![AccountState {
            address_hash: AddressHash(keccak256(a)),
            balance: u256(0),
            nonce: 3,
            code_hash,
            code: None,
            storage: hash_storage(storage([(3, 300)])),
            deleted: false,
        }],
    ))
    .unwrap();
    w.commit_block(&block_update(
        3,
        vec![AccountState {
            address_hash: AddressHash(keccak256(a)),
            balance: u256(0),
            nonce: 4,
            code_hash,
            code: None,
            storage: hash_storage(storage([(4, 400)])),
            deleted: false,
        }],
    ))
    .unwrap();

    drop(w);
    let r = reader_for(&tmp, 3);

    // Code should still be retrievable at block 3
    assert_eq!(
        r.get_code(code_hash, 3).unwrap(),
        Some(Bytes::from(code.clone()))
    );

    // Account should have correct code_hash
    let acc = r.get_account(a.into(), 3).unwrap().unwrap();
    assert_eq!(acc.code_hash, code_hash);
}

#[test]
fn test_many_accounts_stress() {
    let (w, tmp) = writer_env(5);

    // Create 50 accounts in block 0
    let accounts: Vec<_> = (0..50u8)
        .map(|i| account_state_with_storage(addr(i), u64::from(i) * 100, 0, [(1, u64::from(i))]))
        .collect();
    w.commit_block(&block_update(0, accounts)).unwrap();

    // Update half of them in subsequent blocks
    for b in 1..10u64 {
        let accounts: Vec<_> = (0..25u8)
            .map(|i| {
                account_state_with_storage(
                    addr(i),
                    u64::from(i) * 100 + b,
                    b,
                    [(1, u64::from(i) + b)],
                )
            })
            .collect();
        w.commit_block(&block_update(b, accounts)).unwrap();
    }

    drop(w);
    let r = reader_for(&tmp, 5);

    // Verify final state
    // Accounts 0-24: modified, balance = i*100 + 9, nonce = 9, slot 1 = i + 9
    // Accounts 25-49: unmodified, balance = i*100, nonce = 0, slot 1 = i
    for i in 0..50u8 {
        let acc = r.get_account(addr(i).into(), 9).unwrap().unwrap();
        let s = r.get_all_storage(addr(i).into(), 9).unwrap();

        if i < 25 {
            assert_eq!(acc.balance, u256(u64::from(i) * 100 + 9));
            assert_eq!(acc.nonce, 9);
            assert_eq!(
                s.get(&hash_slot(slot_b256(1))),
                Some(&u256(u64::from(i) + 9))
            );
        } else {
            assert_eq!(acc.balance, u256(u64::from(i) * 100));
            assert_eq!(acc.nonce, 0);
            assert_eq!(s.get(&hash_slot(slot_b256(1))), Some(&u256(u64::from(i))));
        }
    }
}

#[test]
fn test_delete_and_recreate_multiple_times() {
    let (w, tmp) = writer_env(7);
    let a = addr(0x50);

    // Create
    w.commit_block(&update_with_storage(0, a, 1000, 0, [(1, 100)]))
        .unwrap();

    // Delete
    w.commit_block(&block_update(1, vec![deleted_account(a)]))
        .unwrap();

    // Recreate
    w.commit_block(&update_with_storage(2, a, 2000, 0, [(2, 200)]))
        .unwrap();

    // Delete again
    w.commit_block(&block_update(3, vec![deleted_account(a)]))
        .unwrap();

    // Recreate again
    w.commit_block(&update_with_storage(4, a, 3000, 0, [(3, 300)]))
        .unwrap();

    // Update
    w.commit_block(&simple_update(5, a, 3500, 1)).unwrap();

    // One more update
    w.commit_block(&simple_update(6, a, 4000, 2)).unwrap();

    drop(w);
    let r = reader_for(&tmp, 7);

    // Check each block state
    assert!(r.get_account(a.into(), 0).unwrap().is_some());
    assert!(r.get_account(a.into(), 1).unwrap().is_none());
    assert!(r.get_account(a.into(), 2).unwrap().is_some());
    assert!(r.get_account(a.into(), 3).unwrap().is_none());
    assert!(r.get_account(a.into(), 4).unwrap().is_some());
    assert!(r.get_account(a.into(), 5).unwrap().is_some());
    assert!(r.get_account(a.into(), 6).unwrap().is_some());

    // Final state
    let acc = r.get_account(a.into(), 6).unwrap().unwrap();
    assert_eq!(acc.balance, u256(4000));
    assert_eq!(acc.nonce, 2);

    let s = r.get_all_storage(a.into(), 6).unwrap();
    assert_eq!(s.len(), 1);
    assert_eq!(s.get(&hash_slot(slot_b256(3))), Some(&u256(300)));
}

#[test]
fn test_storage_value_overwrite_same_block() {
    // Tests that if same slot appears multiple times in same block's diff,
    // only the last value should be used (handled by dedup)
    let (w, tmp) = writer_env(3);
    let a = addr(0x60);

    // Create account with storage via raw BlockStateUpdate
    // (normally wouldn't have duplicate slots, but tests internal handling)
    w.commit_block(&update_with_storage(0, a, 1000, 0, [(1, 100)]))
        .unwrap();

    // Next block updates the same slot
    w.commit_block(&update_with_storage(1, a, 1000, 1, [(1, 200)]))
        .unwrap();

    drop(w);
    let r = reader_for(&tmp, 3);

    assert_eq!(
        r.get_storage(a.into(), hash_slot(slot_b256(1)), 1).unwrap(),
        Some(u256(200))
    );
}

#[test]
fn test_empty_block_preserves_state() {
    let (w, tmp) = writer_env(3);
    let a = addr(0x70);

    w.commit_block(&update_with_storage(0, a, 1000, 0, [(1, 100)]))
        .unwrap();

    // Empty block (no accounts modified)
    w.commit_block(&block_update(1, vec![])).unwrap();

    w.commit_block(&simple_update(2, a, 2000, 1)).unwrap();

    drop(w);
    let r = reader_for(&tmp, 3);

    // State should persist through empty block
    assert_eq!(
        r.get_account(a.into(), 1).unwrap().unwrap().balance,
        u256(1000)
    );
    assert_eq!(
        r.get_storage(a.into(), hash_slot(slot_b256(1)), 1).unwrap(),
        Some(u256(100))
    );

    assert_eq!(
        r.get_account(a.into(), 2).unwrap().unwrap().balance,
        u256(2000)
    );
}

#[test]
fn test_rotation_boundary_exact() {
    // Tests behavior exactly at rotation boundary
    let (w, tmp) = writer_env(3);
    let a = addr(0x80);

    // Fill exactly one rotation
    w.commit_block(&update_with_storage(0, a, 100, 0, [(1, 10)]))
        .unwrap();
    w.commit_block(&update_with_storage(1, a, 200, 1, [(2, 20)]))
        .unwrap();
    w.commit_block(&update_with_storage(2, a, 300, 2, [(3, 30)]))
        .unwrap();

    // Now block 3 rotates back to namespace 0
    w.commit_block(&simple_update(3, a, 400, 3)).unwrap();

    drop(w);
    let r = reader_for(&tmp, 3);

    // Block 0 should no longer be available
    assert!(!r.is_block_available(0).unwrap());

    // Blocks 1, 2, 3 should be available
    assert!(r.is_block_available(1).unwrap());
    assert!(r.is_block_available(2).unwrap());
    assert!(r.is_block_available(3).unwrap());

    // Block 3 should have cumulative state from blocks 1 and 2
    let s = r.get_all_storage(a.into(), 3).unwrap();
    assert_eq!(s.len(), 3);
    assert_eq!(s.get(&hash_slot(slot_b256(1))), Some(&u256(10)));
    assert_eq!(s.get(&hash_slot(slot_b256(2))), Some(&u256(20)));
    assert_eq!(s.get(&hash_slot(slot_b256(3))), Some(&u256(30)));
}

#[test]
fn test_buffer_size_one() {
    // Edge case: buffer size of 1
    let (w, tmp) = writer_env(1);
    let a = addr(0x90);

    w.commit_block(&update_with_storage(0, a, 100, 0, [(1, 10)]))
        .unwrap();

    // Block 1 immediately rotates
    w.commit_block(&update_with_storage(1, a, 200, 1, [(2, 20)]))
        .unwrap();

    w.commit_block(&update_with_storage(2, a, 300, 2, [(3, 30)]))
        .unwrap();

    drop(w);
    let r = reader_for(&tmp, 1);

    // Only block 2 should be available
    assert!(!r.is_block_available(0).unwrap());
    assert!(!r.is_block_available(1).unwrap());
    assert!(r.is_block_available(2).unwrap());

    // Block 2 should have cumulative state
    let s = r.get_all_storage(a.into(), 2).unwrap();
    assert_eq!(s.len(), 3);
}

#[test]
fn test_large_buffer_size() {
    let (w, tmp) = writer_env(100);
    let a = addr(0xa0);

    for b in 0..50u64 {
        // Use b + 1 for slot value to avoid zero (zero = delete in Ethereum semantics)
        w.commit_block(&update_with_storage(b, a, b * 100, b, [(b, b * 10 + 1)]))
            .unwrap();
    }

    drop(w);
    let r = reader_for(&tmp, 100);

    // All 50 blocks should be available
    for b in 0..50u64 {
        assert!(r.is_block_available(b).unwrap());
    }

    // Final state should have 50 storage slots
    let s = r.get_all_storage(a.into(), 49).unwrap();
    assert_eq!(s.len(), 50);
}

#[test]
fn test_account_nonce_only_update() {
    let (w, tmp) = writer_env(3);
    let a = addr(0xb0);

    w.commit_block(&update_with_storage(0, a, 1000, 0, [(1, 100)]))
        .unwrap();

    // Only update nonce (balance same)
    w.commit_block(&simple_update(1, a, 1000, 5)).unwrap();

    // Only update balance (nonce same)
    w.commit_block(&simple_update(2, a, 2000, 5)).unwrap();

    drop(w);
    let r = reader_for(&tmp, 3);

    let acc = r.get_account(a.into(), 2).unwrap().unwrap();
    assert_eq!(acc.balance, u256(2000));
    assert_eq!(acc.nonce, 5);

    // Storage should persist
    assert_eq!(
        r.get_storage(a.into(), hash_slot(slot_b256(1)), 2).unwrap(),
        Some(u256(100))
    );
}

#[test]
fn test_verify_intermediate_states() {
    // Comprehensive test that verifies state at each intermediate block
    let (w, tmp) = writer_env(5);
    let a = addr(0xc0);

    w.commit_block(&update_with_storage(0, a, 100, 0, [(1, 10)]))
        .unwrap();
    w.commit_block(&update_with_storage(1, a, 200, 1, [(2, 20)]))
        .unwrap();
    w.commit_block(&update_with_storage(2, a, 300, 2, [(1, 30)]))
        .unwrap(); // Update slot 1
    w.commit_block(&test_update(
        3,
        B256::ZERO,
        a,
        400,
        3,
        HashMap::from([(slot_b256(2), U256::ZERO)]),
        None,
    ))
    .unwrap(); // Delete slot 2
    w.commit_block(&update_with_storage(4, a, 500, 4, [(3, 50)]))
        .unwrap();

    drop(w);
    let r = reader_for(&tmp, 5);

    // Block 0: balance=100, nonce=0, slot1=10
    let acc0 = r.get_account(a.into(), 0).unwrap().unwrap();
    assert_eq!(acc0.balance, u256(100));
    assert_eq!(acc0.nonce, 0);
    let s0 = r.get_all_storage(a.into(), 0).unwrap();
    assert_eq!(s0.len(), 1);
    assert_eq!(s0.get(&hash_slot(slot_b256(1))), Some(&u256(10)));

    // Block 1: balance=200, nonce=1, slot1=10, slot2=20
    let acc1 = r.get_account(a.into(), 1).unwrap().unwrap();
    assert_eq!(acc1.balance, u256(200));
    assert_eq!(acc1.nonce, 1);
    let s1 = r.get_all_storage(a.into(), 1).unwrap();
    assert_eq!(s1.len(), 2);

    // Block 2: balance=300, nonce=2, slot1=30 (updated), slot2=20
    let acc2 = r.get_account(a.into(), 2).unwrap().unwrap();
    assert_eq!(acc2.balance, u256(300));
    let s2 = r.get_all_storage(a.into(), 2).unwrap();
    assert_eq!(s2.get(&hash_slot(slot_b256(1))), Some(&u256(30)));
    assert_eq!(s2.get(&hash_slot(slot_b256(2))), Some(&u256(20)));

    // Block 3: balance=400, nonce=3, slot1=30, slot2=deleted
    let acc3 = r.get_account(a.into(), 3).unwrap().unwrap();
    assert_eq!(acc3.balance, u256(400));
    let s3 = r.get_all_storage(a.into(), 3).unwrap();
    assert_eq!(s3.len(), 1);
    assert_eq!(s3.get(&hash_slot(slot_b256(1))), Some(&u256(30)));
    assert_eq!(s3.get(&hash_slot(slot_b256(2))), None);

    // Block 4: balance=500, nonce=4, slot1=30, slot3=50
    let acc4 = r.get_account(a.into(), 4).unwrap().unwrap();
    assert_eq!(acc4.balance, u256(500));
    assert_eq!(acc4.nonce, 4);
    let s4 = r.get_all_storage(a.into(), 4).unwrap();
    assert_eq!(s4.len(), 2);
    assert_eq!(s4.get(&hash_slot(slot_b256(1))), Some(&u256(30)));
    assert_eq!(s4.get(&hash_slot(slot_b256(3))), Some(&u256(50)));
}

#[test]
fn test_dedup_keeps_last_occurrence_simple() {
    // Same account modified multiple times across blocks in same rotation
    let (w, tmp) = writer_env(5);
    let a = addr(0x01);

    // Block 0: initial
    w.commit_block(&block_update(
        0,
        vec![account_state_with_storage(a, 100, 0, [(1, 10)])],
    ))
    .unwrap();

    // Blocks 1-4: same account, same slot, different values each time
    w.commit_block(&block_update(
        1,
        vec![account_state_with_storage(a, 200, 1, [(1, 20)])],
    ))
    .unwrap();
    w.commit_block(&block_update(
        2,
        vec![account_state_with_storage(a, 300, 2, [(1, 30)])],
    ))
    .unwrap();
    w.commit_block(&block_update(
        3,
        vec![account_state_with_storage(a, 400, 3, [(1, 40)])],
    ))
    .unwrap();
    w.commit_block(&block_update(
        4,
        vec![account_state_with_storage(a, 500, 4, [(1, 50)])],
    ))
    .unwrap();

    drop(w);
    let r = reader_for(&tmp, 5);

    // Must have the LAST value
    let acc = r.get_account(a.into(), 4).unwrap().unwrap();
    assert_eq!(acc.balance, u256(500), "Balance should be from last block");
    assert_eq!(acc.nonce, 4, "Nonce should be from last block");

    let slot_val = r.get_storage(a.into(), hash_slot(slot_b256(1)), 4).unwrap();
    assert_eq!(
        slot_val,
        Some(u256(50)),
        "Storage should be from last block"
    );
}

#[test]
fn test_dedup_multiple_accounts_interleaved_updates() {
    // Multiple accounts updated in alternating pattern
    let (writer, temp_dir) = writer_env(10);
    let (addr_a, addr_b, addr_c) = (addr(0x0a), addr(0x0b), addr(0x0c));

    writer
        .commit_block(&block_update(
            0,
            vec![
                account_state_with_storage(addr_a, 100, 0, [(1, 10)]),
                account_state_with_storage(addr_b, 100, 0, [(1, 10)]),
                account_state_with_storage(addr_c, 100, 0, [(1, 10)]),
            ],
        ))
        .unwrap();

    // Interleaved updates - each account updated in different blocks
    writer
        .commit_block(&block_update(
            1,
            vec![account_state_with_storage(addr_a, 111, 1, [(1, 11)])],
        ))
        .unwrap();
    writer
        .commit_block(&block_update(
            2,
            vec![account_state_with_storage(addr_b, 222, 2, [(1, 22)])],
        ))
        .unwrap();
    writer
        .commit_block(&block_update(
            3,
            vec![account_state_with_storage(addr_c, 333, 3, [(1, 33)])],
        ))
        .unwrap();
    writer
        .commit_block(&block_update(
            4,
            vec![account_state_with_storage(addr_a, 444, 4, [(1, 44)])],
        ))
        .unwrap();
    writer
        .commit_block(&block_update(
            5,
            vec![account_state_with_storage(addr_b, 555, 5, [(1, 55)])],
        ))
        .unwrap();
    writer
        .commit_block(&block_update(
            6,
            vec![account_state_with_storage(addr_c, 666, 6, [(1, 66)])],
        ))
        .unwrap();
    writer
        .commit_block(&block_update(
            7,
            vec![account_state_with_storage(addr_a, 777, 7, [(1, 77)])],
        ))
        .unwrap();

    drop(writer);
    let reader = reader_for(&temp_dir, 10);

    // Each account should have its LAST update
    assert_eq!(
        reader
            .get_account(addr_a.into(), 7)
            .unwrap()
            .unwrap()
            .balance,
        u256(777)
    );
    assert_eq!(
        reader
            .get_account(addr_b.into(), 7)
            .unwrap()
            .unwrap()
            .balance,
        u256(555)
    );
    assert_eq!(
        reader
            .get_account(addr_c.into(), 7)
            .unwrap()
            .unwrap()
            .balance,
        u256(666)
    );

    assert_eq!(
        reader
            .get_storage(addr_a.into(), hash_slot(slot_b256(1)), 7)
            .unwrap(),
        Some(u256(77))
    );
    assert_eq!(
        reader
            .get_storage(addr_b.into(), hash_slot(slot_b256(1)), 7)
            .unwrap(),
        Some(u256(55))
    );
    assert_eq!(
        reader
            .get_storage(addr_c.into(), hash_slot(slot_b256(1)), 7)
            .unwrap(),
        Some(u256(66))
    );
}

#[test]
fn test_dedup_with_deletions_mixed() {
    // Accounts created, deleted, recreated in various orders
    let (w, tmp) = writer_env(10);
    let (a, b) = (addr(0xaa), addr(0xbb));

    w.commit_block(&block_update(
        0,
        vec![
            account_state_with_storage(a, 100, 0, [(1, 10), (2, 20)]),
            account_state_with_storage(b, 100, 0, [(1, 10), (2, 20)]),
        ],
    ))
    .unwrap();

    // Delete A, update B
    w.commit_block(&block_update(
        1,
        vec![
            deleted_account(a),
            account_state_with_storage(b, 200, 1, [(1, 100)]),
        ],
    ))
    .unwrap();

    // Recreate A, delete B
    w.commit_block(&block_update(
        2,
        vec![
            account_state_with_storage(a, 300, 0, [(3, 30)]),
            deleted_account(b),
        ],
    ))
    .unwrap();

    // Update A, recreate B
    w.commit_block(&block_update(
        3,
        vec![
            account_state_with_storage(a, 400, 1, [(3, 40)]),
            account_state_with_storage(b, 400, 0, [(4, 40)]),
        ],
    ))
    .unwrap();

    drop(w);
    let r = reader_for(&tmp, 10);

    // A: should have balance 400, nonce 1, only slot 3
    let acc_a = r.get_account(a.into(), 3).unwrap().unwrap();
    assert_eq!(acc_a.balance, u256(400));
    assert_eq!(acc_a.nonce, 1);
    let storage_a = r.get_all_storage(a.into(), 3).unwrap();
    assert_eq!(storage_a.len(), 1);
    assert_eq!(storage_a.get(&hash_slot(slot_b256(3))), Some(&u256(40)));
    assert!(
        !storage_a.contains_key(&hash_slot(slot_b256(1))),
        "Old slot should be gone"
    );

    // B: should have balance 400, nonce 0, only slot 4
    let acc_b = r.get_account(b.into(), 3).unwrap().unwrap();
    assert_eq!(acc_b.balance, u256(400));
    assert_eq!(acc_b.nonce, 0);
    let storage_b = r.get_all_storage(b.into(), 3).unwrap();
    assert_eq!(storage_b.len(), 1);
    assert_eq!(storage_b.get(&hash_slot(slot_b256(4))), Some(&u256(40)));
}

#[test]
fn test_rotation_same_slot_overwritten_each_block() {
    // Same slot modified every single block across multiple rotations
    let (w, tmp) = writer_env(3);
    let a = addr(0xcc);

    for b in 0..12u64 {
        w.commit_block(&block_update(
            b,
            vec![account_state_with_storage(a, b * 100, b, [(1, b * 10)])],
        ))
        .unwrap();
    }

    drop(w);
    let r = reader_for(&tmp, 3);

    // Only blocks 9, 10, 11 available
    for b in 9..12u64 {
        let acc = r.get_account(a.into(), b).unwrap().unwrap();
        assert_eq!(acc.balance, u256(b * 100), "Block {b} balance");
        assert_eq!(acc.nonce, b, "Block {b} nonce");

        let slot = r.get_storage(a.into(), hash_slot(slot_b256(1)), b).unwrap();
        assert_eq!(slot, Some(u256(b * 10)), "Block {b} slot value");
    }
}

#[test]
fn test_rotation_delete_in_every_position() {
    // Test deletion happening at each position within a rotation cycle
    let (w, tmp) = writer_env(4);
    let a = addr(0xdd);

    // Block 0: create with slots 1,2,3,4
    w.commit_block(&block_update(
        0,
        vec![account_state_with_storage(
            a,
            1000,
            0,
            [(1, 10), (2, 20), (3, 30), (4, 40)],
        )],
    ))
    .unwrap();

    // Block 1: delete slot 1
    w.commit_block(&block_update(
        1,
        vec![AccountState {
            address_hash: AddressHash(keccak256(a)),
            balance: u256(1000),
            nonce: 1,
            code_hash: B256::ZERO,
            code: None,
            storage: hash_storage(HashMap::from([(slot_b256(1), U256::ZERO)])),
            deleted: false,
        }],
    ))
    .unwrap();

    // Block 2: delete slot 2
    w.commit_block(&block_update(
        2,
        vec![AccountState {
            address_hash: AddressHash(keccak256(a)),
            balance: u256(1000),
            nonce: 2,
            code_hash: B256::ZERO,
            code: None,
            storage: hash_storage(HashMap::from([(slot_b256(2), U256::ZERO)])),
            deleted: false,
        }],
    ))
    .unwrap();

    // Block 3: delete slot 3
    w.commit_block(&block_update(
        3,
        vec![AccountState {
            address_hash: AddressHash(keccak256(a)),
            balance: u256(1000),
            nonce: 3,
            code_hash: B256::ZERO,
            code: None,
            storage: hash_storage(HashMap::from([(slot_b256(3), U256::ZERO)])),
            deleted: false,
        }],
    ))
    .unwrap();

    // Block 4: rotation! Add slot 5
    w.commit_block(&block_update(
        4,
        vec![account_state_with_storage(a, 1000, 4, [(5, 50)])],
    ))
    .unwrap();

    drop(w);
    let r = reader_for(&tmp, 4);

    // Block 4 should have: slot 4 (original), slot 5 (new)
    // Slots 1,2,3 were deleted
    let storage = r.get_all_storage(a.into(), 4).unwrap();
    assert_eq!(storage.len(), 2, "Should have exactly 2 slots");
    assert_eq!(storage.get(&hash_slot(slot_b256(4))), Some(&u256(40)));
    assert_eq!(storage.get(&hash_slot(slot_b256(5))), Some(&u256(50)));
    assert!(!storage.contains_key(&hash_slot(slot_b256(1))));
    assert!(!storage.contains_key(&hash_slot(slot_b256(2))));
    assert!(!storage.contains_key(&hash_slot(slot_b256(3))));
}

#[test]
fn test_rotation_recreate_same_slot_after_delete() {
    // Critical: delete a slot then recreate with same key but different value
    let (w, tmp) = writer_env(3);
    let a = addr(0xee);

    // Block 0 (ns 0): slot 1 = 100
    w.commit_block(&block_update(
        0,
        vec![account_state_with_storage(a, 1000, 0, [(1, 100)])],
    ))
    .unwrap();

    // Block 1 (ns 1): delete slot 1
    w.commit_block(&block_update(
        1,
        vec![AccountState {
            address_hash: AddressHash(keccak256(a)),
            balance: u256(1000),
            nonce: 1,
            code_hash: B256::ZERO,
            code: None,
            storage: hash_storage(HashMap::from([(slot_b256(1), U256::ZERO)])),
            deleted: false,
        }],
    ))
    .unwrap();

    // Block 2 (ns 2): recreate slot 1 = 999
    w.commit_block(&block_update(
        2,
        vec![account_state_with_storage(a, 1000, 2, [(1, 999)])],
    ))
    .unwrap();

    // Block 3 (ns 0 - rotation!): just update balance
    w.commit_block(&block_update(3, vec![simple_account(a, 2000, 3)]))
        .unwrap();

    drop(w);
    let r = reader_for(&tmp, 3);

    // Block 3 MUST have slot 1 = 999, NOT 100
    let slot = r.get_storage(a.into(), hash_slot(slot_b256(1)), 3).unwrap();
    assert_eq!(
        slot,
        Some(u256(999)),
        "Must have recreated value 999, not original 100"
    );
}

#[test]
fn test_rotation_account_delete_recreate_same_slots() {
    // Delete entire account, recreate with SAME slot keys but different values
    let (w, tmp) = writer_env(3);
    let a = addr(0xff);

    // Block 0: create with slots 1=100, 2=200
    w.commit_block(&block_update(
        0,
        vec![account_state_with_storage(a, 1000, 0, [(1, 100), (2, 200)])],
    ))
    .unwrap();

    // Block 1: delete account
    w.commit_block(&block_update(1, vec![deleted_account(a)]))
        .unwrap();

    // Block 2: recreate with same slot keys, different values
    w.commit_block(&block_update(
        2,
        vec![account_state_with_storage(a, 5000, 0, [(1, 111), (2, 222)])],
    ))
    .unwrap();

    // Block 3: rotation
    w.commit_block(&block_update(3, vec![simple_account(a, 5500, 1)]))
        .unwrap();

    drop(w);
    let r = reader_for(&tmp, 3);

    let storage = r.get_all_storage(a.into(), 3).unwrap();
    assert_eq!(storage.len(), 2);
    assert_eq!(
        storage.get(&hash_slot(slot_b256(1))),
        Some(&u256(111)),
        "Must be new value"
    );
    assert_eq!(
        storage.get(&hash_slot(slot_b256(2))),
        Some(&u256(222)),
        "Must be new value"
    );
}

#[test]
fn test_many_accounts_all_modified_every_block() {
    // 20 accounts, all modified in every block
    let (w, tmp) = writer_env(5);

    for b in 0..10u64 {
        let accounts: Vec<_> = (0..20u8)
            .map(|i| {
                account_state_with_storage(
                    addr(i),
                    b * 100 + u64::from(i),
                    b,
                    [(1, b * 10 + u64::from(i))],
                )
            })
            .collect();
        w.commit_block(&block_update(b, accounts)).unwrap();
    }

    drop(w);
    let r = reader_for(&tmp, 5);

    // Verify all accounts at block 9
    for i in 0..20u8 {
        let acc = r.get_account(addr(i).into(), 9).unwrap().unwrap();
        assert_eq!(acc.balance, u256(900 + u64::from(i)), "Account {i} balance",);
        assert_eq!(acc.nonce, 9, "Account {i} nonce");

        let slot = r
            .get_storage(addr(i).into(), hash_slot(slot_b256(1)), 9)
            .unwrap();
        assert_eq!(slot, Some(u256(90 + u64::from(i))), "Account {i} slot");
    }
}

#[test]
fn test_many_accounts_subset_deleted_each_block() {
    // Create 10 accounts, delete 2 per block
    let (w, tmp) = writer_env(10);

    // Block 0: create all
    let accounts: Vec<_> = (0..10u8)
        .map(|i| account_state_with_storage(addr(i), 1000, 0, [(1, 100)]))
        .collect();
    w.commit_block(&block_update(0, accounts)).unwrap();

    // Blocks 1-5: delete 2 accounts each
    for b in 1..=5u64 {
        let idx1 = u8::try_from((b - 1) * 2).expect("account index fits in u8");
        let idx2 = idx1 + 1;
        w.commit_block(&block_update(
            b,
            vec![deleted_account(addr(idx1)), deleted_account(addr(idx2))],
        ))
        .unwrap();
    }

    drop(w);
    let r = reader_for(&tmp, 10);

    // All accounts should be deleted
    for i in 0..10u8 {
        assert!(
            r.get_account(addr(i).into(), 5).unwrap().is_none(),
            "Account {i} should be deleted",
        );
        assert!(
            r.get_all_storage(addr(i).into(), 5).unwrap().is_empty(),
            "Account {i} storage should be empty",
        );
    }
}

#[test]
fn test_many_slots_per_account() {
    // One account with 100 storage slots, modified across rotations
    let (w, tmp) = writer_env(3);
    let a = addr(0x01);

    // Block 0: create with 100 slots
    let slots: Vec<_> = (1..=100u64).map(|i| (i, i * 10)).collect();
    w.commit_block(&block_update(
        0,
        vec![account_state_with_storage(a, 1000, 0, slots)],
    ))
    .unwrap();

    // Block 1: modify odd slots
    let odd_slots: Vec<_> = (1..=100u64)
        .filter(|i| i % 2 == 1)
        .map(|i| (i, i * 100))
        .collect();
    w.commit_block(&block_update(
        1,
        vec![account_state_with_storage(a, 1000, 1, odd_slots)],
    ))
    .unwrap();

    // Block 2: delete even slots
    let even_deletes: HashMap<_, _> = (1..=100u64)
        .filter(|i| i % 2 == 0)
        .map(|i| (slot_b256(i), U256::ZERO))
        .collect();
    w.commit_block(&block_update(
        2,
        vec![AccountState {
            address_hash: AddressHash(keccak256(a)),
            balance: u256(1000),
            nonce: 2,
            code_hash: B256::ZERO,
            code: None,
            storage: hash_storage(even_deletes),
            deleted: false,
        }],
    ))
    .unwrap();

    // Block 3: rotation
    w.commit_block(&block_update(3, vec![simple_account(a, 2000, 3)]))
        .unwrap();

    drop(w);
    let r = reader_for(&tmp, 3);

    let storage = r.get_all_storage(a.into(), 3).unwrap();

    // Should have only odd slots (50 total)
    assert_eq!(storage.len(), 50, "Should have 50 odd slots");

    for i in 1..=100u64 {
        let slot_hash = hash_slot(slot_b256(i));
        if i % 2 == 1 {
            assert_eq!(
                storage.get(&slot_hash),
                Some(&u256(i * 100)),
                "Odd slot {i} should have modified value",
            );
        } else {
            assert!(
                !storage.contains_key(&slot_hash),
                "Even slot {i} should be deleted",
            );
        }
    }
}

#[test]
fn test_delete_then_write_same_block_different_accounts() {
    // In same block: delete account A, create account B with same-ish pattern
    let (w, tmp) = writer_env(5);
    let (a, b) = (addr(0xaa), addr(0xbb));

    w.commit_block(&block_update(
        0,
        vec![account_state_with_storage(a, 1000, 0, [(1, 100)])],
    ))
    .unwrap();

    // Block 1: delete A, create B (both in same block)
    w.commit_block(&block_update(
        1,
        vec![
            deleted_account(a),
            account_state_with_storage(b, 2000, 0, [(1, 200)]),
        ],
    ))
    .unwrap();

    drop(w);
    let r = reader_for(&tmp, 5);

    assert!(r.get_account(a.into(), 1).unwrap().is_none());
    assert_eq!(
        r.get_account(b.into(), 1).unwrap().unwrap().balance,
        u256(2000)
    );
    assert_eq!(
        r.get_storage(b.into(), hash_slot(slot_b256(1)), 1).unwrap(),
        Some(u256(200))
    );
}

#[test]
fn test_rapid_create_delete_cycle() {
    // Account created and deleted multiple times rapidly
    let (w, tmp) = writer_env(10);
    let a = addr(0xcd);

    for b in 0..10u64 {
        if b % 2 == 0 {
            // Even blocks: create
            w.commit_block(&block_update(
                b,
                vec![account_state_with_storage(
                    a,
                    (b + 1) * 100,
                    0,
                    [(1, (b + 1) * 10)],
                )],
            ))
            .unwrap();
        } else {
            // Odd blocks: delete
            w.commit_block(&block_update(b, vec![deleted_account(a)]))
                .unwrap();
        }
    }

    drop(w);
    let r = reader_for(&tmp, 10);

    // Final block 9 is odd -> deleted
    assert!(r.get_account(a.into(), 9).unwrap().is_none());

    // Block 8 is even -> exists
    let acc = r.get_account(a.into(), 8).unwrap().unwrap();
    assert_eq!(acc.balance, u256(900));
    assert_eq!(
        r.get_storage(a.into(), hash_slot(slot_b256(1)), 8).unwrap(),
        Some(u256(90))
    );
}

#[test]
fn test_all_operations_in_single_rotation() {
    // Create, update, delete slot, recreate slot, delete account, recreate account
    // All within a single rotation window
    let (w, tmp) = writer_env(7);
    let a = addr(0xef);

    // Block 0: create
    w.commit_block(&block_update(
        0,
        vec![account_state_with_storage(a, 100, 0, [(1, 10), (2, 20)])],
    ))
    .unwrap();

    // Block 1: update
    w.commit_block(&block_update(
        1,
        vec![account_state_with_storage(a, 200, 1, [(1, 11), (3, 30)])],
    ))
    .unwrap();

    // Block 2: delete slot 2
    w.commit_block(&block_update(
        2,
        vec![AccountState {
            address_hash: AddressHash(keccak256(a)),
            balance: u256(300),
            nonce: 2,
            code_hash: B256::ZERO,
            code: None,
            storage: hash_storage(HashMap::from([(slot_b256(2), U256::ZERO)])),
            deleted: false,
        }],
    ))
    .unwrap();

    // Block 3: recreate slot 2 with new value
    w.commit_block(&block_update(
        3,
        vec![account_state_with_storage(a, 400, 3, [(2, 222)])],
    ))
    .unwrap();

    // Block 4: delete account
    w.commit_block(&block_update(4, vec![deleted_account(a)]))
        .unwrap();

    // Block 5: recreate account
    w.commit_block(&block_update(
        5,
        vec![account_state_with_storage(a, 500, 0, [(5, 50)])],
    ))
    .unwrap();

    // Block 6: final update
    w.commit_block(&block_update(6, vec![simple_account(a, 600, 1)]))
        .unwrap();

    drop(w);
    let r = reader_for(&tmp, 7);

    // Verify each block state
    // Block 0
    assert_eq!(
        r.get_account(a.into(), 0).unwrap().unwrap().balance,
        u256(100)
    );
    assert_eq!(r.get_all_storage(a.into(), 0).unwrap().len(), 2);

    // Block 1
    assert_eq!(
        r.get_account(a.into(), 1).unwrap().unwrap().balance,
        u256(200)
    );
    let s1 = r.get_all_storage(a.into(), 1).unwrap();
    assert_eq!(s1.len(), 3); // slots 1, 2, 3

    // Block 2
    let s2 = r.get_all_storage(a.into(), 2).unwrap();
    assert_eq!(s2.len(), 2); // slots 1, 3 (2 deleted)
    assert!(!s2.contains_key(&hash_slot(slot_b256(2))));

    // Block 3
    let s3 = r.get_all_storage(a.into(), 3).unwrap();
    assert_eq!(s3.get(&hash_slot(slot_b256(2))), Some(&u256(222))); // recreated

    // Block 4
    assert!(r.get_account(a.into(), 4).unwrap().is_none());
    assert!(r.get_all_storage(a.into(), 4).unwrap().is_empty());

    // Block 5
    let acc5 = r.get_account(a.into(), 5).unwrap().unwrap();
    assert_eq!(acc5.balance, u256(500));
    let s5 = r.get_all_storage(a.into(), 5).unwrap();
    assert_eq!(s5.len(), 1);
    assert_eq!(s5.get(&hash_slot(slot_b256(5))), Some(&u256(50)));

    // Block 6
    let acc6 = r.get_account(a.into(), 6).unwrap().unwrap();
    assert_eq!(acc6.balance, u256(600));
    assert_eq!(acc6.nonce, 1);
    let s6 = r.get_all_storage(a.into(), 6).unwrap();
    assert_eq!(s6.len(), 1); // only slot 5 persists
}

#[test]
fn test_triple_rotation_with_mixed_operations() {
    // 3 full rotations with various operations to stress test
    let (w, tmp) = writer_env(3);
    let (a, b) = (addr(0x11), addr(0x22));

    // Rotation 1: blocks 0,1,2
    w.commit_block(&block_update(
        0,
        vec![
            account_state_with_storage(a, 100, 0, [(1, 10)]),
            account_state_with_storage(b, 100, 0, [(1, 10)]),
        ],
    ))
    .unwrap();
    w.commit_block(&block_update(
        1,
        vec![account_state_with_storage(a, 200, 1, [(2, 20)])],
    ))
    .unwrap();
    w.commit_block(&block_update(2, vec![deleted_account(b)]))
        .unwrap();

    // Rotation 2: blocks 3,4,5
    w.commit_block(&block_update(
        3,
        vec![
            account_state_with_storage(a, 300, 2, [(1, 30)]), // update slot 1
            account_state_with_storage(b, 300, 0, [(3, 30)]), // recreate b
        ],
    ))
    .unwrap();
    w.commit_block(&block_update(
        4,
        vec![AccountState {
            address_hash: AddressHash(keccak256(a)),
            balance: u256(400),
            nonce: 3,
            code_hash: B256::ZERO,
            code: None,
            storage: hash_storage(HashMap::from([(slot_b256(2), U256::ZERO)])), // delete slot 2
            deleted: false,
        }],
    ))
    .unwrap();
    w.commit_block(&block_update(
        5,
        vec![account_state_with_storage(b, 500, 1, [(4, 40)])],
    ))
    .unwrap();

    // Rotation 3: blocks 6,7,8
    w.commit_block(&block_update(6, vec![deleted_account(a)]))
        .unwrap();
    w.commit_block(&block_update(
        7,
        vec![
            account_state_with_storage(a, 700, 0, [(5, 50)]), // recreate a
        ],
    ))
    .unwrap();
    w.commit_block(&block_update(8, vec![simple_account(b, 800, 2)]))
        .unwrap();

    drop(w);
    let r = reader_for(&tmp, 3);

    // Only blocks 6,7,8 available
    assert!(!r.is_block_available(5).unwrap());
    assert!(r.is_block_available(6).unwrap());

    // Block 6: A deleted, B has slots 3,4
    assert!(r.get_account(a.into(), 6).unwrap().is_none());
    let sb6 = r.get_all_storage(b.into(), 6).unwrap();
    assert_eq!(sb6.len(), 2);
    assert_eq!(sb6.get(&hash_slot(slot_b256(3))), Some(&u256(30)));
    assert_eq!(sb6.get(&hash_slot(slot_b256(4))), Some(&u256(40)));

    // Block 7: A recreated with slot 5
    let sa7 = r.get_all_storage(a.into(), 7).unwrap();
    assert_eq!(sa7.len(), 1);
    assert_eq!(sa7.get(&hash_slot(slot_b256(5))), Some(&u256(50)));
    // A should NOT have old slots
    assert!(!sa7.contains_key(&hash_slot(slot_b256(1))));
    assert!(!sa7.contains_key(&hash_slot(slot_b256(2))));

    // Block 8: final state
    let acc_a = r.get_account(a.into(), 8).unwrap().unwrap();
    assert_eq!(acc_a.balance, u256(700));
    let acc_b = r.get_account(b.into(), 8).unwrap().unwrap();
    assert_eq!(acc_b.balance, u256(800));
    assert_eq!(acc_b.nonce, 2);
}

#[test]
fn test_storage_slot_zero_vs_nonexistent() {
    // Ensure we distinguish between "slot = 0" (deleted) and "slot never existed"
    let (w, tmp) = writer_env(5);
    let a = addr(0x33);

    // Block 0: create with slot 1 only
    w.commit_block(&block_update(
        0,
        vec![account_state_with_storage(a, 1000, 0, [(1, 100)])],
    ))
    .unwrap();

    // Block 1: explicitly set slot 1 to zero (delete it)
    w.commit_block(&block_update(
        1,
        vec![AccountState {
            address_hash: AddressHash(keccak256(a)),
            balance: u256(1000),
            nonce: 1,
            code_hash: B256::ZERO,
            code: None,
            storage: hash_storage(HashMap::from([(slot_b256(1), U256::ZERO)])),
            deleted: false,
        }],
    ))
    .unwrap();

    drop(w);
    let r = reader_for(&tmp, 5);

    // Slot 1 should be None (deleted), slot 2 should be None (never existed)
    assert_eq!(
        r.get_storage(a.into(), hash_slot(slot_b256(1)), 1).unwrap(),
        None
    );
    assert_eq!(
        r.get_storage(a.into(), hash_slot(slot_b256(2)), 1).unwrap(),
        None
    );

    // All storage should be empty
    assert!(r.get_all_storage(a.into(), 1).unwrap().is_empty());
}

#[test]
fn test_concurrent_accounts_different_lifecycles() {
    // Multiple accounts with completely different lifecycle patterns
    let (writer, temp_dir) = writer_env(6);
    let (addr_a, addr_b, addr_c, addr_d) = (addr(0x0a), addr(0x0b), addr(0x0c), addr(0x0d));

    // A: exists throughout, constantly updated
    // B: created early, deleted mid-way, never recreated
    // C: created mid-way, exists to end
    // D: created, deleted, recreated, deleted, recreated

    writer
        .commit_block(&block_update(
            0,
            vec![
                account_state_with_storage(addr_a, 100, 0, [(1, 10)]),
                account_state_with_storage(addr_b, 100, 0, [(1, 10)]),
            ],
        ))
        .unwrap();

    writer
        .commit_block(&block_update(
            1,
            vec![
                account_state_with_storage(addr_a, 200, 1, [(1, 20)]),
                account_state_with_storage(addr_d, 100, 0, [(1, 10)]),
            ],
        ))
        .unwrap();

    writer
        .commit_block(&block_update(
            2,
            vec![
                account_state_with_storage(addr_a, 300, 2, [(1, 30)]),
                deleted_account(addr_b),
                account_state_with_storage(addr_c, 100, 0, [(1, 10)]),
                deleted_account(addr_d),
            ],
        ))
        .unwrap();

    writer
        .commit_block(&block_update(
            3,
            vec![
                account_state_with_storage(addr_a, 400, 3, [(1, 40)]),
                account_state_with_storage(addr_c, 200, 1, [(1, 20)]),
                account_state_with_storage(addr_d, 200, 0, [(2, 20)]),
            ],
        ))
        .unwrap();

    writer
        .commit_block(&block_update(
            4,
            vec![
                account_state_with_storage(addr_a, 500, 4, [(1, 50)]),
                deleted_account(addr_d),
            ],
        ))
        .unwrap();

    writer
        .commit_block(&block_update(
            5,
            vec![
                account_state_with_storage(addr_a, 600, 5, [(1, 60)]),
                account_state_with_storage(addr_c, 300, 2, [(1, 30)]),
                account_state_with_storage(addr_d, 300, 0, [(3, 30)]),
            ],
        ))
        .unwrap();

    drop(writer);
    let reader = reader_for(&temp_dir, 6);

    // Block 5 final state:
    // A: balance 600, nonce 5, slot1=60
    let acc_a = reader.get_account(addr_a.into(), 5).unwrap().unwrap();
    assert_eq!(acc_a.balance, u256(600));
    assert_eq!(
        reader
            .get_storage(addr_a.into(), hash_slot(slot_b256(1)), 5)
            .unwrap(),
        Some(u256(60))
    );

    // B: deleted (since block 2)
    assert!(reader.get_account(addr_b.into(), 5).unwrap().is_none());

    // C: balance 300, nonce 2, slot1=30
    let acc_c = reader.get_account(addr_c.into(), 5).unwrap().unwrap();
    assert_eq!(acc_c.balance, u256(300));
    assert_eq!(
        reader
            .get_storage(addr_c.into(), hash_slot(slot_b256(1)), 5)
            .unwrap(),
        Some(u256(30))
    );

    // D: balance 300, nonce 0, slot3=30 (recreated in block 5)
    let acc_d = reader.get_account(addr_d.into(), 5).unwrap().unwrap();
    assert_eq!(acc_d.balance, u256(300));
    assert_eq!(acc_d.nonce, 0);
    let sd = reader.get_all_storage(addr_d.into(), 5).unwrap();
    assert_eq!(sd.len(), 1);
    assert_eq!(sd.get(&hash_slot(slot_b256(3))), Some(&u256(30)));
    // Old slots should NOT exist
    assert!(!sd.contains_key(&hash_slot(slot_b256(1))));
    assert!(!sd.contains_key(&hash_slot(slot_b256(2))));
}

#[test]
fn test_dedup_swap_indices_regression() {
    // This test creates a specific pattern that would fail if swap indices were stale
    // Pattern: 5 accounts, keep indices [1, 3, 4] after dedup
    // If the swap algorithm is broken, index 4 might contain wrong data after swaps
    let (writer, temp_dir) = writer_env(5);
    let (addr_a, addr_b, addr_c, addr_d, addr_e) =
        (addr(0xa1), addr(0xa2), addr(0xa3), addr(0xa4), addr(0xa5));

    // Block 0: create all 5 accounts with distinct values
    writer
        .commit_block(&block_update(
            0,
            vec![
                account_state_with_storage(addr_a, 100, 0, [(1, 10)]),
                account_state_with_storage(addr_b, 200, 0, [(1, 20)]),
                account_state_with_storage(addr_c, 300, 0, [(1, 30)]),
                account_state_with_storage(addr_d, 400, 0, [(1, 40)]),
                account_state_with_storage(addr_e, 500, 0, [(1, 50)]),
            ],
        ))
        .unwrap();

    // Block 1: update A and C with new values
    // This creates duplicates for A and C in the batch
    writer
        .commit_block(&block_update(
            1,
            vec![
                account_state_with_storage(addr_a, 111, 1, [(1, 11)]),
                account_state_with_storage(addr_c, 333, 1, [(1, 33)]),
            ],
        ))
        .unwrap();

    // Block 2: update A again
    writer
        .commit_block(&block_update(
            2,
            vec![account_state_with_storage(addr_a, 1111, 2, [(1, 111)])],
        ))
        .unwrap();

    drop(writer);
    let reader = reader_for(&temp_dir, 5);

    // A should have LAST value
    assert_eq!(
        reader
            .get_account(addr_a.into(), 2)
            .unwrap()
            .unwrap()
            .balance,
        u256(1111)
    );
    assert_eq!(
        reader
            .get_storage(addr_a.into(), hash_slot(slot_b256(1)), 2)
            .unwrap(),
        Some(u256(111))
    );

    // B, D, E should be unchanged from block 0
    assert_eq!(
        reader
            .get_account(addr_b.into(), 2)
            .unwrap()
            .unwrap()
            .balance,
        u256(200)
    );
    assert_eq!(
        reader
            .get_account(addr_d.into(), 2)
            .unwrap()
            .unwrap()
            .balance,
        u256(400)
    );
    assert_eq!(
        reader
            .get_account(addr_e.into(), 2)
            .unwrap()
            .unwrap()
            .balance,
        u256(500)
    );

    // C should have value from block 1
    assert_eq!(
        reader
            .get_account(addr_c.into(), 2)
            .unwrap()
            .unwrap()
            .balance,
        u256(333)
    );
}

#[test]
fn test_dedup_all_same_key() {
    // Extreme case: same account updated in every single block
    // All entries have same key, must keep only the last
    let (w, tmp) = writer_env(20);
    let a = addr(0xbb);

    for b in 0..20u64 {
        w.commit_block(&block_update(
            b,
            vec![account_state_with_storage(a, b * 1000, b, [(1, b * 100)])],
        ))
        .unwrap();
    }

    drop(w);
    let r = reader_for(&tmp, 20);

    // Must have the very last value
    let acc = r.get_account(a.into(), 19).unwrap().unwrap();
    assert_eq!(acc.balance, u256(19_000));
    assert_eq!(acc.nonce, 19);
    assert_eq!(
        r.get_storage(a.into(), hash_slot(slot_b256(1)), 19)
            .unwrap(),
        Some(u256(1900))
    );
}

#[test]
fn test_dedup_alternating_two_keys() {
    // Two accounts alternating updates - stresses the dedup merging
    let (w, tmp) = writer_env(15);
    let (a, b) = (addr(0xca), addr(0xcb));

    for block in 0..15u64 {
        if block % 2 == 0 {
            w.commit_block(&block_update(
                block,
                vec![account_state_with_storage(
                    a,
                    block * 100,
                    block,
                    [(1, block * 10)],
                )],
            ))
            .unwrap();
        } else {
            w.commit_block(&block_update(
                block,
                vec![account_state_with_storage(
                    b,
                    block * 100,
                    block,
                    [(1, block * 10)],
                )],
            ))
            .unwrap();
        }
    }

    drop(w);
    let r = reader_for(&tmp, 15);

    // A was last updated at block 14 (even)
    assert_eq!(
        r.get_account(a.into(), 14).unwrap().unwrap().balance,
        u256(1400)
    );
    assert_eq!(
        r.get_storage(a.into(), hash_slot(slot_b256(1)), 14)
            .unwrap(),
        Some(u256(140))
    );

    // B was last updated at block 13 (odd)
    assert_eq!(
        r.get_account(b.into(), 14).unwrap().unwrap().balance,
        u256(1300)
    );
    assert_eq!(
        r.get_storage(b.into(), hash_slot(slot_b256(1)), 14)
            .unwrap(),
        Some(u256(130))
    );
}

#[test]
fn test_dedup_first_and_last_same_key() {
    // First and last entries are same key, middle is different
    let (w, tmp) = writer_env(5);
    let (a, b) = (addr(0xd1), addr(0xd2));

    // Block 0: A
    w.commit_block(&block_update(
        0,
        vec![account_state_with_storage(a, 100, 0, [(1, 10)])],
    ))
    .unwrap();

    // Block 1: B
    w.commit_block(&block_update(
        1,
        vec![account_state_with_storage(b, 200, 0, [(1, 20)])],
    ))
    .unwrap();

    // Block 2: B again
    w.commit_block(&block_update(
        2,
        vec![account_state_with_storage(b, 300, 1, [(1, 30)])],
    ))
    .unwrap();

    // Block 3: A again (first and now last have same key)
    w.commit_block(&block_update(
        3,
        vec![account_state_with_storage(a, 400, 1, [(1, 40)])],
    ))
    .unwrap();

    drop(w);
    let r = reader_for(&tmp, 5);

    // A should have block 3 value
    assert_eq!(
        r.get_account(a.into(), 3).unwrap().unwrap().balance,
        u256(400)
    );
    assert_eq!(
        r.get_storage(a.into(), hash_slot(slot_b256(1)), 3).unwrap(),
        Some(u256(40))
    );

    // B should have block 2 value
    assert_eq!(
        r.get_account(b.into(), 3).unwrap().unwrap().balance,
        u256(300)
    );
    assert_eq!(
        r.get_storage(b.into(), hash_slot(slot_b256(1)), 3).unwrap(),
        Some(u256(30))
    );
}

#[test]
fn test_worst_case_rotation_intermediate_diffs() {
    // Worst case: buffer_size-1 intermediate diffs to apply during rotation
    // With buffer_size=4, blocks 1,2,3 become intermediate diffs when writing block 4
    let (w, tmp) = writer_env(4);
    let a = addr(0xe1);

    // Block 0 (ns 0): base state
    w.commit_block(&block_update(
        0,
        vec![account_state_with_storage(
            a,
            100,
            0,
            [(1, 10), (2, 20), (3, 30), (4, 40)],
        )],
    ))
    .unwrap();

    // Block 1 (ns 1): modify slot 1
    w.commit_block(&block_update(
        1,
        vec![account_state_with_storage(a, 110, 1, [(1, 11)])],
    ))
    .unwrap();

    // Block 2 (ns 2): delete slot 2
    w.commit_block(&block_update(
        2,
        vec![AccountState {
            address_hash: AddressHash(keccak256(a)),
            balance: u256(120),
            nonce: 2,
            code_hash: B256::ZERO,
            code: None,
            storage: hash_storage(HashMap::from([(slot_b256(2), U256::ZERO)])),
            deleted: false,
        }],
    ))
    .unwrap();

    // Block 3 (ns 3): modify slot 3, add slot 5
    w.commit_block(&block_update(
        3,
        vec![account_state_with_storage(a, 130, 3, [(3, 33), (5, 50)])],
    ))
    .unwrap();

    // Block 4 (ns 0 ROTATION): must apply diffs from blocks 1,2,3
    w.commit_block(&block_update(
        4,
        vec![account_state_with_storage(a, 140, 4, [(6, 60)])],
    ))
    .unwrap();

    drop(w);
    let r = reader_for(&tmp, 4);

    // Block 4 cumulative state:
    // - slot 1: 11 (from block 1)
    // - slot 2: DELETED (from block 2)
    // - slot 3: 33 (from block 3)
    // - slot 4: 40 (from block 0, unchanged)
    // - slot 5: 50 (from block 3)
    // - slot 6: 60 (from block 4)
    let storage = r.get_all_storage(a.into(), 4).unwrap();

    assert_eq!(storage.len(), 5, "Should have 5 slots (slot 2 deleted)");
    assert_eq!(storage.get(&hash_slot(slot_b256(1))), Some(&u256(11)));
    assert!(
        !storage.contains_key(&hash_slot(slot_b256(2))),
        "Slot 2 should be deleted"
    );
    assert_eq!(storage.get(&hash_slot(slot_b256(3))), Some(&u256(33)));
    assert_eq!(storage.get(&hash_slot(slot_b256(4))), Some(&u256(40)));
    assert_eq!(storage.get(&hash_slot(slot_b256(5))), Some(&u256(50)));
    assert_eq!(storage.get(&hash_slot(slot_b256(6))), Some(&u256(60)));

    let acc = r.get_account(a.into(), 4).unwrap().unwrap();
    assert_eq!(acc.balance, u256(140));
    assert_eq!(acc.nonce, 4);
}

#[test]
fn test_empty_intermediate_blocks() {
    // Some intermediate blocks have no changes - should not affect state
    let (w, tmp) = writer_env(5);
    let a = addr(0xf1);

    w.commit_block(&block_update(
        0,
        vec![account_state_with_storage(a, 100, 0, [(1, 10)])],
    ))
    .unwrap();

    // Empty block
    w.commit_block(&block_update(1, vec![])).unwrap();

    w.commit_block(&block_update(
        2,
        vec![account_state_with_storage(a, 200, 1, [(2, 20)])],
    ))
    .unwrap();

    // Another empty block
    w.commit_block(&block_update(3, vec![])).unwrap();

    // Another empty block
    w.commit_block(&block_update(4, vec![])).unwrap();

    drop(w);
    let r = reader_for(&tmp, 5);

    // State should be cumulative from blocks 0 and 2
    let acc = r.get_account(a.into(), 4).unwrap().unwrap();
    assert_eq!(acc.balance, u256(200));
    assert_eq!(acc.nonce, 1);

    let storage = r.get_all_storage(a.into(), 4).unwrap();
    assert_eq!(storage.len(), 2);
    assert_eq!(storage.get(&hash_slot(slot_b256(1))), Some(&u256(10)));
    assert_eq!(storage.get(&hash_slot(slot_b256(2))), Some(&u256(20)));
}

#[test]
fn test_storage_overwrite_then_delete_same_rotation() {
    // Overwrite a slot, then delete it, all in intermediate diffs
    let (w, tmp) = writer_env(4);
    let a = addr(0xf2);

    // Block 0 (ns 0): slot 1 = 100
    w.commit_block(&block_update(
        0,
        vec![account_state_with_storage(a, 1000, 0, [(1, 100)])],
    ))
    .unwrap();

    // Block 1 (ns 1): slot 1 = 200 (overwrite)
    w.commit_block(&block_update(
        1,
        vec![account_state_with_storage(a, 1000, 1, [(1, 200)])],
    ))
    .unwrap();

    // Block 2 (ns 2): delete slot 1
    w.commit_block(&block_update(
        2,
        vec![AccountState {
            address_hash: AddressHash(keccak256(a)),
            balance: u256(1000),
            nonce: 2,
            code_hash: B256::ZERO,
            code: None,
            storage: hash_storage(HashMap::from([(slot_b256(1), U256::ZERO)])),
            deleted: false,
        }],
    ))
    .unwrap();

    // Block 3 (ns 3): no storage change
    w.commit_block(&block_update(3, vec![simple_account(a, 2000, 3)]))
        .unwrap();

    // Block 4 (ns 0 ROTATION): must correctly apply all intermediate diffs
    w.commit_block(&block_update(4, vec![simple_account(a, 3000, 4)]))
        .unwrap();

    drop(w);
    let r = reader_for(&tmp, 4);

    // Slot 1 should be DELETED (not 100, not 200)
    let storage = r.get_all_storage(a.into(), 4).unwrap();
    assert!(
        storage.is_empty(),
        "Slot 1 should be deleted, storage should be empty"
    );
}

#[test]
fn test_delete_then_recreate_then_delete_again() {
    // slot: exists -> deleted -> recreated -> deleted (final state: deleted)
    let (w, tmp) = writer_env(6);
    let a = addr(0xf3);

    w.commit_block(&block_update(
        0,
        vec![account_state_with_storage(a, 100, 0, [(1, 10)])],
    ))
    .unwrap();

    // Delete slot 1
    w.commit_block(&block_update(
        1,
        vec![AccountState {
            address_hash: AddressHash(keccak256(a)),
            balance: u256(100),
            nonce: 1,
            code_hash: B256::ZERO,
            code: None,
            storage: hash_storage(HashMap::from([(slot_b256(1), U256::ZERO)])),
            deleted: false,
        }],
    ))
    .unwrap();

    // Recreate slot 1 with new value
    w.commit_block(&block_update(
        2,
        vec![account_state_with_storage(a, 100, 2, [(1, 999)])],
    ))
    .unwrap();

    // Delete slot 1 again
    w.commit_block(&block_update(
        3,
        vec![AccountState {
            address_hash: AddressHash(keccak256(a)),
            balance: u256(100),
            nonce: 3,
            code_hash: B256::ZERO,
            code: None,
            storage: hash_storage(HashMap::from([(slot_b256(1), U256::ZERO)])),
            deleted: false,
        }],
    ))
    .unwrap();

    w.commit_block(&block_update(4, vec![simple_account(a, 200, 4)]))
        .unwrap();
    w.commit_block(&block_update(5, vec![simple_account(a, 300, 5)]))
        .unwrap();

    drop(w);
    let r = reader_for(&tmp, 6);

    // Final state: slot 1 deleted
    assert!(r.get_all_storage(a.into(), 5).unwrap().is_empty());
}

#[test]
fn test_bootstrap_populates_all_namespaces() {
    // Verify that bootstrap_from_snapshot writes to ALL namespaces
    let (w, tmp) = writer_env(3);
    let a = addr(0xaa);

    let accounts = vec![account_state_with_storage(a, 1000, 5, [(1, 100), (2, 200)])];

    let stats = w
        .bootstrap_from_snapshot(
            accounts,
            100,
            B256::repeat_byte(0x11),
            B256::repeat_byte(0x22),
        )
        .unwrap();

    // Should write to all 3 namespaces
    assert_eq!(
        stats.accounts_written, 3,
        "should write to all 3 namespaces"
    );

    drop(w);
    let r = reader_for(&tmp, 3);

    // Verify state
    assert_eq!(r.latest_block_number().unwrap(), Some(100));
    assert!(r.is_block_available(100).unwrap());

    let acc = r.get_account(a.into(), 100).unwrap().unwrap();
    assert_eq!(acc.balance, u256(1000));
    assert_eq!(acc.nonce, 5);

    let storage = r.get_all_storage(a.into(), 100).unwrap();
    assert_eq!(storage.len(), 2);
    assert_eq!(storage.get(&hash_slot(slot_b256(1))), Some(&u256(100)));
    assert_eq!(storage.get(&hash_slot(slot_b256(2))), Some(&u256(200)));
}

#[test]
fn test_bootstrap_block_metadata() {
    let (w, tmp) = writer_env(3);
    let a = addr(0xbb);

    let block_hash = B256::repeat_byte(0x11);
    let state_root = B256::repeat_byte(0x22);

    w.bootstrap_from_snapshot(
        vec![simple_account(a, 1000, 0)],
        100,
        block_hash,
        state_root,
    )
    .unwrap();

    drop(w);
    let r = reader_for(&tmp, 3);

    let meta = r.get_block_metadata(100).unwrap().unwrap();
    assert_eq!(meta.block_hash, block_hash);
    assert_eq!(meta.state_root, state_root);
}

#[test]
fn test_bootstrap_with_code() {
    let (w, tmp) = writer_env(3);
    let a = addr(0xcc);
    let code = vec![0x60, 0x80, 0x60, 0x40, 0x52];
    let code_hash = keccak256(&code);

    let accounts = vec![AccountState {
        address_hash: AddressHash(keccak256(a)),
        balance: u256(0),
        nonce: 1,
        code_hash,
        code: Some(Bytes::from(code.clone())),
        storage: HashMap::new(),
        deleted: false,
    }];

    w.bootstrap_from_snapshot(accounts, 100, B256::ZERO, B256::ZERO)
        .unwrap();

    drop(w);
    let r = reader_for(&tmp, 3);

    let acc = r.get_account(a.into(), 100).unwrap().unwrap();
    assert_eq!(acc.code_hash, code_hash);

    let retrieved_code = r.get_code(code_hash, 100).unwrap();
    assert_eq!(retrieved_code, Some(Bytes::from(code)));
}

#[test]
fn test_bootstrap_multiple_accounts() {
    let (w, tmp) = writer_env(5);

    let accounts: Vec<_> = (1..=10u8)
        .map(|i| {
            account_state_with_storage(
                addr(i),
                u64::from(i) * 100,
                u64::from(i),
                [(1, u64::from(i) * 10)],
            )
        })
        .collect();

    let stats = w
        .bootstrap_from_snapshot(accounts, 50, B256::ZERO, B256::ZERO)
        .unwrap();

    // 10 accounts * 5 namespaces = 50
    assert_eq!(stats.accounts_written, 50);

    drop(w);
    let r = reader_for(&tmp, 5);

    for i in 1..=10u8 {
        let acc = r.get_account(addr(i).into(), 50).unwrap().unwrap();
        assert_eq!(acc.balance, u256(u64::from(i) * 100));
        assert_eq!(acc.nonce, u64::from(i));

        let slot = r
            .get_storage(addr(i).into(), hash_slot(slot_b256(1)), 50)
            .unwrap();
        assert_eq!(slot, Some(u256(u64::from(i) * 10)));
    }
}

#[test]
fn test_bootstrap_then_commit_first_block_no_diffs_needed() {
    // CRITICAL: After bootstrap, the first block in each namespace should NOT need any diffs
    let (w, tmp) = writer_env(3);
    let a = addr(0xdd);

    // Bootstrap at block 100
    w.bootstrap_from_snapshot(
        vec![account_state_with_storage(a, 1000, 0, [(1, 100)])],
        100,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();

    // Block 101 -> namespace 2 (101 % 3 = 2)
    // Namespace 2 has block 100, so start_block = 101, block_number = 101
    // 101 > 101 is FALSE, so no diffs needed!
    let stats_101 = w
        .commit_block(&update_with_storage(101, a, 1100, 1, [(2, 200)]))
        .unwrap();

    assert_eq!(
        stats_101.diffs_applied, 0,
        "Block 101 should NOT need any diffs after bootstrap"
    );

    drop(w);
    let r = reader_for(&tmp, 3);

    // Verify state at block 101
    let acc = r.get_account(a.into(), 101).unwrap().unwrap();
    assert_eq!(acc.balance, u256(1100));
    assert_eq!(acc.nonce, 1);

    let storage = r.get_all_storage(a.into(), 101).unwrap();
    assert_eq!(storage.len(), 2);
    assert_eq!(storage.get(&hash_slot(slot_b256(1))), Some(&u256(100)));
    assert_eq!(storage.get(&hash_slot(slot_b256(2))), Some(&u256(200)));
}

#[test]
fn test_bootstrap_then_commit_sequence_with_diffs() {
    // After bootstrap, commit blocks 101, 102, 103 and verify diff counts
    let (w, tmp) = writer_env(3);
    let a = addr(0xee);

    // Bootstrap at block 100 - all namespaces have block 100
    w.bootstrap_from_snapshot(
        vec![account_state_with_storage(a, 1000, 0, [(1, 100)])],
        100,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();

    // Block 101 -> namespace 2 (101 % 3 = 2)
    // Namespace 2 has block 100, start_block = 101
    // No diffs needed (101 > 101 is false)
    let stats_101 = w
        .commit_block(&update_with_storage(101, a, 1100, 1, [(2, 200)]))
        .unwrap();
    assert_eq!(stats_101.diffs_applied, 0);

    // Block 102 -> namespace 0 (102 % 3 = 0)
    // Namespace 0 has block 100, start_block = 101
    // Needs diff for block 101 (102 > 101 is true, apply 101)
    let stats_102 = w
        .commit_block(&update_with_storage(102, a, 1200, 2, [(3, 300)]))
        .unwrap();
    assert_eq!(
        stats_102.diffs_applied, 1,
        "Block 102 needs 1 diff (block 101)"
    );

    // Block 103 -> namespace 1 (103 % 3 = 1)
    // Namespace 1 has block 100, start_block = 101
    // Needs diffs for blocks 101 and 102
    let stats_103 = w
        .commit_block(&update_with_storage(103, a, 1300, 3, [(4, 400)]))
        .unwrap();
    assert_eq!(
        stats_103.diffs_applied, 2,
        "Block 103 needs 2 diffs (101, 102)"
    );

    drop(w);
    let r = reader_for(&tmp, 3);

    // Block 100 should be rotated out
    assert!(!r.is_block_available(100).unwrap());

    // Blocks 101, 102, 103 should be available
    assert!(r.is_block_available(101).unwrap());
    assert!(r.is_block_available(102).unwrap());
    assert!(r.is_block_available(103).unwrap());

    // Verify cumulative state at block 103
    let acc = r.get_account(a.into(), 103).unwrap().unwrap();
    assert_eq!(acc.balance, u256(1300));
    assert_eq!(acc.nonce, 3);

    let storage = r.get_all_storage(a.into(), 103).unwrap();
    assert_eq!(storage.len(), 4);
    assert_eq!(storage.get(&hash_slot(slot_b256(1))), Some(&u256(100)));
    assert_eq!(storage.get(&hash_slot(slot_b256(2))), Some(&u256(200)));
    assert_eq!(storage.get(&hash_slot(slot_b256(3))), Some(&u256(300)));
    assert_eq!(storage.get(&hash_slot(slot_b256(4))), Some(&u256(400)));
}

#[test]
fn test_bootstrap_full_rotation_cycle() {
    // Bootstrap then process enough blocks for 2 full rotations
    let (w, tmp) = writer_env(3);
    let a = addr(0xff);

    // Bootstrap at block 100
    w.bootstrap_from_snapshot(
        vec![account_state_with_storage(a, 1000, 0, [(1, 100)])],
        100,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();

    // Process blocks 101-106 (2 full rotations)
    for block_num in 101..=106u64 {
        w.commit_block(&update_with_storage(
            block_num,
            a,
            block_num * 10,
            block_num,
            [(block_num, block_num * 100)],
        ))
        .unwrap();
    }

    drop(w);
    let r = reader_for(&tmp, 3);

    // Only blocks 104, 105, 106 should be available
    assert!(!r.is_block_available(103).unwrap());
    assert!(r.is_block_available(104).unwrap());
    assert!(r.is_block_available(105).unwrap());
    assert!(r.is_block_available(106).unwrap());

    // Verify final state at block 106
    let acc = r.get_account(a.into(), 106).unwrap().unwrap();
    assert_eq!(acc.balance, u256(1060));
    assert_eq!(acc.nonce, 106);

    // Should have slots 1 (from bootstrap) + 101-106 = 7 slots
    let storage = r.get_all_storage(a.into(), 106).unwrap();
    assert_eq!(storage.len(), 7);
    assert_eq!(storage.get(&hash_slot(slot_b256(1))), Some(&u256(100)));
    for block_num in 101..=106u64 {
        assert_eq!(
            storage.get(&hash_slot(slot_b256(block_num))),
            Some(&u256(block_num * 100))
        );
    }
}

#[test]
fn test_bootstrap_with_account_deletion_after() {
    let (w, tmp) = writer_env(3);
    let (a, b) = (addr(0x11), addr(0x22));

    // Bootstrap with two accounts
    w.bootstrap_from_snapshot(
        vec![
            account_state_with_storage(a, 1000, 0, [(1, 100)]),
            account_state_with_storage(b, 2000, 0, [(1, 200)]),
        ],
        100,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();

    // Block 101: delete account A
    w.commit_block(&block_update(101, vec![deleted_account(a)]))
        .unwrap();

    // Block 102: update account B
    w.commit_block(&simple_update(102, b, 2500, 1)).unwrap();

    // Block 103: rotation - should apply deletion correctly
    w.commit_block(&simple_update(103, b, 3000, 2)).unwrap();

    drop(w);
    let r = reader_for(&tmp, 3);

    // A should be deleted
    assert!(r.get_account(a.into(), 103).unwrap().is_none());
    assert!(r.get_all_storage(a.into(), 103).unwrap().is_empty());

    // B should exist with correct state
    let acc_b = r.get_account(b.into(), 103).unwrap().unwrap();
    assert_eq!(acc_b.balance, u256(3000));
    assert_eq!(
        r.get_storage(b.into(), hash_slot(slot_b256(1)), 103)
            .unwrap(),
        Some(u256(200))
    );
}

#[test]
fn test_bootstrap_with_storage_deletion_after() {
    let (w, tmp) = writer_env(3);
    let a = addr(0x33);

    // Bootstrap with storage slots 1, 2, 3
    w.bootstrap_from_snapshot(
        vec![account_state_with_storage(
            a,
            1000,
            0,
            [(1, 100), (2, 200), (3, 300)],
        )],
        100,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();

    // Block 101: delete slot 2
    w.commit_block(&block_update(
        101,
        vec![AccountState {
            address_hash: AddressHash(keccak256(a)),
            balance: u256(1000),
            nonce: 1,
            code_hash: B256::ZERO,
            code: None,
            storage: hash_storage(HashMap::from([(slot_b256(2), U256::ZERO)])),
            deleted: false,
        }],
    ))
    .unwrap();

    // Block 102: update slot 1
    w.commit_block(&update_with_storage(102, a, 1000, 2, [(1, 150)]))
        .unwrap();

    // Block 103: add slot 4
    w.commit_block(&update_with_storage(103, a, 1000, 3, [(4, 400)]))
        .unwrap();

    drop(w);
    let r = reader_for(&tmp, 3);

    let storage = r.get_all_storage(a.into(), 103).unwrap();
    assert_eq!(storage.len(), 3); // slots 1, 3, 4 (slot 2 deleted)
    assert_eq!(storage.get(&hash_slot(slot_b256(1))), Some(&u256(150)));
    assert!(!storage.contains_key(&hash_slot(slot_b256(2))));
    assert_eq!(storage.get(&hash_slot(slot_b256(3))), Some(&u256(300)));
    assert_eq!(storage.get(&hash_slot(slot_b256(4))), Some(&u256(400)));
}

#[test]
fn test_bootstrap_large_storage() {
    let (w, tmp) = writer_env(3);
    let a = addr(0x44);

    // Bootstrap with 100 storage slots
    let slots: Vec<_> = (1..=100u64).map(|i| (i, i * 10)).collect();
    w.bootstrap_from_snapshot(
        vec![account_state_with_storage(a, 1000, 0, slots)],
        100,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();

    // Commit a few blocks
    w.commit_block(&simple_update(101, a, 1100, 1)).unwrap();
    w.commit_block(&simple_update(102, a, 1200, 2)).unwrap();
    w.commit_block(&simple_update(103, a, 1300, 3)).unwrap();

    drop(w);
    let r = reader_for(&tmp, 3);

    // All 100 slots should persist
    let storage = r.get_all_storage(a.into(), 103).unwrap();
    assert_eq!(storage.len(), 100);
    for i in 1..=100u64 {
        assert_eq!(storage.get(&hash_slot(slot_b256(i))), Some(&u256(i * 10)));
    }
}

#[test]
fn test_bootstrap_buffer_size_one() {
    // Edge case: buffer_size = 1
    // With buffer_size=1, every block goes to namespace 0
    // No intermediate diffs are ever needed because each block directly replaces the previous
    let (w, tmp) = writer_env(1);
    let a = addr(0x55);

    w.bootstrap_from_snapshot(
        vec![account_state_with_storage(a, 1000, 0, [(1, 100)])],
        100,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();

    // Block 101: namespace 0 has block 100, start_block=101, 101>101 is false → 0 diffs
    let stats_101 = w
        .commit_block(&update_with_storage(101, a, 1100, 1, [(2, 200)]))
        .unwrap();
    assert_eq!(
        stats_101.diffs_applied, 0,
        "buffer_size=1 never needs intermediate diffs"
    );

    // Block 102: namespace 0 has block 101, start_block=102, 102>102 is false → 0 diffs
    let stats_102 = w
        .commit_block(&update_with_storage(102, a, 1200, 2, [(3, 300)]))
        .unwrap();
    assert_eq!(
        stats_102.diffs_applied, 0,
        "buffer_size=1 never needs intermediate diffs"
    );

    drop(w);
    let r = reader_for(&tmp, 1);

    // Only block 102 available (buffer_size=1 means only 1 block retained)
    assert!(!r.is_block_available(100).unwrap());
    assert!(!r.is_block_available(101).unwrap());
    assert!(r.is_block_available(102).unwrap());

    // State is cumulative - the diffs ARE stored, they're just applied during
    // the commit itself (base state copied from previous namespace block)
    let storage = r.get_all_storage(a.into(), 102).unwrap();
    assert_eq!(storage.len(), 3);
    assert_eq!(storage.get(&hash_slot(slot_b256(1))), Some(&u256(100)));
    assert_eq!(storage.get(&hash_slot(slot_b256(2))), Some(&u256(200)));
    assert_eq!(storage.get(&hash_slot(slot_b256(3))), Some(&u256(300)));
}

#[test]
fn test_bootstrap_large_buffer_size() {
    let (w, tmp) = writer_env(100);
    let a = addr(0x66);

    w.bootstrap_from_snapshot(
        vec![account_state_with_storage(a, 1000, 0, [(1, 100)])],
        50,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();

    // Commit 50 blocks - should all be within buffer
    for block_num in 51..=100u64 {
        w.commit_block(&update_with_storage(
            block_num,
            a,
            block_num * 10,
            block_num,
            [(block_num, block_num * 100)],
        ))
        .unwrap();
    }

    drop(w);
    let r = reader_for(&tmp, 100);

    // All blocks 50-100 should be available (buffer_size = 100)
    for block_num in 50..=100u64 {
        assert!(
            r.is_block_available(block_num).unwrap(),
            "Block {block_num} should be available",
        );
    }
}

#[test]
fn test_bootstrap_preserves_state_across_all_namespaces() {
    // Verify that reading from any namespace gives same result after bootstrap
    let (w, tmp) = writer_env(5);
    let a = addr(0x77);

    let code = vec![0x60, 0x80];
    let code_hash = keccak256(&code);

    w.bootstrap_from_snapshot(
        vec![AccountState {
            address_hash: AddressHash(keccak256(a)),
            balance: u256(9999),
            nonce: 42,
            code_hash,
            code: Some(Bytes::from(code.clone())),
            storage: hash_storage(storage([(1, 111), (2, 222), (3, 333)])),
            deleted: false,
        }],
        1000,
        B256::repeat_byte(0xAA),
        B256::repeat_byte(0xBB),
    )
    .unwrap();

    drop(w);
    let r = reader_for(&tmp, 5);

    // Verify complete state
    let acc = r.get_account(a.into(), 1000).unwrap().unwrap();
    assert_eq!(acc.balance, u256(9999));
    assert_eq!(acc.nonce, 42);
    assert_eq!(acc.code_hash, code_hash);

    let storage = r.get_all_storage(a.into(), 1000).unwrap();
    assert_eq!(storage.len(), 3);

    let retrieved_code = r.get_code(code_hash, 1000).unwrap();
    assert_eq!(retrieved_code, Some(Bytes::from(code)));

    let meta = r.get_block_metadata(1000).unwrap().unwrap();
    assert_eq!(meta.block_hash, B256::repeat_byte(0xAA));
    assert_eq!(meta.state_root, B256::repeat_byte(0xBB));
}

#[test]
fn test_bootstrap_empty_accounts_list() {
    let (w, tmp) = writer_env(3);

    w.bootstrap_from_snapshot(
        vec![],
        100,
        B256::repeat_byte(0x11),
        B256::repeat_byte(0x22),
    )
    .unwrap();

    drop(w);
    let r = reader_for(&tmp, 3);

    assert_eq!(r.latest_block_number().unwrap(), Some(100));
    assert!(r.is_block_available(100).unwrap());

    // No accounts to read
    let random_addr = addr(0x99);
    assert!(r.get_account(random_addr.into(), 100).unwrap().is_none());
}

#[test]
fn test_bootstrap_then_complex_rotation_scenario() {
    // Bootstrap, then do complex operations during rotations
    let (writer, temp_dir) = writer_env(3);
    let (addr_a, addr_b, addr_c) = (addr(0x0a), addr(0x0b), addr(0x0c));

    // Bootstrap with 3 accounts
    writer
        .bootstrap_from_snapshot(
            vec![
                account_state_with_storage(addr_a, 1000, 0, [(1, 10)]),
                account_state_with_storage(addr_b, 2000, 0, [(1, 20)]),
                account_state_with_storage(addr_c, 3000, 0, [(1, 30)]),
            ],
            100,
            B256::ZERO,
            B256::ZERO,
        )
        .unwrap();

    // Block 101: update A, delete B
    writer
        .commit_block(&block_update(
            101,
            vec![
                account_state_with_storage(addr_a, 1100, 1, [(2, 110)]),
                deleted_account(addr_b),
            ],
        ))
        .unwrap();

    // Block 102: update C, recreate B
    writer
        .commit_block(&block_update(
            102,
            vec![
                account_state_with_storage(addr_c, 3300, 1, [(2, 330)]),
                account_state_with_storage(addr_b, 5000, 0, [(5, 500)]),
            ],
        ))
        .unwrap();

    // Block 103: rotation - just update A
    writer
        .commit_block(&block_update(
            103,
            vec![account_state_with_storage(addr_a, 1200, 2, [(3, 120)])],
        ))
        .unwrap();

    drop(writer);
    let reader = reader_for(&temp_dir, 3);

    // Verify A at block 103
    let acc_a = reader.get_account(addr_a.into(), 103).unwrap().unwrap();
    assert_eq!(acc_a.balance, u256(1200));
    let storage_a = reader.get_all_storage(addr_a.into(), 103).unwrap();
    assert_eq!(storage_a.len(), 3); // slots 1, 2, 3
    assert_eq!(storage_a.get(&hash_slot(slot_b256(1))), Some(&u256(10)));
    assert_eq!(storage_a.get(&hash_slot(slot_b256(2))), Some(&u256(110)));
    assert_eq!(storage_a.get(&hash_slot(slot_b256(3))), Some(&u256(120)));

    // Verify B at block 103 (recreated)
    let acc_b = reader.get_account(addr_b.into(), 103).unwrap().unwrap();
    assert_eq!(acc_b.balance, u256(5000));
    let storage_b = reader.get_all_storage(addr_b.into(), 103).unwrap();
    assert_eq!(storage_b.len(), 1); // only slot 5 from recreation
    assert!(!storage_b.contains_key(&hash_slot(slot_b256(1)))); // old slot gone

    // Verify C at block 103
    let acc_c = reader.get_account(addr_c.into(), 103).unwrap().unwrap();
    assert_eq!(acc_c.balance, u256(3300));
    let storage_c = reader.get_all_storage(addr_c.into(), 103).unwrap();
    assert_eq!(storage_c.len(), 2); // slots 1, 2
}

#[test]
fn test_bootstrap_high_block_number() {
    // Bootstrap at a high block number (simulating sync from snapshot)
    let (w, tmp) = writer_env(3);
    let a = addr(0x88);

    w.bootstrap_from_snapshot(
        vec![account_state_with_storage(a, 1000, 0, [(1, 100)])],
        10_000_000, // 10 million
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();

    // Commit next few blocks
    for block_num in 10_000_001..=10_000_003u64 {
        w.commit_block(&simple_update(
            block_num,
            a,
            block_num,
            block_num - 10_000_000,
        ))
        .unwrap();
    }

    drop(w);
    let r = reader_for(&tmp, 3);

    assert_eq!(r.latest_block_number().unwrap(), Some(10_000_003));
    assert!(!r.is_block_available(10_000_000).unwrap());
    assert!(r.is_block_available(10_000_001).unwrap());
    assert!(r.is_block_available(10_000_002).unwrap());
    assert!(r.is_block_available(10_000_003).unwrap());
}

#[test]
fn test_bootstrap_zero_balance_and_storage() {
    // Bootstrap with account that has zero balance but non-zero storage
    let (w, tmp) = writer_env(3);
    let a = addr(0x99);

    w.bootstrap_from_snapshot(
        vec![AccountState {
            address_hash: AddressHash(keccak256(a)),
            balance: U256::ZERO,
            nonce: 0,
            code_hash: B256::ZERO,
            code: None,
            storage: hash_storage(storage([(1, 100)])),
            deleted: false,
        }],
        100,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();

    drop(w);
    let r = reader_for(&tmp, 3);

    let acc = r.get_account(a.into(), 100).unwrap().unwrap();
    assert_eq!(acc.balance, U256::ZERO);
    assert_eq!(
        r.get_storage(a.into(), hash_slot(slot_b256(1)), 100)
            .unwrap(),
        Some(u256(100))
    );
}

#[test]
fn test_bootstrap_scan_account_hashes() {
    let (w, tmp) = writer_env(3);

    let accounts: Vec<_> = (0..5u8)
        .map(|i| simple_account(addr(i), u64::from(i) * 100, u64::from(i)))
        .collect();

    w.bootstrap_from_snapshot(accounts, 100, B256::ZERO, B256::ZERO)
        .unwrap();

    drop(w);
    let r = reader_for(&tmp, 3);

    let hashes = r.scan_account_hashes(100).unwrap();
    assert_eq!(hashes.len(), 5);
}

#[test]
fn test_bootstrap_available_block_range() {
    let (w, tmp) = writer_env(5);
    let a = addr(0xab);

    w.bootstrap_from_snapshot(
        vec![simple_account(a, 1000, 0)],
        100,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();

    // Add a few more blocks
    w.commit_block(&simple_update(101, a, 1100, 1)).unwrap();
    w.commit_block(&simple_update(102, a, 1200, 2)).unwrap();

    drop(w);
    let r = reader_for(&tmp, 5);

    // get_available_block_range returns the range of blocks that exist in the buffer.
    // After bootstrap at 100 + commits at 101, 102, the buffer contains blocks 100..=102.
    let range = r.get_available_block_range().unwrap();
    assert_eq!(range, Some((100, 102)));

    // Use is_block_available to check actual block availability
    // After bootstrap at 100 + commits at 101, 102:
    // - ns 0 (100%5=0): block 100
    // - ns 1 (101%5=1): block 101
    // - ns 2 (102%5=2): block 102
    // - ns 3: block 100 (from bootstrap)
    // - ns 4: block 100 (from bootstrap)
    assert!(!r.is_block_available(98).unwrap(), "block 98 never existed");
    assert!(!r.is_block_available(99).unwrap(), "block 99 never existed");
    assert!(
        r.is_block_available(100).unwrap(),
        "block 100 from bootstrap"
    );
    assert!(r.is_block_available(101).unwrap(), "block 101 committed");
    assert!(r.is_block_available(102).unwrap(), "block 102 committed");
}

#[test]
fn test_bootstrap_code_deduplication_across_namespaces() {
    // Same code hash used by multiple accounts - should be stored once per namespace
    let (w, tmp) = writer_env(3);
    let (a, b) = (addr(0xca), addr(0xcb));
    let code = vec![0x60, 0x80, 0x60, 0x40];
    let code_hash = keccak256(&code);

    w.bootstrap_from_snapshot(
        vec![
            AccountState {
                address_hash: AddressHash(keccak256(a)),
                balance: u256(1000),
                nonce: 1,
                code_hash,
                code: Some(Bytes::from(code.clone())),
                storage: HashMap::new(),
                deleted: false,
            },
            AccountState {
                address_hash: AddressHash(keccak256(b)),
                balance: u256(2000),
                nonce: 2,
                code_hash,
                code: Some(Bytes::from(code.clone())),
                storage: HashMap::new(),
                deleted: false,
            },
        ],
        100,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();

    drop(w);
    let r = reader_for(&tmp, 3);

    // Both accounts should have same code hash
    assert_eq!(
        r.get_account(a.into(), 100).unwrap().unwrap().code_hash,
        code_hash
    );
    assert_eq!(
        r.get_account(b.into(), 100).unwrap().unwrap().code_hash,
        code_hash
    );

    // Code should be retrievable
    assert_eq!(r.get_code(code_hash, 100).unwrap(), Some(Bytes::from(code)));
}

#[test]
fn test_bootstrap_stress_many_accounts() {
    // Stress test with many accounts
    let (w, tmp) = writer_env(5);

    let accounts: Vec<_> = (0..100u8)
        .map(|i| {
            account_state_with_storage(
                addr(i),
                u64::from(i) * 100,
                u64::from(i),
                [(1, u64::from(i) * 10)],
            )
        })
        .collect();

    let stats = w
        .bootstrap_from_snapshot(accounts, 1000, B256::ZERO, B256::ZERO)
        .unwrap();

    // 100 accounts * 5 namespaces = 500
    assert_eq!(stats.accounts_written, 500);

    // Commit a few blocks
    for block_num in 1001..=1005u64 {
        let accounts: Vec<_> = (0..10u8)
            .map(|i| {
                account_state_with_storage(
                    addr(i),
                    block_num * 100 + u64::from(i),
                    block_num,
                    [(block_num, block_num * 10)],
                )
            })
            .collect();
        w.commit_block(&block_update(block_num, accounts)).unwrap();
    }

    drop(w);
    let r = reader_for(&tmp, 5);

    // Verify some accounts
    for i in 0..10u8 {
        let acc = r.get_account(addr(i).into(), 1005).unwrap().unwrap();
        assert_eq!(acc.balance, u256(1005 * 100 + u64::from(i)));
    }

    // Unchanged accounts should still have bootstrap values
    for i in 10..100u8 {
        let acc = r.get_account(addr(i).into(), 1005).unwrap().unwrap();
        assert_eq!(acc.balance, u256(u64::from(i) * 100));
    }
}

#[test]
fn test_fix_block_metadata_basic() {
    // Simulate bootstrap that wrote block 0 instead of correct block number
    let (w, tmp) = writer_env(3);
    let a = addr(0xaa);

    // Bootstrap writes to block 0 (simulating missing --block-number)
    w.bootstrap_from_snapshot(
        vec![account_state_with_storage(a, 1000, 5, [(1, 100)])],
        0, // Wrong block number!
        B256::repeat_byte(0x11),
        B256::repeat_byte(0x22),
    )
    .unwrap();

    assert_eq!(w.latest_block_number().unwrap(), Some(0));

    // Fix the metadata to correct block number
    let was_updated = w
        .fix_block_metadata(19_000_000, B256::repeat_byte(0x33), None)
        .unwrap();

    assert!(was_updated);

    // Verify new block number
    assert_eq!(w.latest_block_number().unwrap(), Some(19_000_000));

    drop(w);
    let r = reader_for(&tmp, 3);

    // Block 19_000_000 should be available
    assert!(r.is_block_available(19_000_000).unwrap());
    assert!(!r.is_block_available(0).unwrap());

    // Data should still be accessible
    let acc = r.get_account(a.into(), 19_000_000).unwrap().unwrap();
    assert_eq!(acc.balance, u256(1000));
    assert_eq!(acc.nonce, 5);

    let storage = r.get_all_storage(a.into(), 19_000_000).unwrap();
    assert_eq!(storage.get(&hash_slot(slot_b256(1))), Some(&u256(100)));
}

#[test]
fn test_fix_block_metadata_preserves_state_root() {
    let (w, tmp) = writer_env(3);
    let a = addr(0xbb);

    let original_state_root = B256::repeat_byte(0xBB);

    w.bootstrap_from_snapshot(
        vec![simple_account(a, 1000, 0)],
        0,
        B256::repeat_byte(0xAA),
        original_state_root,
    )
    .unwrap();

    // Fix without providing state_root - should preserve original
    let new_block_hash = B256::repeat_byte(0xCC);
    w.fix_block_metadata(12345, new_block_hash, None).unwrap();

    drop(w);
    let r = reader_for(&tmp, 3);

    let meta = r.get_block_metadata(12345).unwrap().unwrap();
    assert_eq!(meta.block_hash, new_block_hash);
    assert_eq!(meta.state_root, original_state_root); // Preserved
}

#[test]
fn test_fix_block_metadata_with_custom_state_root() {
    let (w, tmp) = writer_env(3);
    let a = addr(0xcc);

    w.bootstrap_from_snapshot(
        vec![simple_account(a, 1000, 0)],
        0,
        B256::repeat_byte(0x11), // Original
        B256::repeat_byte(0x22), // Original
    )
    .unwrap();

    let new_block_hash = B256::repeat_byte(0xDD);
    let new_state_root = B256::repeat_byte(0xEE);

    // Fix with custom hash and root
    w.fix_block_metadata(99999, new_block_hash, Some(new_state_root))
        .unwrap();

    drop(w);
    let r = reader_for(&tmp, 3);

    let meta = r.get_block_metadata(99999).unwrap().unwrap();
    assert_eq!(meta.block_hash, new_block_hash);
    assert_eq!(meta.state_root, new_state_root);
}

#[test]
fn test_fix_block_metadata_no_op_when_same() {
    let (w, _tmp) = writer_env(3);
    let a = addr(0xdd);

    w.bootstrap_from_snapshot(
        vec![simple_account(a, 1000, 0)],
        12345,
        B256::repeat_byte(0x11),
        B256::repeat_byte(0x22),
    )
    .unwrap();

    // Try to "fix" to same block number
    let was_updated = w
        .fix_block_metadata(12345, B256::repeat_byte(0x33), None)
        .unwrap();

    assert!(!was_updated);
}

#[test]
fn test_fix_block_metadata_then_commit_blocks() {
    let (w, tmp) = writer_env(3);
    let a = addr(0xee);

    // Bootstrap at wrong block
    w.bootstrap_from_snapshot(
        vec![account_state_with_storage(a, 1000, 0, [(1, 100)])],
        0,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();

    // Fix to correct block
    w.fix_block_metadata(100, B256::repeat_byte(0x11), None)
        .unwrap();

    // Now commit subsequent blocks
    w.commit_block(&update_with_storage(101, a, 1100, 1, [(2, 200)]))
        .unwrap();
    w.commit_block(&update_with_storage(102, a, 1200, 2, [(3, 300)]))
        .unwrap();
    w.commit_block(&update_with_storage(103, a, 1300, 3, [(4, 400)]))
        .unwrap();

    drop(w);
    let r = reader_for(&tmp, 3);

    // Block 100 should be rotated out
    assert!(!r.is_block_available(100).unwrap());

    // Blocks 101-103 should be available
    assert!(r.is_block_available(101).unwrap());
    assert!(r.is_block_available(102).unwrap());
    assert!(r.is_block_available(103).unwrap());

    // Verify cumulative state at block 103
    let acc = r.get_account(a.into(), 103).unwrap().unwrap();
    assert_eq!(acc.balance, u256(1300));
    assert_eq!(acc.nonce, 3);

    let storage = r.get_all_storage(a.into(), 103).unwrap();
    assert_eq!(storage.len(), 4);
    assert_eq!(storage.get(&hash_slot(slot_b256(1))), Some(&u256(100)));
    assert_eq!(storage.get(&hash_slot(slot_b256(2))), Some(&u256(200)));
    assert_eq!(storage.get(&hash_slot(slot_b256(3))), Some(&u256(300)));
    assert_eq!(storage.get(&hash_slot(slot_b256(4))), Some(&u256(400)));
}

#[test]
fn test_fix_block_metadata_updates_all_namespaces() {
    let (w, tmp) = writer_env(5);
    let a = addr(0xff);

    w.bootstrap_from_snapshot(vec![simple_account(a, 1000, 0)], 0, B256::ZERO, B256::ZERO)
        .unwrap();

    // Fix to high block number
    w.fix_block_metadata(1_000_000, B256::repeat_byte(0x11), None)
        .unwrap();

    drop(w);
    let r = reader_for(&tmp, 5);

    // The block should be available (all namespaces updated)
    assert!(r.is_block_available(1_000_000).unwrap());

    // Data should be accessible
    let acc = r.get_account(a.into(), 1_000_000).unwrap().unwrap();
    assert_eq!(acc.balance, u256(1000));
}

#[test]
fn test_fix_block_metadata_high_block_number() {
    let (w, tmp) = writer_env(3);
    let a = addr(0x11);

    w.bootstrap_from_snapshot(
        vec![account_state_with_storage(
            a,
            5000,
            10,
            [(1, 111), (2, 222)],
        )],
        0,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();

    // Fix to very high block number (mainnet-like)
    let was_updated = w
        .fix_block_metadata(21_500_000, B256::repeat_byte(0xAA), None)
        .unwrap();

    assert!(was_updated);

    drop(w);
    let r = reader_for(&tmp, 3);

    assert_eq!(r.latest_block_number().unwrap(), Some(21_500_000));
    assert!(r.is_block_available(21_500_000).unwrap());

    // Verify data integrity
    let acc = r.get_account(a.into(), 21_500_000).unwrap().unwrap();
    assert_eq!(acc.balance, u256(5000));
    assert_eq!(acc.nonce, 10);

    let storage = r.get_all_storage(a.into(), 21_500_000).unwrap();
    assert_eq!(storage.len(), 2);
}

#[test]
fn test_fix_block_metadata_with_code() {
    let (w, tmp) = writer_env(3);
    let a = addr(0x22);
    let code = vec![0x60, 0x80, 0x60, 0x40, 0x52];
    let code_hash = keccak256(&code);

    w.bootstrap_from_snapshot(
        vec![AccountState {
            address_hash: AddressHash(keccak256(a)),
            balance: u256(0),
            nonce: 1,
            code_hash,
            code: Some(Bytes::from(code.clone())),
            storage: HashMap::new(),
            deleted: false,
        }],
        0,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();

    w.fix_block_metadata(50000, B256::repeat_byte(0x11), None)
        .unwrap();

    drop(w);
    let r = reader_for(&tmp, 3);

    // Code should still be accessible after fix
    let acc = r.get_account(a.into(), 50000).unwrap().unwrap();
    assert_eq!(acc.code_hash, code_hash);

    let retrieved_code = r.get_code(code_hash, 50000).unwrap();
    assert_eq!(retrieved_code, Some(Bytes::from(code)));
}

#[test]
fn test_fix_block_metadata_override_state_root() {
    let (w, tmp) = writer_env(3);
    let a = addr(0x44);

    let original_block_hash = B256::repeat_byte(0xAA);

    w.bootstrap_from_snapshot(
        vec![simple_account(a, 1000, 0)],
        0,
        original_block_hash,
        B256::ZERO, // No state root initially
    )
    .unwrap();

    let new_block_hash = B256::repeat_byte(0xCC);
    let new_state_root = B256::repeat_byte(0xBB);

    // Fix with explicit state_root override
    w.fix_block_metadata(12345, new_block_hash, Some(new_state_root))
        .unwrap();

    drop(w);
    let r = reader_for(&tmp, 3);

    let meta = r.get_block_metadata(12345).unwrap().unwrap();
    assert_eq!(meta.block_hash, new_block_hash);
    assert_eq!(meta.state_root, new_state_root);
}

#[test]
fn test_fix_block_metadata_from_nonzero_block() {
    // Test fixing from a non-zero block to another block
    let (w, tmp) = writer_env(3);
    let a = addr(0x55);

    // Bootstrap at block 100
    w.bootstrap_from_snapshot(
        vec![simple_account(a, 1000, 0)],
        100,
        B256::repeat_byte(0x11),
        B256::repeat_byte(0x22),
    )
    .unwrap();

    // Fix to block 200
    let was_updated = w
        .fix_block_metadata(200, B256::repeat_byte(0x33), None)
        .unwrap();

    assert!(was_updated);

    drop(w);
    let r = reader_for(&tmp, 3);

    assert!(!r.is_block_available(100).unwrap());
    assert!(r.is_block_available(200).unwrap());

    // Metadata should be at new block with new hash but preserved state_root
    let meta = r.get_block_metadata(200).unwrap().unwrap();
    assert_eq!(meta.block_hash, B256::repeat_byte(0x33));
    assert_eq!(meta.state_root, B256::repeat_byte(0x22)); // Preserved

    // Old block metadata should be gone
    assert!(r.get_block_metadata(100).unwrap().is_none());
}

#[test]
fn test_fix_block_metadata_buffer_size_one() {
    let (w, tmp) = writer_env(1);
    let a = addr(0x66);

    w.bootstrap_from_snapshot(
        vec![account_state_with_storage(a, 1000, 0, [(1, 100)])],
        0,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();

    w.fix_block_metadata(5000, B256::ZERO, None).unwrap();

    drop(w);
    let r = reader_for(&tmp, 1);

    assert!(r.is_block_available(5000).unwrap());

    let acc = r.get_account(a.into(), 5000).unwrap().unwrap();
    assert_eq!(acc.balance, u256(1000));
}

#[test]
fn test_fix_block_metadata_large_buffer_size() {
    let (w, tmp) = writer_env(100);
    let a = addr(0x77);

    w.bootstrap_from_snapshot(
        vec![account_state_with_storage(a, 2000, 5, [(1, 111)])],
        0,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();

    w.fix_block_metadata(10_000, B256::ZERO, None).unwrap();

    drop(w);
    let r = reader_for(&tmp, 100);

    assert!(r.is_block_available(10_000).unwrap());

    let acc = r.get_account(a.into(), 10_000).unwrap().unwrap();
    assert_eq!(acc.balance, u256(2000));
    assert_eq!(acc.nonce, 5);
}

#[test]
fn test_fix_block_metadata_then_full_rotation() {
    let (w, tmp) = writer_env(3);
    let a = addr(0x88);

    w.bootstrap_from_snapshot(
        vec![account_state_with_storage(a, 1000, 0, [(1, 100)])],
        0,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();

    w.fix_block_metadata(100, B256::ZERO, None).unwrap();

    // Commit enough blocks for 2 full rotations
    for block_num in 101..=106u64 {
        w.commit_block(&update_with_storage(
            block_num,
            a,
            block_num * 10,
            block_num,
            [(block_num, block_num * 100)],
        ))
        .unwrap();
    }

    drop(w);
    let r = reader_for(&tmp, 3);

    // Only blocks 104, 105, 106 should be available
    assert!(!r.is_block_available(103).unwrap());
    assert!(r.is_block_available(104).unwrap());
    assert!(r.is_block_available(105).unwrap());
    assert!(r.is_block_available(106).unwrap());

    // Verify state is correctly accumulated
    let storage = r.get_all_storage(a.into(), 106).unwrap();
    assert_eq!(storage.len(), 7); // slot 1 from bootstrap + slots 101-106
    assert_eq!(storage.get(&hash_slot(slot_b256(1))), Some(&u256(100)));
    for block_num in 101..=106u64 {
        assert_eq!(
            storage.get(&hash_slot(slot_b256(block_num))),
            Some(&u256(block_num * 100))
        );
    }
}

#[test]
fn test_fix_block_metadata_with_deletions_after() {
    let (w, tmp) = writer_env(3);
    let (a, b) = (addr(0x99), addr(0x9a));

    w.bootstrap_from_snapshot(
        vec![
            account_state_with_storage(a, 1000, 0, [(1, 100)]),
            account_state_with_storage(b, 2000, 0, [(1, 200)]),
        ],
        0,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();

    w.fix_block_metadata(100, B256::ZERO, None).unwrap();

    // Delete account A in next block
    w.commit_block(&block_update(101, vec![deleted_account(a)]))
        .unwrap();

    // Update account B
    w.commit_block(&simple_update(102, b, 2500, 1)).unwrap();

    // Rotation
    w.commit_block(&simple_update(103, b, 3000, 2)).unwrap();

    drop(w);
    let r = reader_for(&tmp, 3);

    // A should be deleted
    assert!(r.get_account(a.into(), 103).unwrap().is_none());

    // B should exist
    let acc_b = r.get_account(b.into(), 103).unwrap().unwrap();
    assert_eq!(acc_b.balance, u256(3000));
}

#[test]
fn test_fix_block_metadata_empty_database_fails() {
    let (w, _tmp) = writer_env(3);

    // Don't bootstrap - database is empty
    // Trying to fix should fail because there's no metadata
    let result = w.fix_block_metadata(100, B256::ZERO, None);

    assert!(result.is_err());
}

#[test]
fn test_block_state_update_json() {
    let update = BlockStateUpdate {
        block_number: 100,
        block_hash: B256::repeat_byte(0x11),
        state_root: B256::repeat_byte(0x22),
        accounts: vec![AccountState {
            address_hash: AddressHash(B256::repeat_byte(0xAA)),
            balance: U256::from(1000),
            nonce: 5,
            code_hash: B256::ZERO,
            code: None,
            storage: HashMap::new(),
            deleted: false,
        }],
    };

    let json = update.to_json().unwrap();
    let decoded = BlockStateUpdate::from_json(&json).unwrap();

    assert_eq!(decoded.block_number, 100);
    assert_eq!(decoded.accounts.len(), 1);
    assert_eq!(decoded.accounts[0].balance, U256::from(1000));
}

#[test]
fn test_commit_stats_default() {
    let stats = CommitStats::default();
    assert_eq!(stats.accounts_written, 0);
    assert_eq!(stats.total_duration, Duration::ZERO);
}

// ============================================================================
// Buffer size migration tests
// ============================================================================

use crate::migration::MigrationResult;

/// Helper: reopen a writer with a different buffer size against an existing DB.
/// The caller MUST have dropped all previous writer/reader handles first.
fn reopen_writer(tmp: &TempDir, buf: u8) -> StateWriter {
    let cfg = CircularBufferConfig::new(buf).unwrap();
    StateWriter::new(tmp.path().join("state"), cfg).unwrap()
}

#[test]
fn test_migration_noop_when_sizes_match() {
    let (w, _tmp) = writer_env(3);
    let a = addr(0x11);

    w.commit_block(&simple_update(0, a, 100, 0)).unwrap();
    w.commit_block(&simple_update(1, a, 200, 1)).unwrap();
    w.commit_block(&simple_update(2, a, 300, 2)).unwrap();

    // Same buffer size — no migration
    assert!(matches!(
        w.migrate_if_needed().unwrap(),
        MigrationResult::NoOp
    ));
}

#[test]
fn test_migration_noop_empty_db() {
    let (w, _tmp) = writer_env(3);

    // Empty DB — no metadata, nothing to migrate
    assert!(matches!(
        w.migrate_if_needed().unwrap(),
        MigrationResult::NoOp
    ));
}

#[test]
fn test_migration_rejects_buffer_increase() {
    let (w, tmp) = writer_env(2);
    let a = addr(0x11);

    w.commit_block(&simple_update(0, a, 100, 0)).unwrap();
    w.commit_block(&simple_update(1, a, 200, 1)).unwrap();
    drop(w);

    // Reopen with larger buffer_size
    let w = reopen_writer(&tmp, 5);
    let err = w.migrate_if_needed().unwrap_err();
    assert!(
        err.to_string().contains("increase not supported"),
        "expected BufferSizeIncrease error, got: {err}"
    );
}

#[test]
fn test_migration_3_to_2() {
    let (w, tmp) = writer_env(3);
    let a = addr(0x11);

    w.bootstrap_from_snapshot(
        vec![account_state_with_storage(a, 1000, 0, [(1, 100)])],
        100,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();

    w.commit_block(&update_with_storage(101, a, 1100, 1, [(2, 200)]))
        .unwrap();
    w.commit_block(&update_with_storage(102, a, 1200, 2, [(3, 300)]))
        .unwrap();

    // Verify pre-migration state
    assert!(w.is_block_available(100).unwrap());
    assert!(w.is_block_available(101).unwrap());
    assert!(w.is_block_available(102).unwrap());

    drop(w);

    // Reopen with buffer_size=2
    let w = reopen_writer(&tmp, 2);
    let result = w.migrate_if_needed().unwrap();
    assert!(
        matches!(
            result,
            MigrationResult::Completed {
                old_size: 3,
                new_size: 2,
                ..
            }
        ),
        "expected Completed(3→2), got: {result:?}"
    );

    if let MigrationResult::Completed {
        cleanup: Some(task),
        ..
    } = result
    {
        task.run().unwrap();
    }

    // After migration:
    // ns 0 = block 102 (102%2=0), ns 1 = block 101 (101%2=1, advanced from 100)
    assert!(w.is_block_available(102).unwrap());
    assert!(w.is_block_available(101).unwrap());
    assert!(!w.is_block_available(100).unwrap());

    // Verify state correctness via writer's Reader impl
    let storage_102 = w.get_all_storage(a.into(), 102).unwrap();
    assert_eq!(storage_102.get(&hash_slot(slot_b256(1))), Some(&u256(100)));
    assert_eq!(storage_102.get(&hash_slot(slot_b256(2))), Some(&u256(200)));
    assert_eq!(storage_102.get(&hash_slot(slot_b256(3))), Some(&u256(300)));

    // Verify state at block 101 (namespace was advanced by applying diff 101)
    let storage_101 = w.get_all_storage(a.into(), 101).unwrap();
    assert_eq!(storage_101.get(&hash_slot(slot_b256(1))), Some(&u256(100)));
    assert_eq!(storage_101.get(&hash_slot(slot_b256(2))), Some(&u256(200)));
    // slot 3 was added in block 102, shouldn't exist at 101
    assert_eq!(storage_101.get(&hash_slot(slot_b256(3))), None);

    // Verify account balances
    let acc_102 = w.get_account(a.into(), 102).unwrap().unwrap();
    assert_eq!(acc_102.balance, u256(1200));
    let acc_101 = w.get_account(a.into(), 101).unwrap().unwrap();
    assert_eq!(acc_101.balance, u256(1100));
}

#[test]
fn test_migration_3_to_1() {
    let (w, tmp) = writer_env(3);
    let a = addr(0x22);

    w.bootstrap_from_snapshot(
        vec![account_state_with_storage(a, 1000, 0, [(1, 100)])],
        100,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();
    w.commit_block(&update_with_storage(101, a, 1100, 1, [(2, 200)]))
        .unwrap();
    w.commit_block(&update_with_storage(102, a, 1200, 2, [(3, 300)]))
        .unwrap();
    drop(w);

    let w = reopen_writer(&tmp, 1);
    let result = w.migrate_if_needed().unwrap();
    assert!(matches!(
        result,
        MigrationResult::Completed {
            old_size: 3,
            new_size: 1,
            ..
        }
    ));

    if let MigrationResult::Completed {
        cleanup: Some(task),
        ..
    } = result
    {
        task.run().unwrap();
    }

    // Only block 102 available
    assert!(w.is_block_available(102).unwrap());
    assert!(!w.is_block_available(101).unwrap());
    assert!(!w.is_block_available(100).unwrap());

    // Verify state at block 102
    let storage = w.get_all_storage(a.into(), 102).unwrap();
    assert_eq!(storage.len(), 3);
    assert_eq!(storage.get(&hash_slot(slot_b256(1))), Some(&u256(100)));
    assert_eq!(storage.get(&hash_slot(slot_b256(2))), Some(&u256(200)));
    assert_eq!(storage.get(&hash_slot(slot_b256(3))), Some(&u256(300)));
}

#[test]
fn test_migration_then_continue_writing() {
    let (w, tmp) = writer_env(3);
    let a = addr(0x33);

    w.bootstrap_from_snapshot(
        vec![account_state_with_storage(a, 1000, 0, [(1, 100)])],
        100,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();
    w.commit_block(&update_with_storage(101, a, 1100, 1, [(2, 200)]))
        .unwrap();
    w.commit_block(&update_with_storage(102, a, 1200, 2, [(3, 300)]))
        .unwrap();
    drop(w);

    // Migrate 3→2 and continue writing
    let w = reopen_writer(&tmp, 2);
    let result = w.migrate_if_needed().unwrap();
    if let MigrationResult::Completed {
        cleanup: Some(task),
        ..
    } = result
    {
        task.run().unwrap();
    }

    // Write new blocks with buffer_size=2
    w.commit_block(&update_with_storage(103, a, 1300, 3, [(4, 400)]))
        .unwrap();
    w.commit_block(&update_with_storage(104, a, 1400, 4, [(5, 500)]))
        .unwrap();

    // With buffer_size=2: blocks 103 and 104 available
    assert!(w.is_block_available(104).unwrap());
    assert!(w.is_block_available(103).unwrap());
    assert!(!w.is_block_available(102).unwrap());

    // Verify cumulative state at block 104
    let storage = w.get_all_storage(a.into(), 104).unwrap();
    assert_eq!(storage.len(), 5);
    assert_eq!(storage.get(&hash_slot(slot_b256(1))), Some(&u256(100)));
    assert_eq!(storage.get(&hash_slot(slot_b256(5))), Some(&u256(500)));

    let acc = w.get_account(a.into(), 104).unwrap().unwrap();
    assert_eq!(acc.balance, u256(1400));
    assert_eq!(acc.nonce, 4);
}

#[test]
fn test_migration_idempotent_on_rerun() {
    // Simulates crash resilience: migration commits but cleanup doesn't finish.
    // Next startup should detect orphaned data and return a cleanup task.
    let (w, tmp) = writer_env(3);
    let a = addr(0x44);

    w.bootstrap_from_snapshot(
        vec![account_state_with_storage(a, 1000, 0, [(1, 100)])],
        100,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();
    w.commit_block(&update_with_storage(101, a, 1100, 1, [(2, 200)]))
        .unwrap();
    w.commit_block(&update_with_storage(102, a, 1200, 2, [(3, 300)]))
        .unwrap();
    drop(w);

    // First migration: 3→2, but DON'T run cleanup
    {
        let w = reopen_writer(&tmp, 2);
        let result = w.migrate_if_needed().unwrap();
        assert!(matches!(
            result,
            MigrationResult::Completed {
                old_size: 3,
                new_size: 2,
                ..
            }
        ));
        // Intentionally skip cleanup — drop the task with the writer
    }

    // Second open: sizes match but orphaned data exists
    {
        let w = reopen_writer(&tmp, 2);
        let result = w.migrate_if_needed().unwrap();
        assert!(
            matches!(
                result,
                MigrationResult::Completed {
                    cleanup: Some(_),
                    ..
                }
            ),
            "expected pending cleanup, got: {result:?}"
        );
        if let MigrationResult::Completed {
            cleanup: Some(task),
            ..
        } = result
        {
            let stats = task.run().unwrap();
            assert!(
                stats.accounts_deleted > 0,
                "should have cleaned up orphaned accounts"
            );
        }
    }

    // Third open: should be fully clean
    let w = reopen_writer(&tmp, 2);
    assert!(matches!(
        w.migrate_if_needed().unwrap(),
        MigrationResult::NoOp
    ));
}

#[test]
fn test_migration_5_to_2() {
    let (w, tmp) = writer_env(5);
    let a = addr(0x55);

    w.bootstrap_from_snapshot(
        vec![account_state_with_storage(a, 1000, 0, [(1, 100)])],
        200,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();

    w.commit_block(&update_with_storage(201, a, 1100, 1, [(2, 200)]))
        .unwrap();
    w.commit_block(&update_with_storage(202, a, 1200, 2, [(3, 300)]))
        .unwrap();
    w.commit_block(&update_with_storage(203, a, 1300, 3, [(4, 400)]))
        .unwrap();
    w.commit_block(&update_with_storage(204, a, 1400, 4, [(5, 500)]))
        .unwrap();
    drop(w);

    // Migrate 5→2
    let w = reopen_writer(&tmp, 2);
    let result = w.migrate_if_needed().unwrap();
    assert!(matches!(
        result,
        MigrationResult::Completed {
            old_size: 5,
            new_size: 2,
            ..
        }
    ));

    if let MigrationResult::Completed {
        cleanup: Some(task),
        ..
    } = result
    {
        task.run().unwrap();
    }

    assert!(w.is_block_available(204).unwrap());
    assert!(w.is_block_available(203).unwrap());
    assert!(!w.is_block_available(202).unwrap());

    // Verify state at 204
    let storage = w.get_all_storage(a.into(), 204).unwrap();
    assert_eq!(storage.len(), 5);
    assert_eq!(storage.get(&hash_slot(slot_b256(5))), Some(&u256(500)));

    let acc = w.get_account(a.into(), 204).unwrap().unwrap();
    assert_eq!(acc.balance, u256(1400));

    // Verify state at 203
    let acc_203 = w.get_account(a.into(), 203).unwrap().unwrap();
    assert_eq!(acc_203.balance, u256(1300));
}

#[test]
fn test_migration_multiple_accounts_with_code() {
    // Two accounts: one EOA, one contract with bytecode and storage.
    // Verifies bytecodes, storage, and balances survive migration for both.
    let (w, tmp) = writer_env(3);
    let eoa = addr(0xAA);
    let contract = addr(0xBB);
    let code = vec![0x60, 0x80, 0x60, 0x40, 0x52]; // PUSH1 0x80 PUSH1 0x40 MSTORE
    let code_hash = keccak256(&code);

    // Bootstrap both accounts
    w.bootstrap_from_snapshot(
        vec![
            account_state_with_storage(eoa, 5000, 0, [(1, 10)]),
            AccountState {
                address_hash: AddressHash(keccak256(contract)),
                balance: u256(0),
                nonce: 1,
                code_hash,
                code: Some(Bytes::from(code.clone())),
                storage: hash_storage(storage([(100, 999), (200, 888)])),
                deleted: false,
            },
        ],
        50,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();

    // Block 51: update EOA balance + contract storage
    w.commit_block(&block_update(
        51,
        vec![
            simple_account(eoa, 4500, 1),
            AccountState {
                address_hash: AddressHash(keccak256(contract)),
                balance: u256(500),
                nonce: 2,
                code_hash,
                code: None,
                storage: hash_storage(storage([(300, 777)])),
                deleted: false,
            },
        ],
    ))
    .unwrap();

    // Block 52: only EOA changes
    w.commit_block(&block_update(52, vec![simple_account(eoa, 4000, 2)]))
        .unwrap();

    drop(w);

    // Migrate 3→2
    let w = reopen_writer(&tmp, 2);
    let result = w.migrate_if_needed().unwrap();
    assert!(matches!(
        result,
        MigrationResult::Completed {
            old_size: 3,
            new_size: 2,
            ..
        }
    ));
    if let MigrationResult::Completed {
        cleanup: Some(task),
        ..
    } = result
    {
        let stats = task.run().unwrap();
        // Orphaned namespace 2 had data from bootstrap
        assert!(stats.accounts_deleted > 0);
        assert!(
            stats.bytecodes_deleted > 0,
            "bytecodes should be cleaned from orphaned ns"
        );
    }

    // Verify EOA at block 52 (ns 0)
    let eoa_52 = w.get_account(eoa.into(), 52).unwrap().unwrap();
    assert_eq!(eoa_52.balance, u256(4000));
    assert_eq!(eoa_52.nonce, 2);

    // Verify EOA at block 51 (ns 1, was advanced from block 50)
    let eoa_51 = w.get_account(eoa.into(), 51).unwrap().unwrap();
    assert_eq!(eoa_51.balance, u256(4500));

    // Verify contract bytecode at block 52
    let retrieved_code = w.get_code(code_hash, 52).unwrap();
    assert_eq!(retrieved_code, Some(Bytes::from(code.clone())));

    // Verify contract storage at block 52 (unchanged since block 51)
    let cstorage = w.get_all_storage(contract.into(), 52).unwrap();
    assert_eq!(cstorage.get(&hash_slot(slot_b256(100))), Some(&u256(999)));
    assert_eq!(cstorage.get(&hash_slot(slot_b256(200))), Some(&u256(888)));
    assert_eq!(cstorage.get(&hash_slot(slot_b256(300))), Some(&u256(777)));

    // Verify contract storage at block 51 (ns advanced)
    let cstorage_51 = w.get_all_storage(contract.into(), 51).unwrap();
    assert_eq!(
        cstorage_51.get(&hash_slot(slot_b256(300))),
        Some(&u256(777))
    );

    // Verify bytecode at block 51
    assert_eq!(w.get_code(code_hash, 51).unwrap(), Some(Bytes::from(code)));
}

#[test]
fn test_migration_with_deleted_account() {
    // Account exists, then gets deleted in a later block.
    // Migration must correctly carry the deletion forward.
    let (w, tmp) = writer_env(3);
    let a = addr(0xDD);

    w.bootstrap_from_snapshot(
        vec![account_state_with_storage(a, 1000, 5, [(1, 100), (2, 200)])],
        100,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();

    // Block 101: account still alive, modify storage
    w.commit_block(&update_with_storage(101, a, 1100, 6, [(3, 300)]))
        .unwrap();

    // Block 102: delete the account
    w.commit_block(&block_update(102, vec![deleted_account(a)]))
        .unwrap();

    drop(w);

    // Migrate 3→1
    let w = reopen_writer(&tmp, 1);
    let result = w.migrate_if_needed().unwrap();
    assert!(matches!(
        result,
        MigrationResult::Completed {
            old_size: 3,
            new_size: 1,
            ..
        }
    ));
    if let MigrationResult::Completed {
        cleanup: Some(task),
        ..
    } = result
    {
        task.run().unwrap();
    }

    // At block 102, account should be deleted (no data)
    let acc = w.get_account(a.into(), 102).unwrap();
    assert!(acc.is_none(), "deleted account should not be found");

    let storage = w.get_all_storage(a.into(), 102).unwrap();
    assert!(storage.is_empty(), "deleted account should have no storage");
}

#[test]
fn test_migration_storage_slot_deletion() {
    // Storage slot set to zero (deletion) must survive migration.
    let (w, tmp) = writer_env(3);
    let a = addr(0xEE);

    w.bootstrap_from_snapshot(
        vec![account_state_with_storage(
            a,
            1000,
            0,
            [(1, 100), (2, 200), (3, 300)],
        )],
        100,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();

    // Block 101: delete slot 2 by setting to zero, add slot 4
    w.commit_block(&block_update(
        101,
        vec![AccountState {
            address_hash: AddressHash(keccak256(a)),
            balance: u256(1000),
            nonce: 1,
            code_hash: B256::ZERO,
            code: None,
            storage: hash_storage(storage([(2, 0), (4, 400)])),
            deleted: false,
        }],
    ))
    .unwrap();

    // Block 102: modify slot 1
    w.commit_block(&update_with_storage(102, a, 1200, 2, [(1, 150)]))
        .unwrap();

    drop(w);

    // Migrate 3→2: ns 1 must advance from block 100 to 101
    let w = reopen_writer(&tmp, 2);
    w.migrate_if_needed().unwrap();

    // At block 101 (advanced namespace): slot 2 should be gone, slot 4 should exist
    let s101 = w.get_all_storage(a.into(), 101).unwrap();
    assert_eq!(s101.get(&hash_slot(slot_b256(1))), Some(&u256(100))); // unchanged
    assert_eq!(s101.get(&hash_slot(slot_b256(2))), None); // deleted
    assert_eq!(s101.get(&hash_slot(slot_b256(3))), Some(&u256(300))); // unchanged
    assert_eq!(s101.get(&hash_slot(slot_b256(4))), Some(&u256(400))); // new

    // At block 102: slot 1 updated
    let s102 = w.get_all_storage(a.into(), 102).unwrap();
    assert_eq!(s102.get(&hash_slot(slot_b256(1))), Some(&u256(150)));
    assert_eq!(s102.get(&hash_slot(slot_b256(2))), None); // still deleted
    assert_eq!(s102.get(&hash_slot(slot_b256(4))), Some(&u256(400)));
}

#[test]
fn test_migration_sequential_reductions_3_to_2_to_1() {
    let (w, tmp) = writer_env(3);
    let a = addr(0x77);

    w.bootstrap_from_snapshot(
        vec![account_state_with_storage(a, 1000, 0, [(1, 100)])],
        100,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();
    w.commit_block(&update_with_storage(101, a, 1100, 1, [(2, 200)]))
        .unwrap();
    w.commit_block(&update_with_storage(102, a, 1200, 2, [(3, 300)]))
        .unwrap();
    drop(w);

    // Step 1: migrate 3→2
    let w = reopen_writer(&tmp, 2);
    let result = w.migrate_if_needed().unwrap();
    assert!(matches!(
        result,
        MigrationResult::Completed {
            old_size: 3,
            new_size: 2,
            ..
        }
    ));
    if let MigrationResult::Completed {
        cleanup: Some(task),
        ..
    } = result
    {
        task.run().unwrap();
    }

    // Write one more block to exercise the buffer with size 2
    w.commit_block(&update_with_storage(103, a, 1300, 3, [(4, 400)]))
        .unwrap();

    assert!(w.is_block_available(103).unwrap());
    assert!(w.is_block_available(102).unwrap());
    assert!(!w.is_block_available(101).unwrap());
    drop(w);

    // Step 2: migrate 2→1
    let w = reopen_writer(&tmp, 1);
    let result = w.migrate_if_needed().unwrap();
    assert!(matches!(
        result,
        MigrationResult::Completed {
            old_size: 2,
            new_size: 1,
            ..
        }
    ));
    if let MigrationResult::Completed {
        cleanup: Some(task),
        ..
    } = result
    {
        let stats = task.run().unwrap();
        assert!(stats.accounts_deleted > 0);
    }

    // Only block 103 should be available
    assert!(w.is_block_available(103).unwrap());
    assert!(!w.is_block_available(102).unwrap());

    // Verify cumulative state at 103
    let storage = w.get_all_storage(a.into(), 103).unwrap();
    assert_eq!(storage.len(), 4);
    assert_eq!(storage.get(&hash_slot(slot_b256(1))), Some(&u256(100)));
    assert_eq!(storage.get(&hash_slot(slot_b256(4))), Some(&u256(400)));

    // Write another block after double migration
    w.commit_block(&update_with_storage(104, a, 1400, 4, [(5, 500)]))
        .unwrap();
    assert!(w.is_block_available(104).unwrap());
    assert!(!w.is_block_available(103).unwrap()); // rotated out with buffer_size=1

    let acc = w.get_account(a.into(), 104).unwrap().unwrap();
    assert_eq!(acc.balance, u256(1400));
    assert_eq!(acc.nonce, 4);
}

#[test]
fn test_migration_block_range_and_metadata() {
    // Verify get_available_block_range and get_block_metadata after migration.
    let (w, tmp) = writer_env(3);
    let a = addr(0x88);

    w.bootstrap_from_snapshot(
        vec![simple_account(a, 1000, 0)],
        100,
        B256::repeat_byte(0x11),
        B256::repeat_byte(0x22),
    )
    .unwrap();
    w.commit_block(&simple_update(101, a, 1100, 1)).unwrap();
    w.commit_block(&simple_update(102, a, 1200, 2)).unwrap();

    // Pre-migration: all 3 blocks available
    let range = w.get_available_block_range().unwrap().unwrap();
    assert_eq!(range, (100, 102));
    drop(w);

    // Migrate 3→2
    let w = reopen_writer(&tmp, 2);
    w.migrate_if_needed().unwrap();

    // Post-migration: range should be (101, 102)
    let range = w.get_available_block_range().unwrap().unwrap();
    assert_eq!(range, (101, 102));

    // Block metadata for 102 should still exist
    let meta_102 = w.get_block_metadata(102).unwrap();
    assert!(meta_102.is_some());

    // Block metadata for 101 should still exist
    let meta_101 = w.get_block_metadata(101).unwrap();
    assert!(meta_101.is_some());

    // Block metadata for 100 should be pruned
    let meta_100 = w.get_block_metadata(100).unwrap();
    assert!(
        meta_100.is_none(),
        "block 100 metadata should be pruned after migration"
    );
}

#[test]
fn test_migration_early_blocks() {
    // Migration with low block numbers (0, 1, 2) to test underflow safety.
    let (w, tmp) = writer_env(3);
    let a = addr(0x99);

    w.commit_block(&update_with_storage(0, a, 100, 0, [(1, 10)]))
        .unwrap();
    w.commit_block(&update_with_storage(1, a, 200, 1, [(2, 20)]))
        .unwrap();
    w.commit_block(&update_with_storage(2, a, 300, 2, [(3, 30)]))
        .unwrap();
    drop(w);

    // Migrate 3→2
    let w = reopen_writer(&tmp, 2);
    let result = w.migrate_if_needed().unwrap();
    assert!(matches!(
        result,
        MigrationResult::Completed {
            old_size: 3,
            new_size: 2,
            ..
        }
    ));

    // With latest=2, new_size=2: target ns 0 = block 2, target ns 1 = block 1
    assert!(w.is_block_available(2).unwrap());
    assert!(w.is_block_available(1).unwrap());
    assert!(!w.is_block_available(0).unwrap());

    let acc = w.get_account(a.into(), 2).unwrap().unwrap();
    assert_eq!(acc.balance, u256(300));

    let acc_1 = w.get_account(a.into(), 1).unwrap().unwrap();
    assert_eq!(acc_1.balance, u256(200));

    // Storage: block 1 should have slots 1,2 but not 3
    let s1 = w.get_all_storage(a.into(), 1).unwrap();
    assert_eq!(s1.get(&hash_slot(slot_b256(1))), Some(&u256(10)));
    assert_eq!(s1.get(&hash_slot(slot_b256(2))), Some(&u256(20)));
    assert_eq!(s1.get(&hash_slot(slot_b256(3))), None);
}

#[test]
fn test_migration_early_blocks_to_1() {
    // Separate test for sequential 3→2→1 with early blocks
    let (w, tmp) = writer_env(3);
    let a = addr(0x99);

    w.commit_block(&update_with_storage(0, a, 100, 0, [(1, 10)]))
        .unwrap();
    w.commit_block(&update_with_storage(1, a, 200, 1, [(2, 20)]))
        .unwrap();
    w.commit_block(&update_with_storage(2, a, 300, 2, [(3, 30)]))
        .unwrap();
    drop(w);

    // Migrate 3→1 directly
    let w = reopen_writer(&tmp, 1);
    let result = w.migrate_if_needed().unwrap();
    assert!(matches!(
        result,
        MigrationResult::Completed {
            old_size: 3,
            new_size: 1,
            ..
        }
    ));

    assert!(w.is_block_available(2).unwrap());
    assert!(!w.is_block_available(1).unwrap());
    assert!(!w.is_block_available(0).unwrap());
}

#[test]
fn test_migration_cleanup_stats_detailed() {
    // Verify cleanup stats report correct counts for accounts, storage, and bytecodes.
    let (w, tmp) = writer_env(3);
    let a1 = addr(0xA1);
    let a2 = addr(0xA2);
    let code = vec![0x60, 0x01];
    let code_hash = keccak256(&code);

    w.bootstrap_from_snapshot(
        vec![
            account_state_with_storage(a1, 1000, 0, [(1, 10), (2, 20)]),
            AccountState {
                address_hash: AddressHash(keccak256(a2)),
                balance: u256(500),
                nonce: 0,
                code_hash,
                code: Some(Bytes::from(code)),
                storage: hash_storage(storage([(10, 100)])),
                deleted: false,
            },
        ],
        100,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();

    w.commit_block(&simple_update(101, a1, 1100, 1)).unwrap();
    w.commit_block(&simple_update(102, a1, 1200, 2)).unwrap();
    drop(w);

    // Migrate 3→1
    let w = reopen_writer(&tmp, 1);
    let result = w.migrate_if_needed().unwrap();

    if let MigrationResult::Completed {
        cleanup: Some(task),
        ..
    } = result
    {
        let stats = task.run().unwrap();
        // Orphaned namespaces 1 and 2 each had 2 accounts from bootstrap
        assert!(
            stats.accounts_deleted >= 2,
            "expected at least 2 orphaned accounts deleted, got {}",
            stats.accounts_deleted
        );
        assert!(
            stats.storage_slots_deleted >= 2,
            "expected at least 2 orphaned storage slots deleted, got {}",
            stats.storage_slots_deleted
        );
        assert!(
            stats.bytecodes_deleted >= 1,
            "expected at least 1 orphaned bytecode deleted, got {}",
            stats.bytecodes_deleted
        );
    } else {
        panic!("expected MigrationResult::Completed with cleanup");
    }
}

#[test]
fn test_migration_heavy_rotation_after_reduction() {
    // After migration, write enough blocks to exercise multiple full rotations
    // of the new (smaller) buffer. This tests that the normal rotation logic
    // works correctly on top of migrated state.
    let (w, tmp) = writer_env(4);
    let a = addr(0xCC);

    w.bootstrap_from_snapshot(
        vec![account_state_with_storage(a, 1000, 0, [(1, 100)])],
        50,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();
    for b in 51..=53 {
        w.commit_block(&update_with_storage(b, a, 1000 + b * 10, b, [(b, b * 10)]))
            .unwrap();
    }
    drop(w);

    // Migrate 4→2
    let w = reopen_writer(&tmp, 2);
    w.migrate_if_needed().unwrap();

    // Write 10 more blocks — 5 full rotations of buffer_size=2
    for b in 54..=63 {
        w.commit_block(&update_with_storage(b, a, 1000 + b * 10, b, [(b, b * 10)]))
            .unwrap();
    }

    // Only the last 2 blocks should be available
    assert!(w.is_block_available(63).unwrap());
    assert!(w.is_block_available(62).unwrap());
    assert!(!w.is_block_available(61).unwrap());

    // Verify cumulative storage at block 63 has all slots from 1 through 63
    let storage = w.get_all_storage(a.into(), 63).unwrap();
    // Original slot 1 + slots 51..=63 = 14 slots
    assert_eq!(storage.len(), 14);
    assert_eq!(storage.get(&hash_slot(slot_b256(1))), Some(&u256(100)));
    assert_eq!(storage.get(&hash_slot(slot_b256(63))), Some(&u256(630)));

    let acc = w.get_account(a.into(), 63).unwrap().unwrap();
    assert_eq!(acc.balance, u256(1630));
    assert_eq!(acc.nonce, 63);
}

#[test]
fn test_migration_different_accounts_per_block() {
    // Different accounts are modified in different blocks.
    // Tests that migration correctly applies diffs that touch different accounts.
    let (w, tmp) = writer_env(3);
    let a = addr(0xD1);
    let b_addr = addr(0xD2);
    let c = addr(0xD3);

    // Bootstrap with account A
    w.bootstrap_from_snapshot(
        vec![account_state_with_storage(a, 1000, 0, [(1, 10)])],
        100,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();

    // Block 101: introduce account B (new account in diff)
    w.commit_block(&block_update(
        101,
        vec![account_state_with_storage(b_addr, 2000, 0, [(1, 20)])],
    ))
    .unwrap();

    // Block 102: introduce account C, modify A
    w.commit_block(&block_update(
        102,
        vec![
            simple_account(a, 1500, 1),
            account_state_with_storage(c, 3000, 0, [(1, 30)]),
        ],
    ))
    .unwrap();

    drop(w);

    // Migrate 3→2: ns 1 advances from block 100 to 101
    let w = reopen_writer(&tmp, 2);
    w.migrate_if_needed().unwrap();

    // At block 101: A exists (from bootstrap), B exists (from block 101), C doesn't
    let a_101 = w.get_account(a.into(), 101).unwrap().unwrap();
    assert_eq!(a_101.balance, u256(1000)); // unchanged at 101
    let b_101 = w.get_account(b_addr.into(), 101).unwrap().unwrap();
    assert_eq!(b_101.balance, u256(2000));
    let c_101 = w.get_account(c.into(), 101).unwrap();
    assert!(c_101.is_none(), "account C shouldn't exist at block 101");

    // At block 102: all three exist
    let a_102 = w.get_account(a.into(), 102).unwrap().unwrap();
    assert_eq!(a_102.balance, u256(1500));
    let b_102 = w.get_account(b_addr.into(), 102).unwrap().unwrap();
    assert_eq!(b_102.balance, u256(2000)); // unchanged
    let c_102 = w.get_account(c.into(), 102).unwrap().unwrap();
    assert_eq!(c_102.balance, u256(3000));
}

#[test]
fn test_migration_rebuild_path_then_write() {
    // Triggers the rebuild path (current > target due to modulo remapping)
    // and then writes blocks that rotate into the rebuilt namespace, proving
    // the rebuilt state is correct for the normal writer rotation logic.
    //
    // Setup: buffer_size=3, bootstrap at block 50, write 51 and 52.
    //   ns 0 = block 51 (51%3=0)
    //   ns 1 = block 52 (52%3=1)
    //   ns 2 = block 50 (bootstrap)
    //
    // Migrate 3→2:
    //   target ns 0 = block 52 (52%2=0)  ← advance from 51
    //   target ns 1 = block 51 (51%2=1)  ← REBUILD (has 52, needs 51)
    let (w, tmp) = writer_env(3);
    let a = addr(0xF1);

    w.bootstrap_from_snapshot(
        vec![account_state_with_storage(a, 1000, 0, [(1, 100)])],
        50,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();
    w.commit_block(&update_with_storage(51, a, 1100, 1, [(2, 200)]))
        .unwrap();
    w.commit_block(&update_with_storage(52, a, 1200, 2, [(3, 300)]))
        .unwrap();
    drop(w);

    let w = reopen_writer(&tmp, 2);
    w.migrate_if_needed().unwrap();

    // Verify rebuilt namespace (ns 1 = block 51)
    let acc_51 = w.get_account(a.into(), 51).unwrap().unwrap();
    assert_eq!(acc_51.balance, u256(1100));
    let s51 = w.get_all_storage(a.into(), 51).unwrap();
    assert_eq!(s51.get(&hash_slot(slot_b256(1))), Some(&u256(100)));
    assert_eq!(s51.get(&hash_slot(slot_b256(2))), Some(&u256(200)));
    assert_eq!(s51.get(&hash_slot(slot_b256(3))), None); // added at 52

    // Verify advanced namespace (ns 0 = block 52)
    let acc_52 = w.get_account(a.into(), 52).unwrap().unwrap();
    assert_eq!(acc_52.balance, u256(1200));

    // Write block 53 → 53%2=1 → ns 1 (the rebuilt one). The writer's
    // load_base_batches will see ns 1 has block 51 and apply diff 52 as
    // an intermediate before writing 53's changes.
    w.commit_block(&update_with_storage(53, a, 1300, 3, [(4, 400)]))
        .unwrap();
    assert!(w.is_block_available(53).unwrap());

    let acc_53 = w.get_account(a.into(), 53).unwrap().unwrap();
    assert_eq!(acc_53.balance, u256(1300));
    let s53 = w.get_all_storage(a.into(), 53).unwrap();
    assert_eq!(s53.len(), 4); // slots 1,2,3,4
    assert_eq!(s53.get(&hash_slot(slot_b256(4))), Some(&u256(400)));

    // Write block 54 → 54%2=0 → ns 0. Normal rotation.
    w.commit_block(&update_with_storage(54, a, 1400, 4, [(5, 500)]))
        .unwrap();
    assert!(w.is_block_available(54).unwrap());
    assert!(w.is_block_available(53).unwrap());
    assert!(!w.is_block_available(52).unwrap());
}

#[test]
fn test_migration_latest_block_number_preserved() {
    // Verify latest_block_number() returns the correct value after migration.
    let (w, tmp) = writer_env(3);
    let a = addr(0xF2);

    w.bootstrap_from_snapshot(
        vec![simple_account(a, 1000, 0)],
        100,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();
    w.commit_block(&simple_update(101, a, 1100, 1)).unwrap();
    w.commit_block(&simple_update(102, a, 1200, 2)).unwrap();

    assert_eq!(w.latest_block_number().unwrap(), Some(102));
    drop(w);

    let w = reopen_writer(&tmp, 2);
    // Before migration, latest_block should still be readable
    assert_eq!(w.latest_block_number().unwrap(), Some(102));

    w.migrate_if_needed().unwrap();

    // After migration, latest_block must be unchanged
    assert_eq!(w.latest_block_number().unwrap(), Some(102));
}

#[test]
fn test_migration_rebuild_with_multiple_diffs() {
    // Rebuild path where source is 2 blocks behind target,
    // requiring multiple diffs to be applied after copy.
    //
    // buffer_size=4, bootstrap at 40, write 41..=43.
    //   ns 0 = block 40 (40%4=0, bootstrap)
    //   ns 1 = block 41 (41%4=1)
    //   ns 2 = block 42 (42%4=2)
    //   ns 3 = block 43 (43%4=3)
    //
    // Migrate 4→2:
    //   target ns 0 = block 42 (42%2=0)
    //   target ns 1 = block 43 (43%2=1)
    //
    //   ns 0 has block 40, target 42 → advance (apply diffs 41, 42)
    //   ns 1 has block 41, target 43 → advance (apply diffs 42, 43)
    let (w, tmp) = writer_env(4);
    let a = addr(0xF3);

    w.bootstrap_from_snapshot(
        vec![account_state_with_storage(a, 1000, 0, [(1, 100)])],
        40,
        B256::ZERO,
        B256::ZERO,
    )
    .unwrap();
    w.commit_block(&update_with_storage(41, a, 1100, 1, [(2, 200)]))
        .unwrap();
    w.commit_block(&update_with_storage(42, a, 1200, 2, [(3, 300)]))
        .unwrap();
    w.commit_block(&update_with_storage(43, a, 1300, 3, [(4, 400)]))
        .unwrap();
    drop(w);

    let w = reopen_writer(&tmp, 2);
    w.migrate_if_needed().unwrap();

    // ns 0 should have block 42 state (advanced from 40 by diffs 41, 42)
    let acc_42 = w.get_account(a.into(), 42).unwrap().unwrap();
    assert_eq!(acc_42.balance, u256(1200));
    let s42 = w.get_all_storage(a.into(), 42).unwrap();
    assert_eq!(s42.len(), 3); // slots 1,2,3
    assert_eq!(s42.get(&hash_slot(slot_b256(3))), Some(&u256(300)));
    assert_eq!(s42.get(&hash_slot(slot_b256(4))), None); // added at 43

    // ns 1 should have block 43 state (advanced from 41 by diffs 42, 43)
    let acc_43 = w.get_account(a.into(), 43).unwrap().unwrap();
    assert_eq!(acc_43.balance, u256(1300));
    let s43 = w.get_all_storage(a.into(), 43).unwrap();
    assert_eq!(s43.len(), 4); // slots 1,2,3,4

    // Continue writing to confirm rotation works
    w.commit_block(&update_with_storage(44, a, 1400, 4, [(5, 500)]))
        .unwrap();
    assert!(w.is_block_available(44).unwrap());
    assert!(w.is_block_available(43).unwrap());
    assert!(!w.is_block_available(42).unwrap());
}

#[test]
fn test_migration_3_to_1_ns0_already_at_target() {
    // Verifies the no-op path: ns 0 already holds the latest block and
    // needs zero diffs. After 3→1, ONLY block 102 is reachable.
    //
    // State before migration:
    //   ns 0 = block 102  (102 % 3 = 0)  ← latest, cumulative state
    //   ns 1 = block 100  (100 % 3 = 1)  ← bootstrap state
    //   ns 2 = block 101  (101 % 3 = 2)
    //
    // Migrate 3→1:
    //   target ns 0 = block 102  (102 % 1 = 0) → already there, no work
    //   ns 1, ns 2 → orphaned
    let (w, tmp) = writer_env(3);
    let a = addr(0xE1);

    w.bootstrap_from_snapshot(
        vec![account_state_with_storage(a, 1000, 0, [(1, 100)])],
        100,
        B256::repeat_byte(0x10),
        B256::repeat_byte(0x20),
    )
    .unwrap();
    w.commit_block(&update_with_storage(101, a, 1100, 1, [(2, 200)]))
        .unwrap();
    w.commit_block(&update_with_storage(102, a, 1200, 2, [(3, 300)]))
        .unwrap();

    // Sanity: all 3 blocks available before migration
    assert!(w.is_block_available(100).unwrap());
    assert!(w.is_block_available(101).unwrap());
    assert!(w.is_block_available(102).unwrap());
    assert_eq!(w.latest_block_number().unwrap(), Some(102));
    drop(w);

    // Migrate 3→1
    let w = reopen_writer(&tmp, 1);
    let result = w.migrate_if_needed().unwrap();
    assert!(matches!(
        result,
        MigrationResult::Completed {
            old_size: 3,
            new_size: 1,
            ..
        }
    ));

    // latest_block must be preserved
    assert_eq!(w.latest_block_number().unwrap(), Some(102));

    // Only block 102 reachable
    assert!(w.is_block_available(102).unwrap());
    assert!(!w.is_block_available(101).unwrap());
    assert!(!w.is_block_available(100).unwrap());

    // Block range should be exactly (102, 102)
    assert_eq!(w.get_available_block_range().unwrap(), Some((102, 102)));

    // Full state at block 102: cumulative across all 3 commits
    let acc = w.get_account(a.into(), 102).unwrap().unwrap();
    assert_eq!(acc.balance, u256(1200));
    assert_eq!(acc.nonce, 2);

    let storage = w.get_all_storage(a.into(), 102).unwrap();
    assert_eq!(storage.len(), 3);
    assert_eq!(storage.get(&hash_slot(slot_b256(1))), Some(&u256(100)));
    assert_eq!(storage.get(&hash_slot(slot_b256(2))), Some(&u256(200)));
    assert_eq!(storage.get(&hash_slot(slot_b256(3))), Some(&u256(300)));

    // Block metadata for 102 must survive, 100 and 101 must be pruned
    assert!(w.get_block_metadata(102).unwrap().is_some());
    assert!(w.get_block_metadata(101).unwrap().is_none());
    assert!(w.get_block_metadata(100).unwrap().is_none());

    // Continue writing — next block goes to ns 0 (103 % 1 = 0)
    w.commit_block(&update_with_storage(103, a, 1300, 3, [(4, 400)]))
        .unwrap();
    assert!(w.is_block_available(103).unwrap());
    assert!(!w.is_block_available(102).unwrap()); // rotated out, buffer=1

    let acc_103 = w.get_account(a.into(), 103).unwrap().unwrap();
    assert_eq!(acc_103.balance, u256(1300));
    let s103 = w.get_all_storage(a.into(), 103).unwrap();
    assert_eq!(s103.len(), 4);
}

#[test]
fn test_migration_3_to_2_ns1_advanced_by_diff() {
    // Verifies the advance path: ns 1 holds block 100 and must be advanced
    // to block 101 by applying exactly one state diff.
    //
    // State before migration:
    //   ns 0 = block 102  (102 % 3 = 0)
    //   ns 1 = block 100  (100 % 3 = 1)  ← bootstrap state
    //   ns 2 = block 101  (101 % 3 = 2)
    //
    // Migrate 3→2:
    //   target ns 0 = block 102  (102 % 2 = 0) → already there
    //   target ns 1 = block 101  (101 % 2 = 1) → advance from 100 by diff 101
    //   ns 2 → orphaned
    //
    // Diff 101 contains: balance=1100, nonce=1, slot 2=200.
    // ns 1 base (block 100): balance=1000, nonce=0, slot 1=100.
    // After applying diff 101: balance=1100, nonce=1, slot 1=100, slot 2=200.
    //   (slot 3 does NOT exist — it was added at block 102)
    let (w, tmp) = writer_env(3);
    let a = addr(0xE2);

    w.bootstrap_from_snapshot(
        vec![account_state_with_storage(a, 1000, 0, [(1, 100)])],
        100,
        B256::repeat_byte(0x10),
        B256::repeat_byte(0x20),
    )
    .unwrap();
    w.commit_block(&update_with_storage(101, a, 1100, 1, [(2, 200)]))
        .unwrap();
    w.commit_block(&update_with_storage(102, a, 1200, 2, [(3, 300)]))
        .unwrap();
    drop(w);

    // Migrate 3→2
    let w = reopen_writer(&tmp, 2);
    let result = w.migrate_if_needed().unwrap();
    assert!(matches!(
        result,
        MigrationResult::Completed {
            old_size: 3,
            new_size: 2,
            ..
        }
    ));

    assert_eq!(w.latest_block_number().unwrap(), Some(102));

    // Both blocks available
    assert!(w.is_block_available(102).unwrap());
    assert!(w.is_block_available(101).unwrap());
    assert!(!w.is_block_available(100).unwrap());

    assert_eq!(w.get_available_block_range().unwrap(), Some((101, 102)));

    // --- Verify ns 0 (block 102, no-op path) ---
    let acc_102 = w.get_account(a.into(), 102).unwrap().unwrap();
    assert_eq!(acc_102.balance, u256(1200));
    assert_eq!(acc_102.nonce, 2);
    let s102 = w.get_all_storage(a.into(), 102).unwrap();
    assert_eq!(s102.len(), 3);
    assert_eq!(s102.get(&hash_slot(slot_b256(1))), Some(&u256(100)));
    assert_eq!(s102.get(&hash_slot(slot_b256(2))), Some(&u256(200)));
    assert_eq!(s102.get(&hash_slot(slot_b256(3))), Some(&u256(300)));

    // --- Verify ns 1 (block 101, advanced from 100 by diff 101) ---
    let acc_101 = w.get_account(a.into(), 101).unwrap().unwrap();
    assert_eq!(acc_101.balance, u256(1100));
    assert_eq!(acc_101.nonce, 1);
    let s101 = w.get_all_storage(a.into(), 101).unwrap();
    assert_eq!(s101.len(), 2); // only slots 1 and 2
    assert_eq!(s101.get(&hash_slot(slot_b256(1))), Some(&u256(100)));
    assert_eq!(s101.get(&hash_slot(slot_b256(2))), Some(&u256(200)));
    assert_eq!(s101.get(&hash_slot(slot_b256(3))), None); // NOT here yet

    // Block metadata: 101 and 102 survive, 100 pruned
    assert!(w.get_block_metadata(102).unwrap().is_some());
    assert!(w.get_block_metadata(101).unwrap().is_some());
    assert!(w.get_block_metadata(100).unwrap().is_none());

    // Continue writing — block 103 → 103%2=1 → ns 1 (the advanced one).
    // load_base_batches sees ns 1 at block 101, applies diff 102 as
    // intermediate, then writes 103's changes.
    w.commit_block(&update_with_storage(103, a, 1300, 3, [(4, 400)]))
        .unwrap();
    assert!(w.is_block_available(103).unwrap());
    assert!(w.is_block_available(102).unwrap());
    assert!(!w.is_block_available(101).unwrap());

    let acc_103 = w.get_account(a.into(), 103).unwrap().unwrap();
    assert_eq!(acc_103.balance, u256(1300));
    assert_eq!(acc_103.nonce, 3);
    let s103 = w.get_all_storage(a.into(), 103).unwrap();
    assert_eq!(s103.len(), 4); // slots 1,2,3,4 — cumulative
    assert_eq!(s103.get(&hash_slot(slot_b256(1))), Some(&u256(100)));
    assert_eq!(s103.get(&hash_slot(slot_b256(2))), Some(&u256(200)));
    assert_eq!(s103.get(&hash_slot(slot_b256(3))), Some(&u256(300)));
    assert_eq!(s103.get(&hash_slot(slot_b256(4))), Some(&u256(400)));
}
