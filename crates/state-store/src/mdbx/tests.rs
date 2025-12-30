//! Integration tests for MDBX state storage.

#![allow(clippy::cast_lossless)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::many_single_char_names)]

use crate::{
    AccountState,
    AddressHash,
    BlockStateUpdate,
    Reader,
    Writer,
    mdbx::{
        common::CircularBufferConfig,
        reader::StateReader,
        writer::StateWriter,
    },
};
use alloy::primitives::{
    Address,
    B256,
    Bytes,
    U256,
    keccak256,
};
use std::collections::HashMap;
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

fn hash_storage(storage: HashMap<B256, U256>) -> HashMap<B256, U256> {
    storage
        .into_iter()
        .map(|(k, v)| (hash_slot(k), v))
        .collect()
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

    let a1 = Address::repeat_byte(0x11);
    w.commit_block(&test_update(
        0,
        B256::repeat_byte(0xAA),
        a1,
        1000,
        5,
        HashMap::new(),
        None,
    ))
    .unwrap();
    let a2 = Address::repeat_byte(0x22);
    w.commit_block(&test_update(
        1,
        B256::repeat_byte(0xBB),
        a2,
        2000,
        10,
        HashMap::new(),
        None,
    ))
    .unwrap();
    let a3 = Address::repeat_byte(0x33);
    w.commit_block(&test_update(
        2,
        B256::repeat_byte(0xCC),
        a3,
        3000,
        15,
        HashMap::new(),
        None,
    ))
    .unwrap();
    let a4 = Address::repeat_byte(0x44);
    w.commit_block(&test_update(
        3,
        B256::repeat_byte(0xDD),
        a4,
        4000,
        20,
        HashMap::new(),
        None,
    ))
    .unwrap();
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
    let addr = Address::repeat_byte(0x55);

    w.commit_block(&test_update(
        0,
        B256::ZERO,
        addr,
        1000,
        0,
        HashMap::new(),
        None,
    ))
    .unwrap();
    w.commit_block(&test_update(
        1,
        B256::ZERO,
        addr,
        1500,
        1,
        HashMap::new(),
        None,
    ))
    .unwrap();
    w.commit_block(&test_update(
        2,
        B256::ZERO,
        addr,
        1200,
        2,
        HashMap::new(),
        None,
    ))
    .unwrap();
    w.commit_block(&test_update(
        3,
        B256::ZERO,
        addr,
        2000,
        3,
        HashMap::new(),
        None,
    ))
    .unwrap();
    drop(w);

    let r = reader_for(&tmp, 3);
    let acc = r.get_account(addr.into(), 3).unwrap().unwrap();
    assert_eq!(acc.balance, u256(2000));
    assert_eq!(acc.nonce, 3);
}

#[test]
fn test_cumulative_storage_updates() {
    let (w, tmp) = writer_env(3);
    let addr = Address::repeat_byte(0x66);

    w.commit_block(&test_update(
        0,
        B256::ZERO,
        addr,
        1000,
        0,
        HashMap::from([(slot_b256(1), u256(100))]),
        None,
    ))
    .unwrap();
    w.commit_block(&test_update(
        1,
        B256::ZERO,
        addr,
        1000,
        1,
        HashMap::from([(slot_b256(2), u256(200))]),
        None,
    ))
    .unwrap();
    w.commit_block(&test_update(
        2,
        B256::ZERO,
        addr,
        1000,
        2,
        HashMap::from([(slot_b256(1), u256(150))]),
        None,
    ))
    .unwrap();
    w.commit_block(&test_update(
        3,
        B256::ZERO,
        addr,
        1000,
        3,
        HashMap::from([(slot_b256(3), u256(300))]),
        None,
    ))
    .unwrap();
    drop(w);

    let r = reader_for(&tmp, 3);
    let storage = r.get_all_storage(addr.into(), 3).unwrap();
    assert_eq!(storage.len(), 3);
    assert_eq!(storage.get(&hash_slot(slot_b256(1))), Some(&u256(150)));
    assert_eq!(storage.get(&hash_slot(slot_b256(2))), Some(&u256(200)));
    assert_eq!(storage.get(&hash_slot(slot_b256(3))), Some(&u256(300)));
}

#[test]
fn test_single_block() {
    let (w, tmp) = writer_env(3);
    let addr = Address::repeat_byte(0x11);

    assert_eq!(w.latest_block_number().unwrap(), None);
    w.commit_block(&test_update(
        0,
        B256::ZERO,
        addr,
        1000,
        5,
        HashMap::new(),
        None,
    ))
    .unwrap();
    assert_eq!(w.latest_block_number().unwrap(), Some(0));
    drop(w);

    let r = reader_for(&tmp, 3);
    let acc = r.get_account(addr.into(), 0).unwrap().unwrap();
    assert_eq!(acc.balance, u256(1000));
    assert_eq!(acc.nonce, 5);
}

#[test]
fn test_large_scale_rotation() {
    let (w, tmp) = writer_env(5);
    let addr = Address::repeat_byte(0xcc);

    for b in 0..20 {
        w.commit_block(&test_update(
            b,
            B256::ZERO,
            addr,
            b * 10,
            b,
            HashMap::new(),
            None,
        ))
        .unwrap();
    }
    drop(w);

    let r = reader_for(&tmp, 5);
    assert_eq!(r.latest_block_number().unwrap(), Some(19));
    assert_eq!(
        r.get_account(addr.into(), 15).unwrap().unwrap().balance,
        u256(150)
    );
    assert_eq!(
        r.get_account(addr.into(), 19).unwrap().unwrap().balance,
        u256(190)
    );
}

#[test]
fn test_zero_storage_deleted() {
    let (w, tmp) = writer_env(3);
    let addr = Address::repeat_byte(0xaa);

    let s0 = HashMap::from([
        (slot_b256(1), u256(100)),
        (slot_b256(2), u256(200)),
        (slot_b256(3), u256(300)),
    ]);
    w.commit_block(&test_update(0, B256::ZERO, addr, 1000, 0, s0, None))
        .unwrap();
    w.commit_block(&test_update(
        1,
        B256::ZERO,
        addr,
        1000,
        1,
        HashMap::from([(slot_b256(2), U256::ZERO)]),
        None,
    ))
    .unwrap();
    w.commit_block(&test_update(
        2,
        B256::ZERO,
        addr,
        1000,
        2,
        HashMap::from([(slot_b256(1), U256::ZERO), (slot_b256(3), U256::ZERO)]),
        None,
    ))
    .unwrap();
    drop(w);

    let r = reader_for(&tmp, 3);
    let s1 = r.get_all_storage(addr.into(), 1).unwrap();
    assert_eq!(s1.len(), 2);
    assert!(!s1.contains_key(&hash_slot(slot_b256(2))));
    assert_eq!(r.get_all_storage(addr.into(), 2).unwrap().len(), 0);
}

#[test]
fn test_roundtrip_basic() {
    let (w, tmp) = writer_env(3);
    let addr = Address::repeat_byte(0xaa);

    w.commit_block(&test_update(
        0,
        B256::ZERO,
        addr,
        1000,
        5,
        HashMap::new(),
        None,
    ))
    .unwrap();
    drop(w);

    let r = reader_for(&tmp, 3);
    let acc = r.get_full_account(addr.into(), 0).unwrap().unwrap();
    assert_eq!(acc.address_hash, AddressHash(keccak256(addr)));
    assert_eq!(acc.balance, u256(1000));
    assert_eq!(acc.nonce, 5);
    assert!(acc.storage.is_empty());
}

#[test]
fn test_roundtrip_with_storage() {
    let (w, tmp) = writer_env(3);
    let addr = Address::repeat_byte(0xbb);
    let storage = HashMap::from([
        (slot_b256(1), u256(100)),
        (slot_b256(2), u256(200)),
        (slot_b256(3), u256(300)),
    ]);

    w.commit_block(&test_update(0, B256::ZERO, addr, 5000, 10, storage, None))
        .unwrap();
    drop(w);

    let r = reader_for(&tmp, 3);
    let acc = r.get_full_account(addr.into(), 0).unwrap().unwrap();
    assert_eq!(acc.balance, u256(5000));
    assert_eq!(acc.storage.len(), 3);
    assert_eq!(
        r.get_storage(addr.into(), hash_slot(slot_b256(2)), 0)
            .unwrap(),
        Some(u256(200))
    );
}

#[test]
fn test_roundtrip_with_code() {
    let (w, tmp) = writer_env(3);
    let addr = Address::repeat_byte(0xcc);
    let code = vec![0x60, 0x80, 0x60, 0x40, 0x52];

    w.commit_block(&test_update(
        0,
        B256::ZERO,
        addr,
        0,
        1,
        HashMap::new(),
        Some(code.clone()),
    ))
    .unwrap();
    drop(w);

    let r = reader_for(&tmp, 3);
    let acc = r.get_account(addr.into(), 0).unwrap().unwrap();
    assert_eq!(acc.code_hash, keccak256(&code));
    assert_eq!(
        r.get_code(keccak256(&code), 0).unwrap(),
        Some(Bytes::from(code))
    );
}

#[test]
fn test_circular_buffer_rotation() {
    let (w, tmp) = writer_env(3);
    let addr = Address::repeat_byte(0xdd);

    for b in 0..6 {
        w.commit_block(&test_update(
            b,
            B256::ZERO,
            addr,
            b * 100,
            b,
            HashMap::new(),
            None,
        ))
        .unwrap();
    }
    drop(w);

    let r = reader_for(&tmp, 3);
    assert_eq!(r.latest_block_number().unwrap(), Some(5));
    assert!(!r.is_block_available(2).unwrap());
    assert!(r.is_block_available(3).unwrap());
    assert_eq!(
        r.get_account(addr.into(), 3).unwrap().unwrap().balance,
        u256(300)
    );
    assert_eq!(
        r.get_account(addr.into(), 5).unwrap().unwrap().balance,
        u256(500)
    );
    assert!(r.get_account(addr.into(), 0).is_err());
}

#[test]
fn test_cumulative_state_reads() {
    let (w, tmp) = writer_env(3);
    let (a, b, c) = (
        Address::repeat_byte(0xa1),
        Address::repeat_byte(0xb1),
        Address::repeat_byte(0xc1),
    );

    w.commit_block(&test_update(
        0,
        B256::ZERO,
        a,
        1000,
        1,
        HashMap::new(),
        None,
    ))
    .unwrap();
    w.commit_block(&test_update(
        1,
        B256::ZERO,
        b,
        2000,
        2,
        HashMap::new(),
        None,
    ))
    .unwrap();
    w.commit_block(&test_update(
        2,
        B256::ZERO,
        c,
        3000,
        3,
        HashMap::new(),
        None,
    ))
    .unwrap();
    w.commit_block(&BlockStateUpdate {
        block_number: 3,
        block_hash: B256::ZERO,
        state_root: B256::ZERO,
        accounts: vec![AccountState {
            address_hash: AddressHash(keccak256(a)),
            balance: u256(1500),
            nonce: 5,
            code_hash: B256::ZERO,
            code: None,
            storage: HashMap::new(),
            deleted: false,
        }],
    })
    .unwrap();
    drop(w);

    let r = reader_for(&tmp, 3);
    assert_eq!(
        r.get_account(a.into(), 3).unwrap().unwrap().balance,
        u256(1500)
    );
    assert_eq!(
        r.get_account(b.into(), 3).unwrap().unwrap().balance,
        u256(2000)
    );
    assert_eq!(
        r.get_account(c.into(), 3).unwrap().unwrap().balance,
        u256(3000)
    );
}

#[test]
fn test_block_metadata() {
    let (w, tmp) = writer_env(5);
    let addr = Address::repeat_byte(0xee);

    for b in 0..5u64 {
        let hash = B256::repeat_byte((b * 10) as u8);
        let root = B256::repeat_byte((b * 20) as u8);
        w.commit_block(&BlockStateUpdate {
            block_number: b,
            block_hash: hash,
            state_root: root,
            accounts: vec![AccountState {
                address_hash: AddressHash(keccak256(addr)),
                balance: u256(b * 100),
                nonce: b,
                code_hash: B256::ZERO,
                code: None,
                storage: HashMap::new(),
                deleted: false,
            }],
        })
        .unwrap();
    }
    drop(w);

    let r = reader_for(&tmp, 5);
    for b in 0..5u64 {
        let meta = r.get_block_metadata(b).unwrap().unwrap();
        assert_eq!(meta.block_hash, B256::repeat_byte((b * 10) as u8));
        assert_eq!(meta.state_root, B256::repeat_byte((b * 20) as u8));
    }
}

#[test]
fn test_storage_evolution() {
    let (w, tmp) = writer_env(4);
    let addr = Address::repeat_byte(0xff);

    w.commit_block(&test_update(
        0,
        B256::ZERO,
        addr,
        1000,
        0,
        HashMap::from([
            (slot_b256(1), u256(100)),
            (slot_b256(2), u256(200)),
            (slot_b256(3), u256(300)),
        ]),
        None,
    ))
    .unwrap();
    w.commit_block(&test_update(
        1,
        B256::ZERO,
        addr,
        1000,
        1,
        HashMap::from([(slot_b256(1), u256(150)), (slot_b256(4), u256(400))]),
        None,
    ))
    .unwrap();
    w.commit_block(&test_update(
        2,
        B256::ZERO,
        addr,
        1000,
        2,
        HashMap::from([(slot_b256(2), U256::ZERO), (slot_b256(3), u256(350))]),
        None,
    ))
    .unwrap();
    w.commit_block(&test_update(
        3,
        B256::ZERO,
        addr,
        1000,
        3,
        HashMap::from([(slot_b256(5), u256(500))]),
        None,
    ))
    .unwrap();
    drop(w);

    let r = reader_for(&tmp, 4);
    let s = r.get_all_storage(addr.into(), 3).unwrap();
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

    w.commit_block(&BlockStateUpdate {
        block_number: 0,
        block_hash: B256::ZERO,
        state_root: B256::ZERO,
        accounts: (1..=3)
            .map(|i| {
                AccountState {
                    address_hash: AddressHash(keccak256(Address::repeat_byte(i))),
                    balance: u256(u64::from(i) * 1000),
                    nonce: u64::from(i),
                    code_hash: B256::ZERO,
                    code: None,
                    storage: HashMap::new(),
                    deleted: false,
                }
            })
            .collect(),
    })
    .unwrap();
    drop(w);

    let r = reader_for(&tmp, 3);
    for i in 1..=3u8 {
        let acc = r
            .get_account(Address::repeat_byte(i).into(), 0)
            .unwrap()
            .unwrap();
        assert_eq!(acc.balance, u256(u64::from(i) * 1000));
    }
}

#[test]
fn test_available_block_range() {
    let (w, tmp) = writer_env(3);

    for b in 0..6 {
        w.commit_block(&test_update(
            b,
            B256::ZERO,
            Address::repeat_byte(b as u8),
            1000,
            1,
            HashMap::new(),
            None,
        ))
        .unwrap();
    }
    drop(w);

    let r = reader_for(&tmp, 3);
    assert_eq!(r.get_available_block_range().unwrap(), Some((3, 5)));
}

#[test]
fn test_scan_account_hashes() {
    let (w, tmp) = writer_env(3);

    w.commit_block(&BlockStateUpdate {
        block_number: 0,
        block_hash: B256::ZERO,
        state_root: B256::ZERO,
        accounts: (0..5)
            .map(|i| {
                AccountState {
                    address_hash: AddressHash(keccak256(Address::repeat_byte(i))),
                    balance: u256(u64::from(i) * 100),
                    nonce: u64::from(i),
                    code_hash: B256::ZERO,
                    code: None,
                    storage: HashMap::new(),
                    deleted: false,
                }
            })
            .collect(),
    })
    .unwrap();
    drop(w);

    let r = reader_for(&tmp, 3);
    assert_eq!(r.scan_account_hashes(0).unwrap().len(), 5);
}

#[test]
fn test_account_deletion() {
    let (w, tmp) = writer_env(3);
    let addr = Address::repeat_byte(0xaa);

    w.commit_block(&test_update(
        0,
        B256::ZERO,
        addr,
        1000,
        1,
        HashMap::from([(slot_b256(1), u256(100))]),
        None,
    ))
    .unwrap();
    w.commit_block(&BlockStateUpdate {
        block_number: 1,
        block_hash: B256::ZERO,
        state_root: B256::ZERO,
        accounts: vec![AccountState {
            address_hash: AddressHash(keccak256(addr)),
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
    assert!(r.get_account(addr.into(), 1).unwrap().is_none());
    assert!(r.get_all_storage(addr.into(), 1).unwrap().is_empty());
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
    let addr = Address::repeat_byte(0xdd);

    for b in 0..=6 {
        w.commit_block(&test_update(
            b,
            B256::ZERO,
            addr,
            b * 100,
            b,
            HashMap::new(),
            None,
        ))
        .unwrap();
    }
    drop(w);

    let r = reader_for(&tmp, 3);
    for b in 4..=6 {
        let acc = r.get_account(addr.into(), b).unwrap().unwrap();
        assert_eq!(acc.balance, u256(b * 100));
    }
}

#[test]
fn test_namespace_isolation() {
    let (w, tmp) = writer_env(3);

    for i in 0..3u64 {
        w.commit_block(&test_update(
            i,
            B256::ZERO,
            Address::repeat_byte(i as u8),
            1000 * (i + 1),
            1,
            HashMap::new(),
            None,
        ))
        .unwrap();
    }
    drop(w);

    let r = reader_for(&tmp, 3);
    for i in 0..3u64 {
        assert_eq!(
            r.get_account(Address::repeat_byte(i as u8).into(), i)
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

    w.commit_block(&BlockStateUpdate {
        block_number: 0,
        block_hash: B256::ZERO,
        state_root: B256::ZERO,
        accounts: vec![
            AccountState {
                address_hash: AddressHash(keccak256(Address::repeat_byte(0x01))),
                balance: u256(1000),
                nonce: 1,
                code_hash,
                code: Some(Bytes::from(code.clone())),
                storage: HashMap::new(),
                deleted: false,
            },
            AccountState {
                address_hash: AddressHash(keccak256(Address::repeat_byte(0x02))),
                balance: u256(2000),
                nonce: 2,
                code_hash,
                code: Some(Bytes::from(code.clone())),
                storage: HashMap::new(),
                deleted: false,
            },
        ],
    })
    .unwrap();
    drop(w);

    let r = reader_for(&tmp, 3);
    assert_eq!(r.get_code(code_hash, 0).unwrap(), Some(Bytes::from(code)));
    assert_eq!(
        r.get_account(Address::repeat_byte(0x01).into(), 0)
            .unwrap()
            .unwrap()
            .code_hash,
        code_hash
    );
    assert_eq!(
        r.get_account(Address::repeat_byte(0x02).into(), 0)
            .unwrap()
            .unwrap()
            .code_hash,
        code_hash
    );
}

#[test]
fn test_large_storage() {
    let (w, tmp) = writer_env(3);
    let addr = Address::repeat_byte(0xaa);
    // Use i+1 to avoid zero value (zero = delete in Ethereum semantics)
    let storage: HashMap<B256, U256> = (0..100u64).map(|i| (slot_b256(i), u256(i + 1))).collect();

    w.commit_block(&test_update(0, B256::ZERO, addr, 1000, 1, storage, None))
        .unwrap();
    drop(w);

    let r = reader_for(&tmp, 3);
    let all = r.get_all_storage(addr.into(), 0).unwrap();
    assert_eq!(all.len(), 100);
    for i in 0..100u64 {
        assert_eq!(
            r.get_storage(addr.into(), hash_slot(slot_b256(i)), 0)
                .unwrap(),
            Some(u256(i + 1))
        );
    }
}

#[test]
fn test_block_not_found_error() {
    let (w, tmp) = writer_env(3);
    let addr = Address::repeat_byte(0xaa);

    w.commit_block(&test_update(
        0,
        B256::ZERO,
        addr,
        1000,
        1,
        HashMap::new(),
        None,
    ))
    .unwrap();
    drop(w);

    let r = reader_for(&tmp, 3);
    match r.get_account(addr.into(), 100) {
        Err(crate::mdbx::common::error::StateError::BlockNotFound { block_number, .. }) => {
            assert_eq!(block_number, 100);
        }
        _ => panic!("Expected BlockNotFound error"),
    }
}
