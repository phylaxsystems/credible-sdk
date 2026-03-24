//! Integration tests for latest-state MDBX storage.

use crate::{
    AccountState,
    AddressHash,
    BlockStateUpdate,
    Reader,
    StateReader,
    StateWriter,
    Writer,
    common::{
        StateError,
        tables::{
            BlockMetadataTable,
            BlockNumber,
            Bytecode,
            Bytecodes,
            Metadata,
            MetadataKey,
            NamespacedAccounts,
            NamespacedStorage,
        },
        types::{
            BlockMetadata as StoredBlockMetadata,
            GlobalMetadata,
            NamespacedAccountKey,
            NamespacedBytecodeKey,
            NamespacedStorageKey,
            StorageValue,
        },
    },
    db::StateDb,
};
use alloy::primitives::{
    Address,
    B256,
    Bytes,
    U256,
    keccak256,
};
use reth_db_api::transaction::{
    DbTx,
    DbTxMut,
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

fn addr(byte: u8) -> Address {
    Address::repeat_byte(byte)
}

fn simple_update(block: u64, addr: Address, bal: u64, nonce: u64) -> BlockStateUpdate {
    test_update(block, addr, bal, nonce, HashMap::new(), None, false)
}

fn update_with_storage(
    block: u64,
    addr: Address,
    bal: u64,
    nonce: u64,
    slots: impl IntoIterator<Item = (u64, u64)>,
) -> BlockStateUpdate {
    let storage = slots
        .into_iter()
        .map(|(slot, value)| (hash_slot(slot_b256(slot)), u256(value)))
        .collect();
    test_update(block, addr, bal, nonce, storage, None, false)
}

fn delete_update(block: u64, addr: Address) -> BlockStateUpdate {
    test_update(block, addr, 0, 0, HashMap::new(), None, true)
}

fn test_update(
    block: u64,
    addr: Address,
    bal: u64,
    nonce: u64,
    storage: HashMap<B256, U256>,
    code: Option<Vec<u8>>,
    deleted: bool,
) -> BlockStateUpdate {
    let code_hash = code.as_ref().map_or(B256::ZERO, keccak256);
    BlockStateUpdate {
        block_number: block,
        block_hash: B256::repeat_byte(u8::try_from(block).unwrap_or(0xff)),
        state_root: B256::repeat_byte(u8::try_from(block + 1).unwrap_or(0xff)),
        accounts: vec![AccountState {
            address_hash: AddressHash(keccak256(addr)),
            balance: u256(bal),
            nonce,
            code_hash,
            code: code.map(Bytes::from),
            storage,
            deleted,
        }],
    }
}

fn account_state(
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
        storage: slots
            .into_iter()
            .map(|(slot, value)| (hash_slot(slot_b256(slot)), u256(value)))
            .collect(),
        deleted: false,
    }
}

fn account_state_with_code(
    addr: Address,
    bal: u64,
    nonce: u64,
    code: Vec<u8>,
    slots: impl IntoIterator<Item = (u64, u64)>,
) -> AccountState {
    let code_hash = keccak256(&code);
    AccountState {
        address_hash: AddressHash(keccak256(addr)),
        balance: u256(bal),
        nonce,
        code_hash,
        code: Some(Bytes::from(code)),
        storage: slots
            .into_iter()
            .map(|(slot, value)| (hash_slot(slot_b256(slot)), u256(value)))
            .collect(),
        deleted: false,
    }
}

fn writer_env() -> (StateWriter, TempDir) {
    let tmp = TempDir::new().unwrap();
    let writer = StateWriter::new(tmp.path().join("state")).unwrap();
    (writer, tmp)
}

fn reader_for(tmp: &TempDir) -> StateReader {
    StateReader::new(tmp.path().join("state")).unwrap()
}

#[test]
fn test_latest_state_only_keeps_current_head_readable() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state");
    let writer = StateWriter::new(&db_path).unwrap();

    writer
        .commit_block(&simple_update(100, addr(0xAA), 1000, 1))
        .unwrap();
    writer
        .commit_block(&simple_update(101, addr(0xAA), 1100, 2))
        .unwrap();
    drop(writer);

    let reader = StateReader::new(&db_path).unwrap();
    assert!(reader.is_block_available(101).unwrap());
    assert!(!reader.is_block_available(100).unwrap());
    assert_eq!(reader.latest_block_number().unwrap(), Some(101));
}

#[test]
fn test_get_available_block_range_collapses_to_latest_head() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state");
    let writer = StateWriter::new(&db_path).unwrap();

    writer
        .commit_block(&simple_update(10, addr(0xAB), 2000, 1))
        .unwrap();
    writer
        .commit_block(&simple_update(11, addr(0xAB), 2100, 2))
        .unwrap();
    drop(writer);

    let reader = StateReader::new(&db_path).unwrap();
    assert_eq!(reader.get_available_block_range().unwrap(), Some((11, 11)));
}

#[test]
fn test_latest_head_reads_include_prior_state_and_new_storage() {
    let (writer, tmp) = writer_env();
    let account = addr(0x11);

    writer
        .commit_block(&update_with_storage(0, account, 1000, 1, [(1, 100)]))
        .unwrap();
    writer
        .commit_block(&update_with_storage(1, account, 1200, 2, [(2, 200)]))
        .unwrap();
    drop(writer);

    let reader = reader_for(&tmp);
    let account_info = reader.get_account(account.into(), 1).unwrap().unwrap();
    assert_eq!(account_info.balance, u256(1200));
    assert_eq!(account_info.nonce, 2);

    let storage = reader.get_all_storage(account.into(), 1).unwrap();
    assert_eq!(storage.get(&hash_slot(slot_b256(1))), Some(&u256(100)));
    assert_eq!(storage.get(&hash_slot(slot_b256(2))), Some(&u256(200)));
}

#[test]
fn test_historical_reads_are_not_available() {
    let (writer, tmp) = writer_env();
    let account = addr(0x22);

    writer
        .commit_block(&simple_update(5, account, 1000, 1))
        .unwrap();
    writer
        .commit_block(&simple_update(6, account, 1100, 2))
        .unwrap();
    drop(writer);

    let reader = reader_for(&tmp);
    let error = reader.get_account(account.into(), 5).unwrap_err();
    assert!(matches!(
        error,
        StateError::BlockNotFound {
            block_number: 5,
            namespace_idx: 0
        }
    ));
}

#[test]
fn test_deleted_accounts_drop_storage_from_latest_state() {
    let (writer, tmp) = writer_env();
    let account = addr(0x33);

    writer
        .commit_block(&update_with_storage(1, account, 500, 1, [(1, 10), (2, 20)]))
        .unwrap();
    writer.commit_block(&delete_update(2, account)).unwrap();
    drop(writer);

    let reader = reader_for(&tmp);
    assert_eq!(reader.get_account(account.into(), 2).unwrap(), None);
    assert!(
        reader
            .get_all_storage(account.into(), 2)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn test_scan_account_hashes_reads_latest_snapshot_only() {
    let (writer, tmp) = writer_env();
    let first = account_state(addr(0x44), 100, 0, []);
    let second = account_state(addr(0x55), 200, 1, [(1, 7)]);

    writer
        .bootstrap_from_snapshot(
            vec![first.clone(), second.clone()],
            20,
            B256::ZERO,
            B256::ZERO,
        )
        .unwrap();
    drop(writer);

    let reader = reader_for(&tmp);
    let hashes = reader.scan_account_hashes(20).unwrap();
    assert_eq!(hashes.len(), 2);
    assert!(hashes.contains(&first.address_hash));
    assert!(hashes.contains(&second.address_hash));
}

#[test]
fn test_bootstrap_replaces_existing_state_and_sets_latest_metadata() {
    let (writer, tmp) = writer_env();
    let old_account = addr(0x66);
    let new_account = addr(0x77);

    writer
        .commit_block(&simple_update(1, old_account, 300, 1))
        .unwrap();
    writer
        .bootstrap_from_snapshot(
            vec![account_state(new_account, 900, 3, [(9, 90)])],
            10,
            B256::repeat_byte(0xAA),
            B256::repeat_byte(0xBB),
        )
        .unwrap();
    drop(writer);

    let reader = reader_for(&tmp);
    assert_eq!(reader.latest_block_number().unwrap(), Some(10));
    assert_eq!(reader.get_account(old_account.into(), 10).unwrap(), None);
    assert_eq!(
        reader
            .get_account(new_account.into(), 10)
            .unwrap()
            .unwrap()
            .balance,
        u256(900)
    );

    let metadata = reader.get_block_metadata(10).unwrap().unwrap();
    assert_eq!(metadata.block_hash, B256::repeat_byte(0xAA));
    assert_eq!(metadata.state_root, B256::repeat_byte(0xBB));
}

#[test]
fn test_fix_block_metadata_moves_latest_head() {
    let (writer, tmp) = writer_env();
    writer
        .bootstrap_from_snapshot(
            vec![account_state(addr(0x88), 111, 1, [])],
            7,
            B256::repeat_byte(0x01),
            B256::repeat_byte(0x02),
        )
        .unwrap();

    assert!(
        writer
            .fix_block_metadata(9, B256::repeat_byte(0x03), Some(B256::repeat_byte(0x04)))
            .unwrap()
    );
    drop(writer);

    let reader = reader_for(&tmp);
    assert_eq!(reader.latest_block_number().unwrap(), Some(9));
    assert_eq!(reader.get_block_metadata(7).unwrap(), None);
    let metadata = reader.get_block_metadata(9).unwrap().unwrap();
    assert_eq!(metadata.block_hash, B256::repeat_byte(0x03));
    assert_eq!(metadata.state_root, B256::repeat_byte(0x04));
}

#[test]
fn test_duplicate_accounts_are_rejected() {
    let (writer, _tmp) = writer_env();
    let address_hash = AddressHash(keccak256(addr(0x99)));
    let update = BlockStateUpdate {
        block_number: 3,
        block_hash: B256::repeat_byte(0x03),
        state_root: B256::repeat_byte(0x04),
        accounts: vec![
            AccountState {
                address_hash,
                balance: u256(1),
                nonce: 1,
                code_hash: B256::ZERO,
                code: None,
                storage: HashMap::new(),
                deleted: false,
            },
            AccountState {
                address_hash,
                balance: u256(2),
                nonce: 2,
                code_hash: B256::ZERO,
                code: None,
                storage: HashMap::new(),
                deleted: false,
            },
        ],
    };

    let error = writer.commit_block(&update).unwrap_err();
    assert!(matches!(error, StateError::DuplicateAccount(found) if found == address_hash));
}

#[test]
fn test_bootstrap_preserves_bytecode_round_trip() {
    let (writer, tmp) = writer_env();
    let account =
        account_state_with_code(addr(0xA0), 700, 4, vec![0x60, 0x80, 0x60, 0x40], [(1, 5)]);
    let expected_code = account.code.clone().unwrap();

    writer
        .bootstrap_from_snapshot(
            vec![account.clone()],
            12,
            B256::repeat_byte(0x0C),
            B256::ZERO,
        )
        .unwrap();
    drop(writer);

    let reader = reader_for(&tmp);
    let full_account = reader
        .get_full_account(account.address_hash, 12)
        .unwrap()
        .unwrap();
    assert_eq!(full_account.code, Some(expected_code.clone()));
    assert_eq!(
        reader.get_code(account.code_hash, 12).unwrap(),
        Some(expected_code)
    );
}

#[test]
fn test_writer_migrates_legacy_namespace_layout_on_open() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("state");
    let db = StateDb::open(&path).unwrap();
    let address = addr(0xB0);
    let address_hash = AddressHash(keccak256(address));
    let code = Bytes::from(vec![0x60, 0xAA]);
    let code_hash = keccak256(&code);
    let legacy_namespace = 2u8;
    let legacy_block = 5u64;
    let slot_hash = hash_slot(slot_b256(7));

    let tx = db.tx_mut().unwrap();
    tx.put::<NamespacedAccounts>(
        NamespacedAccountKey::new(legacy_namespace, address_hash),
        crate::AccountInfo {
            address_hash,
            balance: u256(500),
            nonce: 9,
            code_hash,
        },
    )
    .unwrap();
    tx.put::<NamespacedStorage>(
        NamespacedStorageKey::new(legacy_namespace, address_hash, slot_hash),
        StorageValue(u256(77)),
    )
    .unwrap();
    tx.put::<Bytecodes>(
        NamespacedBytecodeKey::new(legacy_namespace, code_hash),
        Bytecode(code.clone()),
    )
    .unwrap();
    tx.put::<BlockMetadataTable>(
        BlockNumber(legacy_block),
        StoredBlockMetadata {
            block_hash: B256::repeat_byte(0x55),
            state_root: B256::repeat_byte(0x66),
        },
    )
    .unwrap();
    tx.put::<Metadata>(
        MetadataKey,
        GlobalMetadata {
            latest_block: legacy_block,
            buffer_size: 3,
        },
    )
    .unwrap();
    tx.commit().unwrap();
    drop(db);

    let reader = StateReader::new(&path).unwrap();
    assert_eq!(
        reader
            .get_account(address_hash, legacy_block)
            .unwrap()
            .unwrap()
            .balance,
        u256(500)
    );
    drop(reader);

    let writer = StateWriter::new(&path).unwrap();
    let metadata = writer.get_block_metadata(legacy_block).unwrap().unwrap();
    assert_eq!(metadata.block_hash, B256::repeat_byte(0x55));
    drop(writer);

    let writer = StateWriter::new(&path).unwrap();
    writer
        .commit_block(&BlockStateUpdate {
            block_number: 6,
            block_hash: B256::repeat_byte(0x77),
            state_root: B256::repeat_byte(0x88),
            accounts: vec![AccountState {
                address_hash,
                balance: u256(550),
                nonce: 10,
                code_hash,
                code: None,
                storage: HashMap::from([(hash_slot(slot_b256(8)), u256(88))]),
                deleted: false,
            }],
        })
        .unwrap();
    drop(writer);

    let reader = StateReader::new(&path).unwrap();
    let full_account = reader.get_full_account(address_hash, 6).unwrap().unwrap();
    assert_eq!(full_account.balance, u256(550));
    assert_eq!(full_account.nonce, 10);
    assert_eq!(full_account.code, Some(code));
    assert_eq!(full_account.storage.get(&slot_hash), Some(&u256(77)));
    assert_eq!(
        full_account.storage.get(&hash_slot(slot_b256(8))),
        Some(&u256(88))
    );
}
