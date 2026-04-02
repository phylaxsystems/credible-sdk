use crate::{
    AccountState,
    AddressHash,
    BlockStateUpdate,
    Reader,
    StateReader,
    StateWriter,
    Writer,
};
use alloy::primitives::{
    B256,
    Bytes,
    U256,
};
use reth_db_api::transaction::DbTx;
use tempfile::TempDir;

fn make_account(
    address_hash: AddressHash,
    balance: u64,
    nonce: u64,
    storage: &[(u8, u64)],
) -> AccountState {
    let mut account = AccountState {
        address_hash,
        balance: U256::from(balance),
        nonce,
        code_hash: B256::repeat_byte(0xCC),
        code: Some(Bytes::from_static(&[0x60, 0x80])),
        ..Default::default()
    };

    for (slot, value) in storage {
        account
            .storage
            .insert(B256::repeat_byte(*slot), U256::from(*value));
    }

    account
}

#[test]
fn latest_block_replaces_previous_snapshot() {
    let dir = TempDir::new().unwrap();
    let writer = StateWriter::new(dir.path()).unwrap();
    let address_hash = AddressHash::from_hash(B256::repeat_byte(0xAA));

    writer
        .commit_block(&BlockStateUpdate {
            block_number: 10,
            block_hash: B256::repeat_byte(0x11),
            state_root: B256::repeat_byte(0x12),
            accounts: vec![make_account(address_hash, 100, 1, &[(0x01, 10)])],
        })
        .unwrap();

    writer
        .commit_block(&BlockStateUpdate {
            block_number: 11,
            block_hash: B256::repeat_byte(0x21),
            state_root: B256::repeat_byte(0x22),
            accounts: vec![make_account(address_hash, 200, 2, &[(0x01, 20)])],
        })
        .unwrap();

    let reader = writer.reader();
    assert_eq!(reader.latest_block_number().unwrap(), Some(11));
    assert!(reader.is_block_available(11).unwrap());
    assert!(!reader.is_block_available(10).unwrap());

    let account = reader.get_account(address_hash, 11).unwrap().unwrap();
    assert_eq!(account.balance, U256::from(200));
    assert_eq!(account.nonce, 2);
    assert_eq!(
        reader
            .get_storage(address_hash, B256::repeat_byte(0x01), 11)
            .unwrap(),
        Some(U256::from(20))
    );
}

#[test]
fn bootstrap_replaces_existing_state() {
    let dir = TempDir::new().unwrap();
    let writer = StateWriter::new(dir.path()).unwrap();
    let old_address = AddressHash::from_hash(B256::repeat_byte(0xAA));
    let new_address = AddressHash::from_hash(B256::repeat_byte(0xBB));

    writer
        .commit_block(&BlockStateUpdate {
            block_number: 5,
            block_hash: B256::repeat_byte(0x10),
            state_root: B256::repeat_byte(0x11),
            accounts: vec![make_account(old_address, 1, 1, &[(0x01, 1)])],
        })
        .unwrap();

    writer
        .bootstrap_from_snapshot(
            vec![make_account(new_address, 9, 3, &[(0x02, 7)])],
            100,
            B256::repeat_byte(0x40),
            B256::repeat_byte(0x41),
        )
        .unwrap();

    let reader = writer.reader();
    assert_eq!(reader.latest_block_number().unwrap(), Some(100));
    assert!(reader.get_account(old_address, 100).unwrap().is_none());
    assert_eq!(
        reader
            .get_account(new_address, 100)
            .unwrap()
            .unwrap()
            .balance,
        U256::from(9)
    );
}

#[test]
fn deleting_account_clears_storage() {
    let dir = TempDir::new().unwrap();
    let writer = StateWriter::new(dir.path()).unwrap();
    let address_hash = AddressHash::from_hash(B256::repeat_byte(0xCD));

    writer
        .commit_block(&BlockStateUpdate {
            block_number: 1,
            block_hash: B256::repeat_byte(0x01),
            state_root: B256::repeat_byte(0x02),
            accounts: vec![make_account(address_hash, 10, 1, &[(0x01, 11), (0x02, 22)])],
        })
        .unwrap();

    let deleted = AccountState {
        address_hash,
        deleted: true,
        ..Default::default()
    };
    writer
        .commit_block(&BlockStateUpdate {
            block_number: 2,
            block_hash: B256::repeat_byte(0x03),
            state_root: B256::repeat_byte(0x04),
            accounts: vec![deleted],
        })
        .unwrap();

    let reader = writer.reader();
    assert!(reader.get_account(address_hash, 2).unwrap().is_none());
    assert!(
        reader
            .get_storage(address_hash, B256::repeat_byte(0x01), 2)
            .unwrap()
            .is_none()
    );
    assert!(reader.get_all_storage(address_hash, 2).unwrap().is_empty());
}

#[test]
fn zero_value_storage_slots_are_deleted() {
    let dir = TempDir::new().unwrap();
    let writer = StateWriter::new(dir.path()).unwrap();
    let address_hash = AddressHash::from_hash(B256::repeat_byte(0xEE));

    writer
        .commit_block(&BlockStateUpdate {
            block_number: 1,
            block_hash: B256::repeat_byte(0x10),
            state_root: B256::repeat_byte(0x11),
            accounts: vec![make_account(address_hash, 10, 1, &[(0x01, 33), (0x02, 44)])],
        })
        .unwrap();

    let mut update = make_account(address_hash, 10, 1, &[(0x01, 0)]);
    update.code = None;
    writer
        .commit_block(&BlockStateUpdate {
            block_number: 2,
            block_hash: B256::repeat_byte(0x12),
            state_root: B256::repeat_byte(0x13),
            accounts: vec![update],
        })
        .unwrap();

    let reader = writer.reader();
    assert!(
        reader
            .get_storage(address_hash, B256::repeat_byte(0x01), 2)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        reader
            .get_storage(address_hash, B256::repeat_byte(0x02), 2)
            .unwrap(),
        Some(U256::from(44))
    );
}

#[test]
fn empty_block_preserves_existing_state() {
    let dir = TempDir::new().unwrap();
    let writer = StateWriter::new(dir.path()).unwrap();
    let address_hash = AddressHash::from_hash(B256::repeat_byte(0xAA));

    writer
        .commit_block(&BlockStateUpdate {
            block_number: 7,
            block_hash: B256::repeat_byte(0x21),
            state_root: B256::repeat_byte(0x22),
            accounts: vec![make_account(address_hash, 77, 3, &[(0x01, 55)])],
        })
        .unwrap();

    writer
        .commit_block(&BlockStateUpdate {
            block_number: 8,
            block_hash: B256::repeat_byte(0x23),
            state_root: B256::repeat_byte(0x24),
            accounts: Vec::new(),
        })
        .unwrap();

    let reader = writer.reader();
    assert_eq!(
        reader
            .get_account(address_hash, 8)
            .unwrap()
            .unwrap()
            .balance,
        U256::from(77)
    );
    assert_eq!(
        reader
            .get_storage(address_hash, B256::repeat_byte(0x01), 8)
            .unwrap(),
        Some(U256::from(55))
    );
}

#[test]
fn updating_one_account_preserves_other_accounts() {
    let dir = TempDir::new().unwrap();
    let writer = StateWriter::new(dir.path()).unwrap();
    let account_a = AddressHash::from_hash(B256::repeat_byte(0xA1));
    let account_b = AddressHash::from_hash(B256::repeat_byte(0xB2));

    writer
        .commit_block(&BlockStateUpdate {
            block_number: 1,
            block_hash: B256::repeat_byte(0x31),
            state_root: B256::repeat_byte(0x32),
            accounts: vec![
                make_account(account_a, 10, 1, &[(0x01, 100)]),
                make_account(account_b, 20, 2, &[(0x02, 200)]),
            ],
        })
        .unwrap();

    writer
        .commit_block(&BlockStateUpdate {
            block_number: 2,
            block_hash: B256::repeat_byte(0x33),
            state_root: B256::repeat_byte(0x34),
            accounts: vec![make_account(account_a, 99, 3, &[(0x01, 999)])],
        })
        .unwrap();

    let reader = writer.reader();
    assert_eq!(
        reader.get_account(account_a, 2).unwrap().unwrap().balance,
        U256::from(99)
    );
    assert_eq!(
        reader.get_account(account_b, 2).unwrap().unwrap().balance,
        U256::from(20)
    );
    assert_eq!(
        reader
            .get_storage(account_b, B256::repeat_byte(0x02), 2)
            .unwrap(),
        Some(U256::from(200))
    );
}

#[test]
fn get_all_storage_handles_many_slots() {
    let dir = TempDir::new().unwrap();
    let writer = StateWriter::new(dir.path()).unwrap();
    let address_hash = AddressHash::from_hash(B256::repeat_byte(0xC3));
    let storage: Vec<_> = (0u8..100).map(|slot| (slot, u64::from(slot) + 1)).collect();

    writer
        .commit_block(&BlockStateUpdate {
            block_number: 3,
            block_hash: B256::repeat_byte(0x41),
            state_root: B256::repeat_byte(0x42),
            accounts: vec![make_account(address_hash, 7, 1, &storage)],
        })
        .unwrap();

    let all_storage = writer.reader().get_all_storage(address_hash, 3).unwrap();
    assert_eq!(all_storage.len(), 100);
    for (slot, value) in storage {
        assert_eq!(
            all_storage.get(&B256::repeat_byte(slot)).copied(),
            Some(U256::from(value))
        );
    }
}

#[test]
fn multiple_accounts_and_code_round_trip() {
    let dir = TempDir::new().unwrap();
    let writer = StateWriter::new(dir.path()).unwrap();
    let account_a = AddressHash::from_hash(B256::repeat_byte(0x01));
    let account_b = AddressHash::from_hash(B256::repeat_byte(0x02));
    let shared_code = Bytes::from_static(&[0x60, 0x80, 0x60, 0x40]);
    let code_hash = B256::repeat_byte(0xCC);

    let mut first = make_account(account_a, 1, 1, &[(0x01, 10)]);
    first.code = Some(shared_code.clone());
    first.code_hash = code_hash;

    let mut second = make_account(account_b, 2, 2, &[(0x02, 20)]);
    second.code = Some(shared_code.clone());
    second.code_hash = code_hash;

    writer
        .commit_block(&BlockStateUpdate {
            block_number: 9,
            block_hash: B256::repeat_byte(0x31),
            state_root: B256::repeat_byte(0x32),
            accounts: vec![first, second],
        })
        .unwrap();

    let reader = writer.reader();
    assert_eq!(
        reader.get_account(account_a, 9).unwrap().unwrap().balance,
        U256::from(1)
    );
    assert_eq!(
        reader.get_account(account_b, 9).unwrap().unwrap().balance,
        U256::from(2)
    );
    assert_eq!(reader.get_code(code_hash, 9).unwrap(), Some(shared_code));
}

#[test]
fn get_full_account_and_scan_account_hashes_work_for_latest_block() {
    let dir = TempDir::new().unwrap();
    let writer = StateWriter::new(dir.path()).unwrap();
    let account_a = AddressHash::from_hash(B256::repeat_byte(0x0A));
    let account_b = AddressHash::from_hash(B256::repeat_byte(0x0B));
    let code_hash = B256::repeat_byte(0xCC);
    let code = Bytes::from_static(&[0x60, 0x80, 0x60, 0x40]);

    let mut first = make_account(account_a, 11, 1, &[(0x01, 100), (0x02, 200)]);
    first.code_hash = code_hash;
    first.code = Some(code.clone());
    let mut second = make_account(account_b, 22, 2, &[(0x03, 300)]);
    second.code_hash = B256::repeat_byte(0xDD);
    second.code = None;

    writer
        .commit_block(&BlockStateUpdate {
            block_number: 15,
            block_hash: B256::repeat_byte(0x51),
            state_root: B256::repeat_byte(0x52),
            accounts: vec![first, second],
        })
        .unwrap();

    let reader = writer.reader();
    let full = reader.get_full_account(account_a, 15).unwrap().unwrap();
    assert_eq!(full.balance, U256::from(11));
    assert_eq!(full.nonce, 1);
    assert_eq!(full.code, Some(code));
    assert_eq!(full.storage.len(), 2);

    let hashes = reader.scan_account_hashes(15).unwrap();
    assert_eq!(hashes.len(), 2);
    assert!(hashes.contains(&account_a));
    assert!(hashes.contains(&account_b));
}

#[test]
fn available_block_range_and_stale_block_error_match_latest_only_semantics() {
    let dir = TempDir::new().unwrap();
    let writer = StateWriter::new(dir.path()).unwrap();
    let address = AddressHash::from_hash(B256::repeat_byte(0x22));

    writer
        .commit_block(&BlockStateUpdate {
            block_number: 20,
            block_hash: B256::repeat_byte(0x61),
            state_root: B256::repeat_byte(0x62),
            accounts: vec![make_account(address, 5, 1, &[])],
        })
        .unwrap();

    let reader = writer.reader();
    assert_eq!(reader.get_available_block_range().unwrap(), Some((20, 20)));

    let err = reader.get_account(address, 19).unwrap_err();
    assert!(matches!(
        err,
        crate::common::StateError::BlockNotAvailable {
            block_number: 19,
            latest_block: 20
        }
    ));
}

#[test]
fn begin_bootstrap_setters_finalize_and_fix_block_metadata_work() {
    let dir = TempDir::new().unwrap();
    let writer = StateWriter::new(dir.path()).unwrap();
    let address = AddressHash::from_hash(B256::repeat_byte(0x33));

    let mut bootstrap = writer.begin_bootstrap(0, B256::ZERO, B256::ZERO).unwrap();
    bootstrap
        .write_account(&make_account(address, 42, 7, &[(0x01, 77)]))
        .unwrap();
    bootstrap.set_block_number(30);
    bootstrap.set_metadata(B256::repeat_byte(0x71), B256::repeat_byte(0x72));
    let stats = bootstrap.finalize().unwrap();
    assert_eq!(stats.accounts_written, 1);

    let reader = writer.reader();
    assert_eq!(reader.latest_block_number().unwrap(), Some(30));
    assert_eq!(
        reader.get_block_metadata(30).unwrap().unwrap().block_hash,
        B256::repeat_byte(0x71)
    );

    assert!(
        writer
            .fix_block_metadata(31, B256::repeat_byte(0x81), Some(B256::repeat_byte(0x82)))
            .unwrap()
    );
    assert_eq!(reader.latest_block_number().unwrap(), Some(31));
    let metadata = reader.get_block_metadata(31).unwrap().unwrap();
    assert_eq!(metadata.block_hash, B256::repeat_byte(0x81));
    assert_eq!(metadata.state_root, B256::repeat_byte(0x82));
}

#[test]
fn legacy_layout_requires_resync() {
    let dir = TempDir::new().unwrap();
    {
        let db = crate::db::StateDb::open(dir.path()).unwrap();
        let tx = db.tx_mut().unwrap();
        let address_hash = AddressHash::from_hash(B256::repeat_byte(0xAB));
        let raw_db = tx.inner.open_db(Some("NamespacedAccounts")).unwrap();
        let legacy_key = {
            let mut key = vec![0];
            key.extend_from_slice(address_hash.as_b256().as_slice());
            key
        };
        tx.inner
            .put(
                raw_db.dbi(),
                &legacy_key,
                vec![0u8; 104],
                reth_libmdbx::WriteFlags::empty(),
            )
            .unwrap();
        tx.commit().unwrap();
    }

    let err = StateReader::new(dir.path()).unwrap_err();
    assert!(
        err.to_string().contains("re-sync"),
        "unexpected error: {err:?}"
    );
}
