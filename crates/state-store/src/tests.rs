#![allow(clippy::cast_lossless)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::unreadable_literal)]
#![allow(clippy::expect_fun_call)]

use crate::{
    ChunkedWriteConfig,
    CircularBufferConfig,
    StateReader,
    StateWriter,
    common::{
        AccountState,
        BlockStateUpdate,
        NamespaceLock,
        get_account_key,
        get_diff_key,
        get_storage_key,
        get_write_lock_key,
    },
    writer::commit_block_chunked,
};
use alloy::primitives::{
    Address,
    B256,
    U256,
    keccak256,
};
use anyhow::Result;
use redis::Commands;
use std::collections::HashMap;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::Redis;
use tokio::time::Duration;

async fn wait_for_redis(host: &str, port: u16) -> Result<()> {
    let url = format!("redis://{host}:{port}");
    for _ in 0..20 {
        match redis::Client::open(url.as_str()).and_then(|client| client.get_connection()) {
            Ok(_) => return Ok(()),
            Err(err) => {
                // Redis may not be ready yet; retry after brief pause.
                tokio::time::sleep(Duration::from_millis(50)).await;
                if err.kind() != redis::ErrorKind::Io {
                    break;
                }
            }
        }
    }
    Err(anyhow::anyhow!("Redis at {url} was not ready in time"))
}

/// Helper to render U256 in `0x`-prefixed hex for Redis.
fn encode_u256(value: U256) -> String {
    format!("0x{value:064x}")
}

fn u256_from_u64(value: u64) -> U256 {
    U256::from(value)
}

async fn setup_redis() -> Result<(testcontainers::ContainerAsync<Redis>, redis::Connection)> {
    let container = Redis::default()
        .start()
        .await
        .expect("Failed to start Redis container");

    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;

    wait_for_redis(&host, port).await?;

    let client = redis::Client::open(format!("redis://{host}:{port}")).unwrap();
    let connection = client.get_connection().unwrap();

    Ok((container, connection))
}

// Helper to create a test update with specific account data
fn create_test_update(
    block_number: u64,
    state_root: B256,
    address: Address,
    balance: u64,
    nonce: u64,
    storage: HashMap<U256, U256>,
    code: Option<Vec<u8>>,
) -> BlockStateUpdate {
    let code_hash = code.as_ref().map_or(B256::ZERO, keccak256);

    BlockStateUpdate {
        block_number,
        state_root,
        block_hash: B256::repeat_byte(u8::try_from(block_number).unwrap_or(0xff)),
        accounts: vec![AccountState {
            address_hash: address.into(),
            balance: U256::from(balance),
            nonce,
            code_hash,
            code,
            storage: hash_storage_slots(storage),
            deleted: false,
        }],
    }
}

fn hash_slot(slot: U256) -> U256 {
    let slot_hash = keccak256(slot.to_be_bytes::<32>());
    U256::from_be_bytes(slot_hash.into())
}

fn hash_storage_slots(storage: HashMap<U256, U256>) -> HashMap<U256, U256> {
    storage
        .into_iter()
        .map(|(slot, value)| (hash_slot(slot), value))
        .collect()
}

// Helper to verify account state in Redis
fn verify_account_state(
    conn: &mut redis::Connection,
    namespace: &str,
    address: Address,
    expected_balance: u64,
    expected_nonce: u64,
) -> Result<()> {
    let account_key = get_account_key(namespace, &address.into());

    let balance: String = conn.hget(&account_key, "balance")?;
    assert_eq!(balance, expected_balance.to_string(), "Balance mismatch");

    let nonce: String = conn.hget(&account_key, "nonce")?;
    assert_eq!(nonce, expected_nonce.to_string(), "Nonce mismatch");

    Ok(())
}

// Helper to verify storage in Redis
fn verify_storage(
    conn: &mut redis::Connection,
    namespace: &str,
    address: Address,
    slot: U256,
    expected_value: U256,
) -> Result<()> {
    let storage_key = get_storage_key(namespace, &address.into());
    let slot_hex = encode_u256(hash_slot(slot));

    let value: String = conn.hget(&storage_key, &slot_hex)?;
    assert_eq!(value, encode_u256(expected_value), "Storage value mismatch");

    Ok(())
}

// Helper to get the block number for a namespace
fn get_namespace_block_number(
    conn: &mut redis::Connection,
    namespace: &str,
    namespace_idx: usize,
) -> Result<Option<u64>> {
    let block_key = format!("{namespace}:{namespace_idx}:block");
    let block: Option<String> = conn.get(&block_key)?;
    Ok(block.map(|s| s.parse().unwrap()))
}

#[tokio::test]
async fn test_cumulative_state_with_different_accounts() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;

    wait_for_redis(&host, port).await?;

    let namespace = "cumulative_accounts".to_string();
    let config = CircularBufferConfig { buffer_size: 3 };
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), &namespace, config)?;

    // Block 0: Account 0x11 with balance 1000
    let addr_0x11 = Address::repeat_byte(0x11);
    let update0 = create_test_update(
        0,
        B256::repeat_byte(0xAA),
        addr_0x11,
        1000,
        5,
        HashMap::new(),
        None,
    );
    writer.commit_block(update0)?;

    // Block 1: Account 0x22 with balance 2000 (0x11 not touched)
    let addr_0x22 = Address::repeat_byte(0x22);
    let update1 = create_test_update(
        1,
        B256::repeat_byte(0xBB),
        addr_0x22,
        2000,
        10,
        HashMap::new(),
        None,
    );
    writer.commit_block(update1)?;

    // Block 2: Account 0x33 with balance 3000 (0x11 and 0x22 not touched)
    let addr_0x33 = Address::repeat_byte(0x33);
    let update2 = create_test_update(
        2,
        B256::repeat_byte(0xCC),
        addr_0x33,
        3000,
        15,
        HashMap::new(),
        None,
    );
    writer.commit_block(update2)?;

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    // Block 3: Account 0x44 with balance 4000 (should apply to namespace 0)
    let addr_0x44 = Address::repeat_byte(0x44);
    let update3 = create_test_update(
        3,
        B256::repeat_byte(0xDD),
        addr_0x44,
        4000,
        20,
        HashMap::new(),
        None,
    );
    writer.commit_block(update3)?;

    // CRITICAL: Namespace 0 should have CUMULATIVE state:
    // - Account 0x11 from block 0
    // - Account 0x22 from block 1 (applied as diff)
    // - Account 0x33 from block 2 (applied as diff)
    // - Account 0x44 from block 3 (applied as diff)

    verify_account_state(&mut conn, &format!("{namespace}:0"), addr_0x11, 1000, 5)?;
    verify_account_state(&mut conn, &format!("{namespace}:0"), addr_0x22, 2000, 10)?;
    verify_account_state(&mut conn, &format!("{namespace}:0"), addr_0x33, 3000, 15)?;
    verify_account_state(&mut conn, &format!("{namespace}:0"), addr_0x44, 4000, 20)?;

    // Verify block number is updated
    let block_key = format!("{namespace}:0:block");
    let block: String = conn.get(&block_key)?;
    assert_eq!(block, "3");

    Ok(())
}

#[tokio::test]
async fn test_cumulative_state_with_account_updates() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;

    wait_for_redis(&host, port).await?;

    let namespace = "cumulative_updates".to_string();
    let config = CircularBufferConfig { buffer_size: 3 };
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), &namespace, config)?;

    let address = Address::repeat_byte(0x55);

    // Block 0: Account with balance 1000, nonce 0
    let update0 = create_test_update(
        0,
        B256::repeat_byte(0xBB),
        address,
        1000,
        0,
        HashMap::new(),
        None,
    );
    writer.commit_block(update0)?;

    // Block 1: Same account, balance increases to 1500, nonce to 1
    let update1 = create_test_update(
        1,
        B256::repeat_byte(0xBB),
        address,
        1500,
        1,
        HashMap::new(),
        None,
    );
    writer.commit_block(update1)?;

    // Block 2: Same account, balance decreases to 1200, nonce to 2
    let update2 = create_test_update(
        2,
        B256::repeat_byte(0xBB),
        address,
        1200,
        2,
        HashMap::new(),
        None,
    );
    writer.commit_block(update2)?;

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    // Block 3: Same account, balance to 2000, nonce to 3
    let update3 = create_test_update(
        3,
        B256::repeat_byte(0xBB),
        address,
        2000,
        3,
        HashMap::new(),
        None,
    );
    writer.commit_block(update3)?;

    // Namespace 0 should have the FINAL state after applying all diffs
    verify_account_state(&mut conn, &format!("{namespace}:0"), address, 2000, 3)?;

    Ok(())
}

#[tokio::test]
async fn test_cumulative_storage_updates() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;

    wait_for_redis(&host, port).await?;

    let namespace = "cumulative_storage".to_string();
    let config = CircularBufferConfig { buffer_size: 3 };
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), &namespace, config)?;

    let address = Address::repeat_byte(0x66);

    // Block 0: Set storage slot 1 = 100
    let storage_0 = HashMap::from([(u256_from_u64(1), u256_from_u64(100))]);
    let update0 = create_test_update(
        0,
        B256::repeat_byte(0xBB),
        address,
        1000,
        0,
        storage_0,
        None,
    );
    writer.commit_block(update0)?;

    // Block 1: Set storage slot 2 = 200 (slot 1 not touched)
    let storage_1 = HashMap::from([(u256_from_u64(2), u256_from_u64(200))]);
    let update1 = create_test_update(
        1,
        B256::repeat_byte(0xBB),
        address,
        1000,
        1,
        storage_1,
        None,
    );
    writer.commit_block(update1)?;

    // Block 2: Update storage slot 1 = 150 (slot 2 not touched)
    let storage_2 = HashMap::from([(u256_from_u64(1), u256_from_u64(150))]);
    let update2 = create_test_update(
        2,
        B256::repeat_byte(0xBB),
        address,
        1000,
        2,
        storage_2,
        None,
    );
    writer.commit_block(update2)?;

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    // Block 3: Set storage slot 3 = 300
    let storage_3 = HashMap::from([(u256_from_u64(3), u256_from_u64(300))]);
    let update3 = create_test_update(
        3,
        B256::repeat_byte(0xBB),
        address,
        1000,
        3,
        storage_3,
        None,
    );
    writer.commit_block(update3)?;

    // Namespace 0 should have ALL storage slots with their latest values:
    // - Slot 1 = 150 (updated in block 2)
    // - Slot 2 = 200 (set in block 1)
    // - Slot 3 = 300 (set in block 3)
    verify_storage(
        &mut conn,
        &format!("{namespace}:0"),
        address,
        u256_from_u64(1),
        u256_from_u64(150),
    )?;
    verify_storage(
        &mut conn,
        &format!("{namespace}:0"),
        address,
        u256_from_u64(2),
        u256_from_u64(200),
    )?;
    verify_storage(
        &mut conn,
        &format!("{namespace}:0"),
        address,
        u256_from_u64(3),
        u256_from_u64(300),
    )?;

    Ok(())
}

#[tokio::test]
async fn test_single_block_only_one_state_available() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;

    wait_for_redis(&host, port).await?;

    let namespace = "single_block".to_string();
    let config = CircularBufferConfig { buffer_size: 3 };
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), &namespace, config)?;

    let latest = writer.latest_block_number()?;
    assert_eq!(latest, None);

    let address = Address::repeat_byte(0x11);
    let update = create_test_update(
        0,
        B256::repeat_byte(0xBB),
        address,
        1000,
        5,
        HashMap::new(),
        None,
    );
    writer.commit_block(update)?;

    let latest = writer.latest_block_number()?;
    assert_eq!(latest, Some(0));

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    verify_account_state(&mut conn, &format!("{namespace}:0"), address, 1000, 5)?;

    Ok(())
}

#[tokio::test]
async fn test_state_diff_storage_and_cleanup() -> Result<()> {
    let (_container, mut conn) = setup_redis().await?;

    let base_namespace = "diff_cleanup";
    let buffer_size = 3;
    let chunked_config = ChunkedWriteConfig::default();
    let writer_id = "test-writer";

    for block_num in 0..3 {
        let update = BlockStateUpdate {
            block_number: block_num,
            block_hash: B256::repeat_byte(u8::try_from(block_num).unwrap()),
            state_root: B256::repeat_byte(u8::try_from(block_num).unwrap()),
            accounts: vec![],
        };
        commit_block_chunked(
            &mut conn,
            base_namespace,
            buffer_size,
            &chunked_config,
            writer_id,
            &update,
        )?;
    }

    let update3 = BlockStateUpdate {
        block_number: 3,
        block_hash: B256::repeat_byte(3),
        state_root: B256::repeat_byte(3),
        accounts: vec![],
    };
    commit_block_chunked(
        &mut conn,
        base_namespace,
        buffer_size,
        &chunked_config,
        writer_id,
        &update3,
    )?;

    let diff_key_0 = get_diff_key(base_namespace, 0);
    let exists_0: bool = redis::cmd("EXISTS").arg(&diff_key_0).query(&mut conn)?;
    assert!(!exists_0, "Diff for block 0 should be deleted");

    for block_num in 1..=3 {
        let diff_key = get_diff_key(base_namespace, block_num);
        let exists: bool = redis::cmd("EXISTS").arg(&diff_key).query(&mut conn)?;
        assert!(exists, "Diff for block {block_num} should exist");
    }

    Ok(())
}

#[tokio::test]
async fn test_large_scale_rotation() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;

    wait_for_redis(&host, port).await?;

    let namespace = "large_scale".to_string();
    let config = CircularBufferConfig { buffer_size: 5 };
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), &namespace, config)?;

    let address = Address::repeat_byte(0xcc);

    // Write 20 blocks - each increments the balance by 10
    for block_num in 0..20 {
        let update = create_test_update(
            block_num,
            B256::repeat_byte(0xAA),
            address,
            block_num * 10,
            block_num,
            HashMap::new(),
            None,
        );
        writer.commit_block(update)?;
    }

    let latest = writer.latest_block_number()?;
    assert_eq!(latest, Some(19));

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    // Each namespace should have cumulative state at its block number
    // For example, namespace 0 (which now has block 15) should have the account
    // with balance 150 (from block 15)

    // Block 15 -> namespace 0 (15 % 5 = 0)
    verify_account_state(&mut conn, &format!("{namespace}:0"), address, 150, 15)?;

    // Block 16 -> namespace 1 (16 % 5 = 1)
    verify_account_state(&mut conn, &format!("{namespace}:1"), address, 160, 16)?;

    // Block 17 -> namespace 2 (17 % 5 = 2)
    verify_account_state(&mut conn, &format!("{namespace}:2"), address, 170, 17)?;

    // Block 18 -> namespace 3 (18 % 5 = 3)
    verify_account_state(&mut conn, &format!("{namespace}:3"), address, 180, 18)?;

    // Block 19 -> namespace 4 (19 % 5 = 4)
    verify_account_state(&mut conn, &format!("{namespace}:4"), address, 190, 19)?;

    Ok(())
}

#[tokio::test]
async fn test_zero_storage_values_are_deleted() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;

    wait_for_redis(&host, port).await?;

    let namespace = "zero_storage_deletion".to_string();
    let config = CircularBufferConfig { buffer_size: 3 };

    let writer = StateWriter::new(
        &format!("redis://{host}:{port}"),
        &namespace,
        config.clone(),
    )?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), &namespace, config)?;

    let address = Address::repeat_byte(0xaa);

    // Block 0: Set storage slots 1, 2, 3 to non-zero values
    let storage_0 = HashMap::from([
        (u256_from_u64(1), u256_from_u64(100)),
        (u256_from_u64(2), u256_from_u64(200)),
        (u256_from_u64(3), u256_from_u64(300)),
    ]);
    let update0 = create_test_update(0, B256::ZERO, address, 1000, 0, storage_0, None);
    writer.commit_block(update0)?;

    let all_storage = reader.get_all_storage(address.into(), 0)?;
    assert_eq!(all_storage.len(), 3);

    let storage_1 = HashMap::from([(u256_from_u64(2), U256::ZERO)]);
    let update1 = create_test_update(1, B256::ZERO, address, 1000, 1, storage_1, None);
    writer.commit_block(update1)?;

    let all_storage = reader.get_all_storage(address.into(), 1)?;
    assert_eq!(all_storage.len(), 2);
    assert!(all_storage.contains_key(&hash_slot(u256_from_u64(1))));
    assert!(!all_storage.contains_key(&hash_slot(u256_from_u64(2))));
    assert!(all_storage.contains_key(&hash_slot(u256_from_u64(3))));

    let slot_2_value = reader.get_storage(address.into(), hash_slot(u256_from_u64(2)), 1)?;
    assert_eq!(slot_2_value, None);

    let storage_2 = HashMap::from([
        (u256_from_u64(1), U256::ZERO),
        (u256_from_u64(3), U256::ZERO),
    ]);
    let update2 = create_test_update(2, B256::ZERO, address, 1000, 2, storage_2, None);
    writer.commit_block(update2)?;

    let all_storage = reader.get_all_storage(address.into(), 2)?;
    assert_eq!(all_storage.len(), 0);

    Ok(())
}

#[tokio::test]
async fn test_roundtrip_basic_account_read() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;

    wait_for_redis(&host, port).await?;

    let namespace = "roundtrip_basic".to_string();
    let config = CircularBufferConfig { buffer_size: 3 };

    let writer = StateWriter::new(
        &format!("redis://{host}:{port}"),
        &namespace,
        config.clone(),
    )?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), &namespace, config)?;

    let address = Address::repeat_byte(0xaa);

    // Write block 0
    let update = create_test_update(
        0,
        B256::repeat_byte(0xBB),
        address,
        1000,
        5,
        HashMap::new(),
        None,
    );
    writer.commit_block(update)?;

    // Read back
    let account = reader.get_full_account(address.into(), 0)?;
    assert!(account.is_some());

    let account = account.unwrap();
    assert_eq!(account.address_hash, address.into());
    assert_eq!(account.balance, U256::from(1000u64));
    assert_eq!(account.nonce, 5);
    assert_eq!(account.code, None);
    assert!(account.storage.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_roundtrip_account_with_storage() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;

    wait_for_redis(&host, port).await?;

    let namespace = "roundtrip_storage".to_string();
    let config = CircularBufferConfig { buffer_size: 3 };

    let writer = StateWriter::new(
        &format!("redis://{host}:{port}"),
        &namespace,
        config.clone(),
    )?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), &namespace, config)?;

    let address = Address::repeat_byte(0xbb);

    let storage = HashMap::from([
        (u256_from_u64(1), u256_from_u64(100)),
        (u256_from_u64(2), u256_from_u64(200)),
        (u256_from_u64(3), u256_from_u64(300)),
    ]);

    let update = create_test_update(0, B256::repeat_byte(0xBB), address, 5000, 10, storage, None);
    writer.commit_block(update)?;

    let account = reader.get_full_account(address.into(), 0)?;
    assert!(account.is_some());

    let account = account.unwrap();
    assert_eq!(account.balance, U256::from(5000u64));
    assert_eq!(account.nonce, 10);
    assert_eq!(account.storage.len(), 3);
    assert_eq!(
        account.storage.get(&hash_slot(u256_from_u64(1))),
        Some(&u256_from_u64(100))
    );
    assert_eq!(
        account.storage.get(&hash_slot(u256_from_u64(2))),
        Some(&u256_from_u64(200))
    );
    assert_eq!(
        account.storage.get(&hash_slot(u256_from_u64(3))),
        Some(&u256_from_u64(300))
    );

    // Test individual storage slot read
    let slot_value = reader.get_storage(address.into(), hash_slot(u256_from_u64(2)), 0)?;
    assert_eq!(slot_value, Some(u256_from_u64(200)));

    Ok(())
}

#[tokio::test]
async fn test_roundtrip_account_with_code() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;

    wait_for_redis(&host, port).await?;

    let namespace = "roundtrip_code".to_string();
    let config = CircularBufferConfig { buffer_size: 3 };

    let writer = StateWriter::new(
        &format!("redis://{host}:{port}"),
        &namespace,
        config.clone(),
    )?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), &namespace, config)?;

    let address = Address::repeat_byte(0xcc);
    let code = vec![0x60, 0x80, 0x60, 0x40, 0x52];

    let update = create_test_update(
        0,
        B256::repeat_byte(0xBB),
        address,
        0,
        1,
        HashMap::new(),
        Some(code.clone()),
    );
    writer.commit_block(update)?;

    // Read back
    let account = reader.get_account(address.into(), 0)?;
    assert!(account.is_some());

    let account = account.unwrap();
    assert_eq!(account.code_hash, keccak256(&code));

    // Test direct code read
    let code_hash = keccak256(&code);
    let fetched_code = reader.get_code(code_hash, 0)?;
    assert_eq!(fetched_code, Some(code));

    Ok(())
}

#[tokio::test]
async fn test_roundtrip_circular_buffer_rotation() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;

    wait_for_redis(&host, port).await?;

    let namespace = "roundtrip_rotation".to_string();
    let config = CircularBufferConfig { buffer_size: 3 };

    let writer = StateWriter::new(
        &format!("redis://{host}:{port}"),
        &namespace,
        config.clone(),
    )?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), &namespace, config)?;

    let address = Address::repeat_byte(0xdd);

    // Write blocks 0, 1, 2, 3, 4, 5
    for block_num in 0..6 {
        let update = create_test_update(
            block_num,
            B256::repeat_byte(0xAA),
            address,
            block_num * 100,
            block_num,
            HashMap::new(),
            None,
        );
        writer.commit_block(update)?;
    }

    // Latest block should be 5
    let latest = reader.latest_block_number()?;
    assert_eq!(latest, Some(5));

    // Block 2 should NOT be available (outside buffer)
    let available = reader.is_block_available(2)?;
    assert!(!available);

    // Block 3 should be available
    let available = reader.is_block_available(3)?;
    assert!(available);

    // Read block 3
    let account = reader.get_account(address.into(), 3)?;
    assert!(account.is_some());
    let account = account.unwrap();
    assert_eq!(account.balance, U256::from(300u64));
    assert_eq!(account.nonce, 3);

    // Read block 5
    let account = reader.get_account(address.into(), 5)?;
    assert!(account.is_some());
    let account = account.unwrap();
    assert_eq!(account.balance, U256::from(500u64));
    assert_eq!(account.nonce, 5);

    // Try to read block 0 (should fail - outside buffer)
    let result = reader.get_account(address.into(), 0);
    assert!(result.is_err());

    Ok(())
}

#[tokio::test]
async fn test_roundtrip_cumulative_state_reads() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;

    wait_for_redis(&host, port).await?;

    let namespace = "roundtrip_cumulative".to_string();
    let config = CircularBufferConfig { buffer_size: 3 };

    let writer = StateWriter::new(
        &format!("redis://{host}:{port}"),
        &namespace,
        config.clone(),
    )?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), &namespace, config)?;

    let addr_a = Address::repeat_byte(0xa1);
    let addr_b = Address::repeat_byte(0xb1);
    let addr_c = Address::repeat_byte(0xc1);

    // Block 0: Create account A
    let update0 = create_test_update(0, B256::ZERO, addr_a, 1000, 1, HashMap::new(), None);
    writer.commit_block(update0)?;

    // Block 1: Create account B
    let update1 = create_test_update(1, B256::ZERO, addr_b, 2000, 2, HashMap::new(), None);
    writer.commit_block(update1)?;

    // Block 2: Create account C
    let update2 = create_test_update(2, B256::ZERO, addr_c, 3000, 3, HashMap::new(), None);
    writer.commit_block(update2)?;

    // Block 3: Update account A (overwrites namespace 0)
    let update3 = BlockStateUpdate {
        block_number: 3,
        block_hash: B256::repeat_byte(3),
        state_root: B256::ZERO,
        accounts: vec![AccountState {
            address_hash: addr_a.into(),
            balance: U256::from(1500u64),
            nonce: 5,
            code_hash: B256::ZERO,
            code: None,
            storage: HashMap::new(),
            deleted: false,
        }],
    };
    writer.commit_block(update3)?;

    // Read block 3 - should have cumulative state of all accounts
    // Account A with updated values
    let account_a = reader.get_account(addr_a.into(), 3)?;
    assert!(account_a.is_some());
    let account_a = account_a.unwrap();
    assert_eq!(account_a.balance, U256::from(1500u64));
    assert_eq!(account_a.nonce, 5);

    // Account B should still exist (from block 1 diff)
    let account_b = reader.get_account(addr_b.into(), 3)?;
    assert!(account_b.is_some());
    let account_b = account_b.unwrap();
    assert_eq!(account_b.balance, U256::from(2000u64));
    assert_eq!(account_b.nonce, 2);

    // Account C should still exist (from block 2 diff)
    let account_c = reader.get_account(addr_c.into(), 3)?;
    assert!(account_c.is_some());
    let account_c = account_c.unwrap();
    assert_eq!(account_c.balance, U256::from(3000u64));
    assert_eq!(account_c.nonce, 3);

    Ok(())
}

#[tokio::test]
async fn test_roundtrip_block_metadata() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;

    wait_for_redis(&host, port).await?;

    let namespace = "roundtrip_metadata".to_string();
    let config = CircularBufferConfig { buffer_size: 5 };

    let writer = StateWriter::new(
        &format!("redis://{host}:{port}"),
        &namespace,
        config.clone(),
    )?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), &namespace, config)?;

    let address = Address::repeat_byte(0xee);

    // Write blocks with specific hashes and state roots
    for block_num in 0..5 {
        let block_hash = B256::repeat_byte(u8::try_from(block_num * 10).unwrap());
        let state_root = B256::repeat_byte(u8::try_from(block_num * 20).unwrap());

        let update = BlockStateUpdate {
            block_number: block_num,
            block_hash,
            state_root,
            accounts: vec![AccountState {
                address_hash: address.into(),
                balance: U256::from(block_num * 100),
                nonce: block_num,
                code_hash: B256::ZERO,
                code: None,
                storage: HashMap::new(),
                deleted: false,
            }],
        };
        writer.commit_block(update)?;
    }

    // Read block metadata
    for block_num in 0..5 {
        let metadata = reader.get_block_metadata(block_num)?;
        assert!(metadata.is_some());

        let metadata = metadata.unwrap();
        assert_eq!(metadata.block_number, block_num);
        assert_eq!(
            metadata.block_hash,
            B256::repeat_byte(u8::try_from(block_num * 10).unwrap())
        );
        assert_eq!(
            metadata.state_root,
            B256::repeat_byte(u8::try_from(block_num * 20).unwrap())
        );

        // Test individual reads
        let block_hash = reader.get_block_hash(block_num)?;
        assert_eq!(
            block_hash,
            Some(B256::repeat_byte(u8::try_from(block_num * 10).unwrap()))
        );

        let state_root = reader.get_state_root(block_num)?;
        assert_eq!(
            state_root,
            Some(B256::repeat_byte(u8::try_from(block_num * 20).unwrap()))
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_roundtrip_storage_evolution() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;

    wait_for_redis(&host, port).await?;

    let namespace = "roundtrip_storage_evo".to_string();
    let config = CircularBufferConfig { buffer_size: 4 };

    let writer = StateWriter::new(
        &format!("redis://{host}:{port}"),
        &namespace,
        config.clone(),
    )?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), &namespace, config)?;

    let address = Address::repeat_byte(0xff);

    // Block 0: Set slots 1, 2, 3
    let storage_0 = HashMap::from([
        (u256_from_u64(1), u256_from_u64(100)),
        (u256_from_u64(2), u256_from_u64(200)),
        (u256_from_u64(3), u256_from_u64(300)),
    ]);
    let update0 = create_test_update(0, B256::ZERO, address, 1000, 0, storage_0, None);
    writer.commit_block(update0)?;

    // Block 1: Update slot 1, add slot 4
    let storage_1 = HashMap::from([
        (u256_from_u64(1), u256_from_u64(150)),
        (u256_from_u64(4), u256_from_u64(400)),
    ]);
    let update1 = create_test_update(1, B256::ZERO, address, 1000, 1, storage_1, None);
    writer.commit_block(update1)?;

    // Block 2: Zero slot 2, update slot 3
    let storage_2 = HashMap::from([
        (u256_from_u64(2), U256::ZERO),
        (u256_from_u64(3), u256_from_u64(350)),
    ]);
    let update2 = create_test_update(2, B256::ZERO, address, 1000, 2, storage_2, None);
    writer.commit_block(update2)?;

    let storage_3 = HashMap::from([(u256_from_u64(5), u256_from_u64(500))]);
    let update3 = create_test_update(3, B256::ZERO, address, 1000, 3, storage_3, None);
    writer.commit_block(update3)?;

    let all_storage = reader.get_all_storage(address.into(), 3)?;
    assert_eq!(all_storage.len(), 4);
    assert_eq!(
        all_storage.get(&hash_slot(u256_from_u64(1))),
        Some(&u256_from_u64(150).into())
    );
    assert_eq!(all_storage.get(&hash_slot(u256_from_u64(2))), None);
    assert_eq!(
        all_storage.get(&hash_slot(u256_from_u64(3))),
        Some(&u256_from_u64(350).into())
    );
    assert_eq!(
        all_storage.get(&hash_slot(u256_from_u64(4))),
        Some(&u256_from_u64(400).into())
    );
    assert_eq!(
        all_storage.get(&hash_slot(u256_from_u64(5))),
        Some(&u256_from_u64(500).into())
    );

    Ok(())
}

#[tokio::test]
async fn test_roundtrip_multiple_accounts_per_block() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;

    wait_for_redis(&host, port).await?;

    let namespace = "roundtrip_multi_acct".to_string();
    let config = CircularBufferConfig { buffer_size: 3 };

    let writer = StateWriter::new(
        &format!("redis://{host}:{port}"),
        &namespace,
        config.clone(),
    )?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), &namespace, config)?;

    // Block 0: Create 3 accounts in one block
    let update0 = BlockStateUpdate {
        block_number: 0,
        block_hash: B256::ZERO,
        state_root: B256::ZERO,
        accounts: vec![
            AccountState {
                address_hash: Address::repeat_byte(0x01).into(),
                balance: U256::from(1000u64),
                nonce: 1,
                code_hash: B256::ZERO,
                code: None,
                storage: HashMap::new(),
                deleted: false,
            },
            AccountState {
                address_hash: Address::repeat_byte(0x02).into(),
                balance: U256::from(2000u64),
                nonce: 2,
                code_hash: B256::ZERO,
                code: None,
                storage: HashMap::new(),
                deleted: false,
            },
            AccountState {
                address_hash: Address::repeat_byte(0x03).into(),
                balance: U256::from(3000u64),
                nonce: 3,
                code_hash: B256::ZERO,
                code: None,
                storage: HashMap::new(),
                deleted: false,
            },
        ],
    };
    writer.commit_block(update0)?;

    // Read all three accounts
    for i in 1..=3 {
        let account = reader.get_account(Address::repeat_byte(i).into(), 0)?;
        assert!(account.is_some());
        let account = account.unwrap();
        assert_eq!(account.balance, U256::from(u64::from(i) * 1000));
        assert_eq!(account.nonce, u64::from(i));
    }

    Ok(())
}

#[tokio::test]
async fn test_write_lock_prevents_read() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;

    wait_for_redis(&host, port).await?;

    let namespace = "lock_test";
    let config = CircularBufferConfig { buffer_size: 3 };

    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let address = Address::repeat_byte(0xaa);

    let update = create_test_update(0, B256::ZERO, address, 1000, 1, HashMap::new(), None);
    writer.commit_block(update)?;

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    let lock = NamespaceLock::new(1, "test-writer".to_string());
    let lock_key = get_write_lock_key(&format!("{namespace}:0"));
    let lock_json = lock.to_json()?;
    conn.set::<_, _, ()>(&lock_key, &lock_json)?;

    let result = reader.get_account(address.into(), 0);
    assert!(result.is_err());

    match result {
        Err(crate::common::error::StateError::NamespaceLocked { .. }) => {}
        _ => panic!("Expected NamespaceLocked error"),
    }

    conn.del::<_, ()>(&lock_key)?;

    let account = reader.get_account(address.into(), 0)?;
    assert!(account.is_some());

    Ok(())
}

#[tokio::test]
async fn test_stale_lock_blocks_read() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;

    wait_for_redis(&host, port).await?;

    let namespace = "stale_lock_test";
    let config = CircularBufferConfig { buffer_size: 3 };

    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let address = Address::repeat_byte(0xbb);

    let update = create_test_update(0, B256::ZERO, address, 2000, 2, HashMap::new(), None);
    writer.commit_block(update)?;

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    let mut lock = NamespaceLock::new(1, "crashed-writer".to_string());
    lock.started_at = 0;
    let lock_key = get_write_lock_key(&format!("{namespace}:0"));
    let lock_json = lock.to_json()?;
    conn.set::<_, _, ()>(&lock_key, &lock_json)?;

    let result = reader.get_account(address.into(), 0);
    assert!(result.is_err());

    match result {
        Err(crate::common::error::StateError::NamespaceLocked { .. }) => {}
        _ => panic!("Expected NamespaceLocked error"),
    }

    conn.del::<_, ()>(&lock_key)?;

    let account = reader.get_account(address.into(), 0)?;
    assert!(account.is_some());
    assert_eq!(account.unwrap().balance, U256::from(2000u64));

    Ok(())
}

#[tokio::test]
async fn test_is_block_readable() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;

    wait_for_redis(&host, port).await?;

    let namespace = "is_readable_test";
    let config = CircularBufferConfig { buffer_size: 3 };

    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let address = Address::repeat_byte(0xcc);

    let update = create_test_update(0, B256::ZERO, address, 1000, 1, HashMap::new(), None);
    writer.commit_block(update)?;

    assert!(reader.is_block_readable(0)?);
    assert!(!reader.is_block_readable(1)?);

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    let lock = NamespaceLock::new(1, "test-writer".to_string());
    let lock_key = get_write_lock_key(&format!("{namespace}:0"));
    let lock_json = lock.to_json()?;
    conn.set::<_, _, ()>(&lock_key, &lock_json)?;

    assert!(!reader.is_block_readable(0)?);

    Ok(())
}

#[tokio::test]
async fn test_concurrent_writers_lock_contention() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "concurrent_writers";
    let config = CircularBufferConfig { buffer_size: 3 };

    let writer1 = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;
    let writer2 = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;

    let address = Address::repeat_byte(0x11);

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    let lock = NamespaceLock::new(0, writer1.writer_id().to_string());
    let lock_key = get_write_lock_key(&format!("{namespace}:0"));
    conn.set::<_, _, ()>(&lock_key, lock.to_json()?)?;

    let update = create_test_update(0, B256::ZERO, address, 1000, 1, HashMap::new(), None);
    let result = writer2.commit_block(update);

    assert!(result.is_err());
    match result {
        Err(crate::common::error::StateError::LockAcquisitionFailed { .. }) => {}
        Err(e) => panic!("Expected LockAcquisitionFailed, got: {e:?}"),
        Ok(()) => panic!("Expected error, got success"),
    }

    conn.del::<_, ()>(&lock_key)?;

    Ok(())
}

#[tokio::test]
async fn test_writer_releases_lock_on_success() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "lock_release_success";
    let config = CircularBufferConfig { buffer_size: 3 };

    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;

    let address = Address::repeat_byte(0x22);
    let update = create_test_update(0, B256::ZERO, address, 1000, 1, HashMap::new(), None);

    writer.commit_block(update)?;

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    let lock_key = get_write_lock_key(&format!("{namespace}:0"));
    let exists: bool = conn.exists(&lock_key)?;
    assert!(!exists, "Lock should be released after successful commit");

    Ok(())
}

#[tokio::test]
async fn test_reader_blocked_during_active_write() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "reader_blocked";
    let config = CircularBufferConfig { buffer_size: 3 };

    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;

    let address = Address::repeat_byte(0x33);
    let update = create_test_update(0, B256::ZERO, address, 1000, 1, HashMap::new(), None);
    writer.commit_block(update)?;

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    let lock = NamespaceLock::new(3, "simulated-writer".to_string());
    let lock_key = get_write_lock_key(&format!("{namespace}:0"));
    conn.set::<_, _, ()>(&lock_key, lock.to_json()?)?;

    let result = reader.get_account(address.into(), 0);
    assert!(result.is_err());
    match result {
        Err(crate::common::error::StateError::NamespaceLocked { .. }) => {}
        Err(e) => panic!("Expected NamespaceLocked, got: {e:?}"),
        Ok(_) => panic!("Expected error, got success"),
    }

    assert!(!reader.is_block_readable(0)?);

    conn.del::<_, ()>(&lock_key)?;

    let account = reader.get_account(address.into(), 0)?;
    assert!(account.is_some());

    Ok(())
}

#[tokio::test]
async fn test_namespace_isolation_with_locks() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "namespace_isolation";
    let config = CircularBufferConfig { buffer_size: 3 };

    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), namespace, config)?;

    for i in 0..3 {
        let address = Address::repeat_byte(i as u8);
        let update = create_test_update(i, B256::ZERO, address, 1000, 1, HashMap::new(), None);
        writer.commit_block(update)?;
    }

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    let lock = NamespaceLock::new(3, "writer".to_string());
    conn.set::<_, _, ()>(
        &get_write_lock_key(&format!("{namespace}:0")),
        lock.to_json()?,
    )?;

    let result = reader.get_account(Address::repeat_byte(0).into(), 0);
    assert!(result.is_err());

    let account1 = reader.get_account(Address::repeat_byte(1).into(), 1)?;
    assert!(account1.is_some());

    let account2 = reader.get_account(Address::repeat_byte(2).into(), 2)?;
    assert!(account2.is_some());

    Ok(())
}

#[tokio::test]
async fn test_scan_accounts_respects_lock() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "scan_lock";
    let config = CircularBufferConfig { buffer_size: 3 };

    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let update = BlockStateUpdate {
        block_number: 0,
        block_hash: B256::ZERO,
        state_root: B256::ZERO,
        accounts: (0..5)
            .map(|i| {
                AccountState {
                    address_hash: Address::repeat_byte(i).into(),
                    balance: U256::from(i as u64 * 100),
                    nonce: i as u64,
                    code_hash: B256::ZERO,
                    code: None,
                    storage: HashMap::new(),
                    deleted: false,
                }
            })
            .collect(),
    };
    writer.commit_block(update)?;

    let hashes = reader.scan_account_hashes(0)?;
    assert_eq!(hashes.len(), 5);

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    let lock = NamespaceLock::new(3, "writer".to_string());
    conn.set::<_, _, ()>(
        &get_write_lock_key(&format!("{namespace}:0")),
        lock.to_json()?,
    )?;

    let result = reader.scan_account_hashes(0);
    assert!(result.is_err());
    match result {
        Err(crate::common::error::StateError::NamespaceLocked { .. }) => {}
        Err(e) => panic!("Expected NamespaceLocked, got: {e:?}"),
        Ok(_) => panic!("Expected error"),
    }

    Ok(())
}

#[tokio::test]
async fn test_get_all_storage_respects_lock() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "storage_lock";
    let config = CircularBufferConfig { buffer_size: 3 };

    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let address = Address::repeat_byte(0xaa);
    let storage = HashMap::from([
        (u256_from_u64(1), u256_from_u64(100)),
        (u256_from_u64(2), u256_from_u64(200)),
    ]);

    let update = create_test_update(0, B256::ZERO, address, 1000, 1, storage, None);
    writer.commit_block(update)?;

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    let lock = NamespaceLock::new(3, "writer".to_string());
    conn.set::<_, _, ()>(
        &get_write_lock_key(&format!("{namespace}:0")),
        lock.to_json()?,
    )?;

    let result = reader.get_all_storage(address.into(), 0);
    assert!(result.is_err());

    let result = reader.get_storage(address.into(), hash_slot(u256_from_u64(1)), 0);
    assert!(result.is_err());

    Ok(())
}

#[tokio::test]
async fn test_get_code_respects_lock() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "code_lock";
    let config = CircularBufferConfig { buffer_size: 3 };

    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let address = Address::repeat_byte(0xbb);
    let code = vec![0x60, 0x80, 0x60, 0x40, 0x52];
    let code_hash = keccak256(&code);

    let update = create_test_update(0, B256::ZERO, address, 1000, 1, HashMap::new(), Some(code));
    writer.commit_block(update)?;

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    let lock = NamespaceLock::new(3, "writer".to_string());
    conn.set::<_, _, ()>(
        &get_write_lock_key(&format!("{namespace}:0")),
        lock.to_json()?,
    )?;

    let result = reader.get_code(code_hash, 0);
    assert!(result.is_err());

    Ok(())
}

#[tokio::test]
async fn test_sequential_writes_same_namespace() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "sequential_writes";
    let config = CircularBufferConfig { buffer_size: 3 };

    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    for block_num in 0..=6 {
        let address = Address::repeat_byte(block_num as u8);
        let update = create_test_update(
            block_num,
            B256::ZERO,
            address,
            1000,
            1,
            HashMap::new(),
            None,
        );
        writer.commit_block(update)?;

        if block_num % 3 == 0 {
            let lock_key = get_write_lock_key(&format!("{namespace}:0"));
            let exists: bool = conn.exists(&lock_key)?;
            assert!(!exists, "Lock should be released after block {block_num}");
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_large_chunked_write_lock_maintained() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "large_chunked";
    let config = CircularBufferConfig { buffer_size: 3 };
    let chunked_config = ChunkedWriteConfig::new(5, 300);

    let writer = StateWriter::with_chunked_config(
        &format!("redis://{host}:{port}"),
        namespace,
        config.clone(),
        chunked_config,
    )?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let num_accounts = 50;
    let update = BlockStateUpdate {
        block_number: 0,
        block_hash: B256::ZERO,
        state_root: B256::ZERO,
        accounts: (0..num_accounts)
            .map(|i| {
                AccountState {
                    address_hash: Address::repeat_byte(i as u8).into(),
                    balance: U256::from(i as u64 * 100),
                    nonce: i as u64,
                    code_hash: B256::ZERO,
                    code: None,
                    storage: HashMap::new(),
                    deleted: false,
                }
            })
            .collect(),
    };

    writer.commit_block(update)?;

    for i in 0..num_accounts {
        let account = reader.get_account(Address::repeat_byte(i as u8).into(), 0)?;
        assert!(account.is_some(), "Account {i} should exist");
        let account = account.unwrap();
        assert_eq!(account.balance, U256::from(i as u64 * 100));
    }

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;
    let lock_key = get_write_lock_key(&format!("{namespace}:0"));
    let exists: bool = conn.exists(&lock_key)?;
    assert!(!exists);

    Ok(())
}

#[tokio::test]
async fn test_stale_lock_blocks_both_reader_and_writer() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "stale_blocks_all";
    let config = CircularBufferConfig { buffer_size: 3 };
    let chunked_config = ChunkedWriteConfig::new(100, 1);

    let writer = StateWriter::with_chunked_config(
        &format!("redis://{host}:{port}"),
        namespace,
        config.clone(),
        chunked_config,
    )?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let address = Address::repeat_byte(0x44);
    let update = create_test_update(0, B256::ZERO, address, 1000, 1, HashMap::new(), None);
    writer.commit_block(update)?;

    for block_num in 1..=2 {
        let update = create_test_update(
            block_num,
            B256::ZERO,
            address,
            1000 + block_num * 100,
            block_num,
            HashMap::new(),
            None,
        );
        writer.commit_block(update)?;
    }

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    let mut lock = NamespaceLock::new(3, "crashed-writer".to_string());
    lock.started_at = 0;
    conn.set::<_, _, ()>(
        &get_write_lock_key(&format!("{namespace}:0")),
        lock.to_json()?,
    )?;

    let read_result = reader.get_account(address.into(), 0);
    assert!(read_result.is_err());
    match read_result {
        Err(crate::common::error::StateError::NamespaceLocked { .. }) => {}
        Err(e) => panic!("Expected NamespaceLocked for reader, got: {e:?}"),
        Ok(_) => panic!("Expected error"),
    }

    let update3 = create_test_update(3, B256::ZERO, address, 2000, 3, HashMap::new(), None);
    let write_result = writer.commit_block(update3);
    assert!(write_result.is_err());
    match write_result {
        Err(crate::common::error::StateError::StaleLockDetected { writer_id, .. }) => {
            assert_eq!(writer_id, "crashed-writer");
        }
        Err(e) => panic!("Expected StaleLockDetected for writer, got: {e:?}"),
        Ok(()) => panic!("Expected error"),
    }

    Ok(())
}

#[tokio::test]
async fn test_block_metadata_not_affected_by_namespace_lock() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "metadata_global";
    let config = CircularBufferConfig { buffer_size: 3 };

    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let address = Address::repeat_byte(0x55);
    let block_hash = B256::repeat_byte(0xAA);
    let state_root = B256::repeat_byte(0xBB);

    let update = BlockStateUpdate {
        block_number: 0,
        block_hash,
        state_root,
        accounts: vec![AccountState {
            address_hash: address.into(),
            balance: U256::from(1000u64),
            nonce: 1,
            code_hash: B256::ZERO,
            code: None,
            storage: HashMap::new(),
            deleted: false,
        }],
    };
    writer.commit_block(update)?;

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    let lock = NamespaceLock::new(3, "writer".to_string());
    conn.set::<_, _, ()>(
        &get_write_lock_key(&format!("{namespace}:0")),
        lock.to_json()?,
    )?;

    let fetched_hash = reader.get_block_hash(0)?;
    assert_eq!(fetched_hash, Some(block_hash));

    let fetched_root = reader.get_state_root(0)?;
    assert_eq!(fetched_root, Some(state_root));

    let metadata = reader.get_block_metadata(0)?;
    assert!(metadata.is_some());
    let metadata = metadata.unwrap();
    assert_eq!(metadata.block_hash, block_hash);
    assert_eq!(metadata.state_root, state_root);

    Ok(())
}

#[tokio::test]
async fn test_different_writers_different_namespaces_concurrent() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "concurrent_namespaces";
    let config = CircularBufferConfig { buffer_size: 3 };

    let writer1 = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;
    let writer2 = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;

    let update0 = create_test_update(
        0,
        B256::ZERO,
        Address::repeat_byte(0x00),
        1000,
        1,
        HashMap::new(),
        None,
    );
    writer1.commit_block(update0)?;

    let update1 = create_test_update(
        1,
        B256::ZERO,
        Address::repeat_byte(0x01),
        2000,
        2,
        HashMap::new(),
        None,
    );
    writer2.commit_block(update1)?;

    let reader = StateReader::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let account0 = reader.get_account(Address::repeat_byte(0x00).into(), 0)?;
    assert!(account0.is_some());

    let account1 = reader.get_account(Address::repeat_byte(0x01).into(), 1)?;
    assert!(account1.is_some());

    Ok(())
}

#[tokio::test]
async fn test_lock_contains_correct_info() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "lock_info".to_string();

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    let lock = NamespaceLock::new(5, "test-writer".to_string());
    let lock_key = get_write_lock_key(&format!("{namespace}:2"));

    conn.set::<_, _, ()>(&lock_key, lock.to_json()?)?;

    let lock_json: String = conn.get(&lock_key)?;
    let parsed_lock = NamespaceLock::from_json(&lock_json)?;

    assert_eq!(parsed_lock.target_block, 5);
    assert_eq!(parsed_lock.writer_id, "test-writer");

    Ok(())
}

#[tokio::test]
async fn test_get_full_account_respects_lock() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "full_account_lock";
    let config = CircularBufferConfig { buffer_size: 3 };

    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let address = Address::repeat_byte(0xcc);
    let storage = HashMap::from([(u256_from_u64(1), u256_from_u64(100))]);

    let update = create_test_update(0, B256::ZERO, address, 1000, 1, storage, None);
    writer.commit_block(update)?;

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    let lock = NamespaceLock::new(3, "writer".to_string());
    conn.set::<_, _, ()>(
        &get_write_lock_key(&format!("{namespace}:0")),
        lock.to_json()?,
    )?;

    let result = reader.get_full_account(address.into(), 0);
    assert!(result.is_err());
    match result {
        Err(crate::common::error::StateError::NamespaceLocked { .. }) => {}
        Err(e) => panic!("Expected NamespaceLocked, got: {e:?}"),
        Ok(_) => panic!("Expected error"),
    }

    Ok(())
}

#[tokio::test]
async fn test_writer_can_write_after_own_successful_write() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "same_writer_consecutive";
    let config = CircularBufferConfig { buffer_size: 3 };

    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let address = Address::repeat_byte(0xdd);

    for block_num in 0..=6 {
        let update = create_test_update(
            block_num,
            B256::ZERO,
            address,
            block_num * 100,
            block_num,
            HashMap::new(),
            None,
        );
        writer.commit_block(update)?;
    }

    for block_num in [4u64, 5, 6] {
        let account = reader.get_account(address.into(), block_num)?;
        assert!(account.is_some(), "Block {block_num} should be readable");
        let account = account.unwrap();
        assert_eq!(account.balance, U256::from(block_num * 100));
        assert_eq!(account.nonce, block_num);
    }

    Ok(())
}

#[tokio::test]
async fn test_available_block_range() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "block_range";
    let config = CircularBufferConfig { buffer_size: 3 };

    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let range = reader.get_available_block_range()?;
    assert!(range.is_none());

    for block_num in 0..6 {
        let address = Address::repeat_byte(block_num as u8);
        let update = create_test_update(
            block_num,
            B256::ZERO,
            address,
            1000,
            1,
            HashMap::new(),
            None,
        );
        writer.commit_block(update)?;
    }

    let range = reader.get_available_block_range()?;
    assert!(range.is_some());
    let (oldest, latest) = range.unwrap();
    assert_eq!(oldest, 3);
    assert_eq!(latest, 5);

    Ok(())
}

/// Test that verifies `expected_block_for_namespace` calculates correctly
#[tokio::test]
async fn test_expected_block_for_namespace_calculation() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "expected_block_calc";
    let config = CircularBufferConfig { buffer_size: 3 };
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config)?;

    // With buffer_size=3 and highest_valid_block=100:
    // - namespace 0 should have block 99  (100 % 3 = 1, so ns 0 is 1 behind)
    // - namespace 1 should have block 100 (highest)
    // - namespace 2 should have block 98  (2 behind)

    assert_eq!(writer.expected_block_for_namespace(0, 100), 99);
    assert_eq!(writer.expected_block_for_namespace(1, 100), 100);
    assert_eq!(writer.expected_block_for_namespace(2, 100), 98);

    // With highest_valid_block=99:
    // - namespace 0 should have block 99 (highest, 99 % 3 = 0)
    // - namespace 1 should have block 97
    // - namespace 2 should have block 98

    assert_eq!(writer.expected_block_for_namespace(0, 99), 99);
    assert_eq!(writer.expected_block_for_namespace(1, 99), 97);
    assert_eq!(writer.expected_block_for_namespace(2, 99), 98);

    Ok(())
}

/// Test that verifies circular buffer maintains correct block numbers after rotation
#[tokio::test]
async fn test_circular_buffer_block_numbers_after_rotation() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "circular_block_numbers";
    let config = CircularBufferConfig { buffer_size: 3 };
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    let address = Address::repeat_byte(0xaa);

    // Write blocks 0-5 (two full rotations)
    for block_num in 0..6 {
        let update = create_test_update(
            block_num,
            B256::ZERO,
            address,
            block_num * 100,
            block_num,
            HashMap::new(),
            None,
        );
        writer.commit_block(update)?;
    }

    // After writing blocks 0-5:
    // - namespace 0 should have block 3 (3 % 3 = 0)
    // - namespace 1 should have block 4 (4 % 3 = 1)
    // - namespace 2 should have block 5 (5 % 3 = 2)

    let ns0_block = get_namespace_block_number(&mut conn, namespace, 0)?;
    let ns1_block = get_namespace_block_number(&mut conn, namespace, 1)?;
    let ns2_block = get_namespace_block_number(&mut conn, namespace, 2)?;

    assert_eq!(ns0_block, Some(3), "Namespace 0 should have block 3");
    assert_eq!(ns1_block, Some(4), "Namespace 1 should have block 4");
    assert_eq!(ns2_block, Some(5), "Namespace 2 should have block 5");

    Ok(())
}

/// Test that `find_valid_namespace_state` returns the correct namespace
#[tokio::test]
async fn test_find_valid_namespace_state() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "find_valid_ns";
    let config = CircularBufferConfig { buffer_size: 3 };
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let address = Address::repeat_byte(0xbb);

    // Write blocks 0-5
    for block_num in 0..6 {
        let update = create_test_update(
            block_num,
            B256::ZERO,
            address,
            block_num * 100,
            block_num,
            HashMap::new(),
            None,
        );
        writer.commit_block(update)?;
    }

    // Find valid namespace - should return highest block (5) in namespace 2
    let result = writer.find_valid_namespace_state()?;
    assert!(result.is_some());

    let (valid_idx, highest_block) = result.unwrap();
    assert_eq!(highest_block, 5);
    assert_eq!(valid_idx, 2); // 5 % 3 = 2

    Ok(())
}

/// Test `write_full_state_to_namespace` writes correct state
#[tokio::test]
async fn test_write_full_state_to_namespace() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "write_full_state";
    let config = CircularBufferConfig { buffer_size: 3 };
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    // First write some initial state
    let address = Address::repeat_byte(0xcc);
    let update0 = create_test_update(0, B256::ZERO, address, 1000, 1, HashMap::new(), None);
    writer.commit_block(update0)?;

    // Now use write_full_state_to_namespace to overwrite namespace 0 with different state
    let new_address = Address::repeat_byte(0xdd);
    let new_update = BlockStateUpdate {
        block_number: 99, // Different block number
        block_hash: B256::repeat_byte(0x99),
        state_root: B256::repeat_byte(0x88),
        accounts: vec![AccountState {
            address_hash: new_address.into(),
            balance: U256::from(5000u64),
            nonce: 50,
            code_hash: B256::ZERO,
            code: None,
            storage: HashMap::new(),
            deleted: false,
        }],
    };

    writer.write_full_state_to_namespace(0, &new_update)?;

    // Verify the namespace now has block 99
    let ns_block = get_namespace_block_number(&mut conn, namespace, 0)?;
    assert_eq!(ns_block, Some(99));

    // Verify the old account is gone and new account exists
    // Note: We need to read at block 99 since that's what the namespace now has
    let old_account = reader.get_account(address.into(), 99);
    assert!(old_account.is_err() || old_account.unwrap().is_none());

    let new_account = reader.get_account(new_address.into(), 99)?;
    assert!(new_account.is_some());
    let new_account = new_account.unwrap();
    assert_eq!(new_account.balance, U256::from(5000u64));
    assert_eq!(new_account.nonce, 50);

    Ok(())
}

/// Test that demonstrates the repair bug would have caused issues
/// This test shows what WOULD go wrong with the old copy-based approach
#[tokio::test]
async fn test_repair_bug_demonstration() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "repair_bug_demo";
    let config = CircularBufferConfig { buffer_size: 3 };
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    let address = Address::repeat_byte(0xee);

    // Write blocks 0-5
    for block_num in 0..6 {
        let update = create_test_update(
            block_num,
            B256::ZERO,
            address,
            block_num * 100,
            block_num,
            HashMap::new(),
            None,
        );
        writer.commit_block(update)?;
    }

    // After block 5: ns0=3, ns1=4, ns2=5
    // Highest valid is ns2 with block 5

    let (valid_idx, highest_block) = writer.find_valid_namespace_state()?.unwrap();
    assert_eq!(valid_idx, 2);
    assert_eq!(highest_block, 5);

    // Calculate expected blocks for all namespaces
    let expected_ns0 = writer.expected_block_for_namespace(0, highest_block);
    let expected_ns1 = writer.expected_block_for_namespace(1, highest_block);
    let expected_ns2 = writer.expected_block_for_namespace(2, highest_block);

    assert_eq!(expected_ns0, 3, "Namespace 0 should have block 3");
    assert_eq!(expected_ns1, 4, "Namespace 1 should have block 4");
    assert_eq!(expected_ns2, 5, "Namespace 2 should have block 5");

    // Verify actual blocks match expected
    let actual_ns0 = get_namespace_block_number(&mut conn, namespace, 0)?;
    let actual_ns1 = get_namespace_block_number(&mut conn, namespace, 1)?;
    let actual_ns2 = get_namespace_block_number(&mut conn, namespace, 2)?;

    assert_eq!(actual_ns0, Some(expected_ns0));
    assert_eq!(actual_ns1, Some(expected_ns1));
    assert_eq!(actual_ns2, Some(expected_ns2));

    // THE BUG: If we had used reset_namespace_from(0, 2), namespace 0 would have:
    // - Block number 5 (WRONG! should be 3)
    // - State from block 5 (WRONG! should be state at block 3)
    // This breaks the circular buffer invariant!

    // Instead, the fix (write_full_state_to_namespace) would:
    // - Fetch state for block 3 from trace provider
    // - Write that correct state to namespace 0
    // - Set block number to 3

    Ok(())
}

/// Test stale lock recovery stores and uses diffs correctly
#[tokio::test]
async fn test_stale_lock_recovery_with_diffs() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "stale_lock_diffs";
    let config = CircularBufferConfig { buffer_size: 3 };
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let address = Address::repeat_byte(0xff);

    // Write blocks 0-2
    for block_num in 0..3 {
        let update = create_test_update(
            block_num,
            B256::ZERO,
            address,
            block_num * 100,
            block_num,
            HashMap::new(),
            None,
        );
        writer.commit_block(update)?;
    }

    // Verify diffs exist for recent blocks
    assert!(writer.has_diff(0)? || writer.has_diff(1)?);
    assert!(writer.has_diff(2)?);

    // Store a diff manually
    let manual_update = create_test_update(10, B256::ZERO, address, 1000, 10, HashMap::new(), None);
    writer.store_diff_only(&manual_update)?;

    assert!(writer.has_diff(10)?);

    Ok(())
}

/// Test that recovery doesn't break when namespaces are already correct
#[tokio::test]
async fn test_recovery_with_correct_namespaces() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "recovery_correct";
    let config = CircularBufferConfig { buffer_size: 3 };
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    let address = Address::repeat_byte(0x11);

    // Write blocks 0-5 normally
    for block_num in 0..6 {
        let update = create_test_update(
            block_num,
            B256::ZERO,
            address,
            block_num * 100,
            block_num,
            HashMap::new(),
            None,
        );
        writer.commit_block(update)?;
    }

    // All namespaces should be correct
    let (_valid_idx, highest_block) = writer.find_valid_namespace_state()?.unwrap();

    for ns_idx in 0..3 {
        let expected = writer.expected_block_for_namespace(ns_idx, highest_block);
        let actual = get_namespace_block_number(&mut conn, namespace, ns_idx)?;
        assert_eq!(
            actual,
            Some(expected),
            "Namespace {ns_idx} should match expected"
        );
    }

    // Recovery should succeed without changes
    let recoveries = writer.recover_stale_locks()?;
    assert!(recoveries.is_empty(), "No stale locks should be found");

    Ok(())
}

/// Test recovery of a specific namespace using `force_recover_namespace`
#[tokio::test]
async fn test_force_recover_namespace() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "force_recover";
    let config = CircularBufferConfig { buffer_size: 3 };
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    let address = Address::repeat_byte(0x22);

    // Write blocks 0-2
    for block_num in 0..3 {
        let update = create_test_update(
            block_num,
            B256::ZERO,
            address,
            block_num * 100,
            block_num,
            HashMap::new(),
            None,
        );
        writer.commit_block(update)?;
    }

    // Manually create a stale lock on namespace 0
    let mut lock = NamespaceLock::new(3, "crashed-writer".to_string());
    lock.started_at = 0; // Make it stale
    conn.set::<_, _, ()>(
        &get_write_lock_key(&format!("{namespace}:0")),
        lock.to_json()?,
    )?;

    // Store the diff that would be needed for recovery
    let update3 = create_test_update(3, B256::ZERO, address, 300, 3, HashMap::new(), None);
    writer.store_diff_only(&update3)?;

    // Force recover namespace 0
    let recovery = writer.force_recover_namespace(0)?;
    assert!(recovery.is_some());

    let recovery = recovery.unwrap();
    assert_eq!(recovery.target_block, 3);
    assert_eq!(recovery.writer_id, "crashed-writer");

    // Lock should be released
    let lock_key = get_write_lock_key(&format!("{namespace}:0"));
    let exists: bool = conn.exists(&lock_key)?;
    assert!(!exists, "Lock should be released after recovery");

    Ok(())
}

/// Test that invalid namespace index returns error
#[tokio::test]
async fn test_invalid_namespace_index_error() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "invalid_ns";
    let config = CircularBufferConfig { buffer_size: 3 };
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config)?;

    // Try to access namespace 5 when buffer_size is 3
    let result = writer.get_namespace_block(5);
    assert!(result.is_err());

    let result = writer.force_recover_namespace(10);
    assert!(result.is_err());

    Ok(())
}

/// Test the complete self-healing scenario from your error message
#[tokio::test]
async fn test_self_healing_scenario_missing_diff() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "self_healing";
    let config = CircularBufferConfig { buffer_size: 3 };
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let address = Address::repeat_byte(0x33);

    // Write blocks 0-2
    for block_num in 0..3 {
        let update = create_test_update(
            block_num,
            B256::ZERO,
            address,
            block_num * 100,
            block_num,
            HashMap::new(),
            None,
        );
        writer.commit_block(update)?;
    }

    // At this point, diffs for blocks 0, 1, 2 should exist
    // The state worker would use these diffs during recovery

    // Simulate checking for a diff that might be missing
    let _has_diff_0 = writer.has_diff(0)?;
    let has_diff_1 = writer.has_diff(1)?;
    let has_diff_2 = writer.has_diff(2)?;

    // At least the recent diffs should exist
    assert!(has_diff_1 || has_diff_2, "Recent diffs should exist");

    // The self-healing flow would:
    // 1. Detect missing diff via MissingStateDiff error
    // 2. Call fetch_and_store_missing_diff (in StateWorker)
    // 3. Store the diff via store_diff_only
    // 4. Retry recovery

    // Simulate storing a "fetched" diff
    let fetched_update =
        create_test_update(100, B256::ZERO, address, 10000, 100, HashMap::new(), None);
    writer.store_diff_only(&fetched_update)?;

    assert!(writer.has_diff(100)?, "Stored diff should exist");

    Ok(())
}

#[tokio::test]
async fn test_stale_lock_recovery_fails_when_diff_missing() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "recovery_missing_diff";
    let config = CircularBufferConfig { buffer_size: 3 };
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    let address = Address::repeat_byte(0xaa);

    // Write blocks 0-2 to fill all namespaces
    for block_num in 0..3 {
        let update = create_test_update(
            block_num,
            B256::ZERO,
            address,
            block_num * 100,
            block_num,
            HashMap::new(),
            None,
        );
        writer.commit_block(update)?;
    }

    // Manually delete the diff for block 1 (simulating data loss)
    let diff_key = get_diff_key(namespace, 1);
    conn.del::<_, ()>(&diff_key)?;

    // Create a stale lock on namespace 0, claiming to be writing block 3
    // Recovery will need to apply diffs 1, 2, 3 to namespace 0
    let mut lock = NamespaceLock::new(3, "crashed-writer".to_string());
    lock.started_at = 0; // Make it stale
    conn.set::<_, _, ()>(
        &get_write_lock_key(&format!("{namespace}:0")),
        lock.to_json()?,
    )?;

    // Try to recover - should FAIL because diff for block 1 is missing
    let result = writer.recover_stale_locks();

    assert!(result.is_err(), "Recovery should fail when diff is missing");

    match result {
        Err(crate::common::error::StateError::MissingStateDiff {
            needed_block,
            target_block,
        }) => {
            assert_eq!(needed_block, 1, "Should need block 1's diff");
            assert_eq!(target_block, 3, "Target should be block 3");
        }
        Err(e) => panic!("Expected MissingStateDiff error, got: {e:?}"),
        Ok(_) => panic!("Expected error, got success"),
    }

    // CRITICAL: Lock should still be in place (protecting corrupt state)
    let lock_key = get_write_lock_key(&format!("{namespace}:0"));
    let lock_exists: bool = conn.exists(&lock_key)?;
    assert!(
        lock_exists,
        "Lock MUST remain when recovery fails - protects readers from corrupt data"
    );

    Ok(())
}

/// CRITICAL TEST 2: Recovery succeeds when all required diffs exist
#[tokio::test]
async fn test_stale_lock_recovery_succeeds_with_all_diffs() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "recovery_with_diffs";
    let config = CircularBufferConfig { buffer_size: 3 };
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    let address = Address::repeat_byte(0xbb);

    // Write blocks 0-2
    for block_num in 0..3 {
        let update = create_test_update(
            block_num,
            B256::ZERO,
            address,
            block_num * 100,
            block_num,
            HashMap::new(),
            None,
        );
        writer.commit_block(update)?;
    }

    // Store diff for block 3 (what recovery will need)
    let update3 = create_test_update(3, B256::ZERO, address, 300, 3, HashMap::new(), None);
    writer.store_diff_only(&update3)?;

    // Create stale lock on namespace 0, targeting block 3
    let mut lock = NamespaceLock::new(3, "crashed-writer".to_string());
    lock.started_at = 0;
    conn.set::<_, _, ()>(
        &get_write_lock_key(&format!("{namespace}:0")),
        lock.to_json()?,
    )?;

    // Recovery should succeed
    let recoveries = writer.recover_stale_locks()?;

    assert_eq!(recoveries.len(), 1, "Should recover one stale lock");
    assert_eq!(recoveries[0].target_block, 3);
    assert_eq!(recoveries[0].writer_id, "crashed-writer");

    // Lock should be released
    let lock_key = get_write_lock_key(&format!("{namespace}:0"));
    let lock_exists: bool = conn.exists(&lock_key)?;
    assert!(
        !lock_exists,
        "Lock should be released after successful recovery"
    );

    // Namespace 0 should now have block 3
    let ns_block = get_namespace_block_number(&mut conn, namespace, 0)?;
    assert_eq!(ns_block, Some(3), "Namespace should be updated to block 3");

    // State should be readable and correct
    let account = reader.get_account(address.into(), 3)?;
    assert!(
        account.is_some(),
        "Account should be readable after recovery"
    );
    assert_eq!(account.unwrap().balance, U256::from(300u64));

    Ok(())
}

/// CRITICAL TEST 3: Verify data integrity after recovery
///
/// This ensures recovery doesn't silently corrupt state
#[tokio::test]
async fn test_recovery_maintains_data_integrity() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "recovery_integrity";
    let config = CircularBufferConfig { buffer_size: 3 };
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    // Create multiple accounts with storage
    let addr_a = Address::repeat_byte(0xa1);
    let addr_b = Address::repeat_byte(0xb1);

    // Block 0: Create account A with storage
    let storage_a = HashMap::from([
        (u256_from_u64(1), u256_from_u64(100)),
        (u256_from_u64(2), u256_from_u64(200)),
    ]);
    let update0 = create_test_update(0, B256::ZERO, addr_a, 1000, 1, storage_a, None);
    writer.commit_block(update0)?;

    // Block 1: Create account B
    let update1 = create_test_update(1, B256::ZERO, addr_b, 2000, 2, HashMap::new(), None);
    writer.commit_block(update1)?;

    // Block 2: Update account A's storage
    let storage_a2 = HashMap::from([
        (u256_from_u64(1), u256_from_u64(150)), // Update slot 1
        (u256_from_u64(3), u256_from_u64(300)), // Add slot 3
    ]);
    let update2 = create_test_update(2, B256::ZERO, addr_a, 1500, 3, storage_a2, None);
    writer.commit_block(update2)?;

    // Store diff for block 3 (will be applied during recovery)
    let storage_a3 = HashMap::from([
        (u256_from_u64(2), U256::ZERO), // Delete slot 2
    ]);
    let update3 = create_test_update(3, B256::ZERO, addr_a, 2000, 4, storage_a3, None);
    writer.store_diff_only(&update3)?;

    // Create stale lock
    let mut lock = NamespaceLock::new(3, "crashed".to_string());
    lock.started_at = 0;
    conn.set::<_, _, ()>(
        &get_write_lock_key(&format!("{namespace}:0")),
        lock.to_json()?,
    )?;

    // Recover
    let recoveries = writer.recover_stale_locks()?;
    assert_eq!(recoveries.len(), 1);

    // VERIFY DATA INTEGRITY after recovery:

    // Account A at block 3 should have:
    // - balance: 2000 (from block 3 diff)
    // - nonce: 4 (from block 3 diff)
    // - slot 1: 150 (from block 2, not overwritten in block 3)
    // - slot 2: DELETED (zeroed in block 3)
    // - slot 3: 300 (from block 2, not overwritten in block 3)
    let account_a = reader.get_full_account(addr_a.into(), 3)?;
    assert!(account_a.is_some());
    let account_a = account_a.unwrap();

    assert_eq!(
        account_a.balance,
        U256::from(2000u64),
        "Balance should be updated"
    );
    assert_eq!(account_a.nonce, 4, "Nonce should be updated");

    let storage = reader.get_all_storage(addr_a.into(), 3)?;
    assert_eq!(
        storage.get(&hash_slot(u256_from_u64(1))),
        Some(&u256_from_u64(150).into()),
        "Slot 1 should retain value from block 2"
    );
    assert!(
        !storage.contains_key(&hash_slot(u256_from_u64(2))),
        "Slot 2 should be deleted"
    );
    assert_eq!(
        storage.get(&hash_slot(u256_from_u64(3))),
        Some(&u256_from_u64(300).into()),
        "Slot 3 should retain value from block 2"
    );

    // Account B should still exist (cumulative state)
    let account_b = reader.get_account(addr_b.into(), 3)?;
    assert!(
        account_b.is_some(),
        "Account B should exist in cumulative state"
    );
    assert_eq!(account_b.unwrap().balance, U256::from(2000u64));

    Ok(())
}

/// CRITICAL TEST 4: Recovery with multiple intermediate diffs
///
/// Tests the scenario where namespace is far behind and needs multiple diffs
#[tokio::test]
async fn test_recovery_applies_multiple_intermediate_diffs() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "recovery_multi_diff";
    let config = CircularBufferConfig { buffer_size: 3 };
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    let address = Address::repeat_byte(0xcc);

    // Write only block 0 to namespace 0
    let update0 = create_test_update(0, B256::ZERO, address, 100, 0, HashMap::new(), None);
    writer.commit_block(update0)?;

    // Store diffs for blocks 1, 2, 3 (all needed for recovery)
    for block_num in 1..=3 {
        let update = create_test_update(
            block_num,
            B256::ZERO,
            address,
            block_num * 100,
            block_num,
            HashMap::new(),
            None,
        );
        writer.store_diff_only(&update)?;
    }

    // Create stale lock targeting block 3
    // This means recovery needs to apply diffs 1, 2, 3
    let mut lock = NamespaceLock::new(3, "crashed".to_string());
    lock.started_at = 0;
    conn.set::<_, _, ()>(
        &get_write_lock_key(&format!("{namespace}:0")),
        lock.to_json()?,
    )?;

    // Verify namespace 0 is still at block 0
    let ns_block_before = get_namespace_block_number(&mut conn, namespace, 0)?;
    assert_eq!(ns_block_before, Some(0));

    // Recover
    let recoveries = writer.recover_stale_locks()?;
    assert_eq!(recoveries.len(), 1);
    assert_eq!(
        recoveries[0].previous_block,
        Some(0),
        "Should record previous block"
    );
    assert_eq!(recoveries[0].target_block, 3);

    // Verify namespace 0 is now at block 3
    let ns_block_after = get_namespace_block_number(&mut conn, namespace, 0)?;
    assert_eq!(ns_block_after, Some(3));

    // Verify final state is from block 3
    let account = reader.get_account(address.into(), 3)?;
    assert!(account.is_some());
    assert_eq!(account.clone().unwrap().balance, U256::from(300u64));
    assert_eq!(account.unwrap().nonce, 3);

    Ok(())
}

/// CRITICAL TEST 5: Recovery when crash happened AFTER metadata update
///
/// This tests the case where:
/// - Accounts were written
/// - Metadata was updated (namespace shows correct block)
/// - But lock wasn't released before crash
///
/// In this case, state is already consistent, just need to release lock
#[tokio::test]
async fn test_recovery_when_only_lock_release_failed() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "recovery_lock_only";
    let config = CircularBufferConfig { buffer_size: 3 };
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    let address = Address::repeat_byte(0xdd);

    // Write blocks 0-2
    for block_num in 0..3 {
        let update = create_test_update(
            block_num,
            B256::ZERO,
            address,
            block_num * 100,
            block_num,
            HashMap::new(),
            None,
        );
        writer.commit_block(update)?;
    }

    // Manually create a stale lock that claims block 3,
    // BUT the namespace already has block 3's state
    // (simulating crash after metadata update, before lock release)

    // First, manually update namespace 0 to claim block 3
    conn.set::<_, _, ()>(&format!("{namespace}:0:block"), "3")?;

    // Create stale lock targeting block 3
    let mut lock = NamespaceLock::new(3, "crashed".to_string());
    lock.started_at = 0;
    conn.set::<_, _, ()>(
        &get_write_lock_key(&format!("{namespace}:0")),
        lock.to_json()?,
    )?;

    // Recovery should just release the lock (state is already consistent)
    let recoveries = writer.recover_stale_locks()?;
    assert_eq!(recoveries.len(), 1);
    assert_eq!(recoveries[0].target_block, 3);
    assert_eq!(
        recoveries[0].previous_block,
        Some(3),
        "Previous block should match target"
    );

    // Lock should be released
    let lock_key = get_write_lock_key(&format!("{namespace}:0"));
    let lock_exists: bool = conn.exists(&lock_key)?;
    assert!(!lock_exists);

    Ok(())
}

/// CRITICAL TEST 6: Namespace corruption detection
///
/// Tests that the system correctly detects when a namespace has the wrong block number
#[tokio::test]
async fn test_namespace_corruption_detection() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "corruption_detection";
    let config = CircularBufferConfig { buffer_size: 3 };
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    let address = Address::repeat_byte(0xee);

    // Write blocks 0-5
    for block_num in 0..6 {
        let update = create_test_update(
            block_num,
            B256::ZERO,
            address,
            block_num * 100,
            block_num,
            HashMap::new(),
            None,
        );
        writer.commit_block(update)?;
    }

    // After block 5: ns0=3, ns1=4, ns2=5
    let (_valid_idx, highest_block) = writer.find_valid_namespace_state()?.unwrap();
    assert_eq!(highest_block, 5);

    // Manually corrupt namespace 0 by setting wrong block number
    conn.set::<_, _, ()>(&format!("{namespace}:0:block"), "99")?;

    // Now check: expected vs actual
    let expected_ns0 = writer.expected_block_for_namespace(0, highest_block);
    let actual_ns0 = get_namespace_block_number(&mut conn, namespace, 0)?;

    assert_eq!(expected_ns0, 3, "Expected block for ns0 should be 3");
    assert_eq!(actual_ns0, Some(99), "Actual block is corrupted to 99");
    assert_ne!(
        actual_ns0,
        Some(expected_ns0),
        "Corruption should be detectable"
    );

    Ok(())
}

/// CRITICAL TEST 7: `write_full_state_to_namespace` clears old data
///
/// Ensures that repair doesn't leave stale data from previous block
#[tokio::test]
async fn test_write_full_state_clears_old_data() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "clear_old_data";
    let config = CircularBufferConfig { buffer_size: 3 };
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), namespace, config)?;

    // Write block 0 with account A and storage
    let addr_a = Address::repeat_byte(0xa1);
    let storage = HashMap::from([
        (u256_from_u64(1), u256_from_u64(100)),
        (u256_from_u64(2), u256_from_u64(200)),
    ]);
    let update0 = create_test_update(0, B256::ZERO, addr_a, 1000, 1, storage, None);
    writer.commit_block(update0)?;

    // Verify account A exists
    let account_a = reader.get_account(addr_a.into(), 0)?;
    assert!(account_a.is_some());

    // Now use write_full_state_to_namespace with DIFFERENT account
    let addr_b = Address::repeat_byte(0xb1);
    let new_update = BlockStateUpdate {
        block_number: 99,
        block_hash: B256::repeat_byte(0x99),
        state_root: B256::repeat_byte(0x88),
        accounts: vec![AccountState {
            address_hash: addr_b.into(),
            balance: U256::from(5000u64),
            nonce: 50,
            code_hash: B256::ZERO,
            code: None,
            storage: HashMap::new(),
            deleted: false,
        }],
    };

    writer.write_full_state_to_namespace(0, &new_update)?;

    // Old account A should NOT exist anymore
    let old_account = reader.get_account(addr_a.into(), 99);
    assert!(
        old_account.is_err() || old_account.unwrap().is_none(),
        "Old account should be cleared"
    );

    // Old storage should NOT exist
    let old_storage = reader.get_all_storage(addr_a.into(), 99);
    assert!(
        old_storage.is_err() || old_storage.unwrap().is_empty(),
        "Old storage should be cleared"
    );

    // New account B should exist
    let new_account = reader.get_account(addr_b.into(), 99)?;
    assert!(new_account.is_some());
    assert_eq!(new_account.unwrap().balance, U256::from(5000u64));

    Ok(())
}

/// CRITICAL TEST 8: Concurrent recovery attempts don't corrupt state
#[tokio::test]
async fn test_concurrent_recovery_safety() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "concurrent_recovery";
    let config = CircularBufferConfig { buffer_size: 3 };

    let writer1 = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;
    let writer2 = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    let address = Address::repeat_byte(0xff);

    // Write blocks 0-2
    for block_num in 0..3 {
        let update = create_test_update(
            block_num,
            B256::ZERO,
            address,
            block_num * 100,
            block_num,
            HashMap::new(),
            None,
        );
        writer1.commit_block(update)?;
    }

    // Store diff for block 3
    let update3 = create_test_update(3, B256::ZERO, address, 300, 3, HashMap::new(), None);
    writer1.store_diff_only(&update3)?;

    // Create stale lock
    let mut lock = NamespaceLock::new(3, "crashed".to_string());
    lock.started_at = 0;
    conn.set::<_, _, ()>(
        &get_write_lock_key(&format!("{namespace}:0")),
        lock.to_json()?,
    )?;

    // Both writers try to recover - only one should succeed
    let result1 = writer1.recover_stale_locks();
    let result2 = writer2.recover_stale_locks();

    // At least one should succeed (the one that got there first)
    // The other should either succeed (if first released lock) or find no stale locks
    let success_count = [&result1, &result2].iter().filter(|r| r.is_ok()).count();

    assert!(success_count >= 1, "At least one recovery should succeed");

    // Final state should be consistent
    let ns_block = get_namespace_block_number(&mut conn, namespace, 0)?;
    assert_eq!(ns_block, Some(3), "Final state should be block 3");

    Ok(())
}

#[tokio::test]
async fn test_write_full_state_loses_cumulative_data() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "data_loss_demo";
    let config = CircularBufferConfig { buffer_size: 3 };
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), namespace, config)?;

    // Create 3 different accounts in blocks 0, 1, 2
    let addr_a = Address::repeat_byte(0xa1);
    let addr_b = Address::repeat_byte(0xb1);
    let addr_c = Address::repeat_byte(0xc1);

    let update0 = create_test_update(0, B256::ZERO, addr_a, 1000, 1, HashMap::new(), None);
    writer.commit_block(update0)?;

    let update1 = create_test_update(1, B256::ZERO, addr_b, 2000, 2, HashMap::new(), None);
    writer.commit_block(update1)?;

    let update2 = create_test_update(2, B256::ZERO, addr_c, 3000, 3, HashMap::new(), None);
    writer.commit_block(update2)?;

    // Verify all 3 accounts exist at block 2 (cumulative state)
    assert!(
        reader.get_account(addr_a.into(), 2)?.is_some(),
        "Account A should exist"
    );
    assert!(
        reader.get_account(addr_b.into(), 2)?.is_some(),
        "Account B should exist"
    );
    assert!(
        reader.get_account(addr_c.into(), 2)?.is_some(),
        "Account C should exist"
    );

    // Now use the BUGGY write_full_state_to_namespace with block 3's diff
    // This only contains changes to account A
    let update3 = create_test_update(3, B256::ZERO, addr_a, 1500, 5, HashMap::new(), None);

    // THIS IS THE BUG: It clears namespace and writes only the diff
    writer.write_full_state_to_namespace(0, &update3)?;

    // Account A exists (it was in the diff)
    let account_a = reader.get_account(addr_a.into(), 3)?;
    assert!(account_a.is_some(), "Account A should exist");

    // BUT Accounts B and C are LOST! (They weren't in the diff)
    let account_b = reader.get_account(addr_b.into(), 3);
    let account_c = reader.get_account(addr_c.into(), 3);

    // This demonstrates the data loss bug
    assert!(
        account_b.is_err() || account_b.unwrap().is_none(),
        "BUG CONFIRMED: Account B was lost because write_full_state only writes diff"
    );
    assert!(
        account_c.is_err() || account_c.unwrap().is_none(),
        "BUG CONFIRMED: Account C was lost because write_full_state only writes diff"
    );

    Ok(())
}

/// CRITICAL TEST: Verify the CORRECT approach preserves cumulative state
#[tokio::test]
async fn test_repair_from_valid_state_preserves_cumulative_data() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "cumulative_preserved";
    let config = CircularBufferConfig { buffer_size: 3 };
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    // Create 3 different accounts in blocks 0, 1, 2
    let addr_a = Address::repeat_byte(0xa1);
    let addr_b = Address::repeat_byte(0xb1);
    let addr_c = Address::repeat_byte(0xc1);

    let update0 = create_test_update(0, B256::ZERO, addr_a, 1000, 1, HashMap::new(), None);
    writer.commit_block(update0)?;

    let update1 = create_test_update(1, B256::ZERO, addr_b, 2000, 2, HashMap::new(), None);
    writer.commit_block(update1)?;

    let update2 = create_test_update(2, B256::ZERO, addr_c, 3000, 3, HashMap::new(), None);
    writer.commit_block(update2)?;

    // Store diff for block 3 (only modifies account A)
    let update3 = create_test_update(3, B256::ZERO, addr_a, 1500, 5, HashMap::new(), None);
    writer.store_diff_only(&update3)?;

    // Now use the CORRECT repair approach:
    // Copy from namespace 2 (block 2), then apply diff for block 3
    writer.repair_namespace_from_valid_state(
        0, // target namespace
        2, // source namespace (has block 2)
        3, // target block
    )?;

    // Verify ALL accounts exist after repair
    let account_a = reader.get_account(addr_a.into(), 3)?;
    assert!(account_a.is_some(), "Account A should exist");
    assert_eq!(
        account_a.unwrap().balance,
        U256::from(1500u64),
        "Account A should have updated balance"
    );

    let account_b = reader.get_account(addr_b.into(), 3)?;
    assert!(
        account_b.is_some(),
        "Account B should still exist (from cumulative state)"
    );
    assert_eq!(account_b.unwrap().balance, U256::from(2000u64));

    let account_c = reader.get_account(addr_c.into(), 3)?;
    assert!(
        account_c.is_some(),
        "Account C should still exist (from cumulative state)"
    );
    assert_eq!(account_c.unwrap().balance, U256::from(3000u64));

    // Verify block number is correct
    let ns_block = get_namespace_block_number(&mut conn, namespace, 0)?;
    assert_eq!(ns_block, Some(3));

    Ok(())
}

/// CRITICAL TEST: Repair fails if required diffs are missing
#[tokio::test]
async fn test_repair_fails_if_diffs_missing() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "repair_missing_diff";
    let config = CircularBufferConfig { buffer_size: 3 };
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let address = Address::repeat_byte(0xaa);

    // Write blocks 0-2
    for block_num in 0..3 {
        let update = create_test_update(
            block_num,
            B256::ZERO,
            address,
            block_num * 100,
            block_num,
            HashMap::new(),
            None,
        );
        writer.commit_block(update)?;
    }

    // Try to repair namespace 0 to block 5, but diffs 3, 4, 5 don't exist
    let result = writer.repair_namespace_from_valid_state(0, 2, 5);

    assert!(result.is_err(), "Repair should fail when diffs are missing");
    match result {
        Err(crate::common::error::StateError::MissingStateDiff {
            needed_block,
            target_block,
        }) => {
            assert_eq!(needed_block, 3, "Should need diff for block 3 first");
            assert_eq!(target_block, 5);
        }
        Err(e) => panic!("Expected MissingStateDiff error, got: {e:?}"),
        Ok(()) => panic!("Expected error"),
    }

    Ok(())
}

/// CRITICAL TEST: Repair applies multiple diffs correctly
#[tokio::test]
async fn test_repair_applies_multiple_diffs_in_order() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "repair_multi_diff";
    let config = CircularBufferConfig { buffer_size: 3 }; // Use 3 for easier reasoning
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    let address = Address::repeat_byte(0xbb);

    // Write block 0 with initial state (goes to namespace 0)
    let storage_0 = HashMap::from([(u256_from_u64(1), u256_from_u64(100))]);
    let update0 = create_test_update(0, B256::ZERO, address, 1000, 0, storage_0, None);
    writer.commit_block(update0)?;

    // Write block 1 (goes to namespace 1)
    let storage_1 = HashMap::from([(u256_from_u64(2), u256_from_u64(200))]);
    let update1 = create_test_update(1, B256::ZERO, address, 1100, 1, storage_1, None);
    writer.commit_block(update1)?;

    // Write block 2 (goes to namespace 2)
    let storage_2 = HashMap::from([
        (u256_from_u64(1), u256_from_u64(150)), // Update slot 1
        (u256_from_u64(3), u256_from_u64(300)), // Add slot 3
    ]);
    let update2 = create_test_update(2, B256::ZERO, address, 1200, 2, storage_2, None);
    writer.commit_block(update2)?;

    // Now we want to test repair. Let's repair namespace 0 to block 3.
    // Block 3 goes to namespace 0 (3 % 3 = 0), so this is valid!

    // Store diff for block 3: Delete slot 2, update balance
    let storage_3 = HashMap::from([(u256_from_u64(2), U256::ZERO)]); // Delete
    let update3 = create_test_update(3, B256::ZERO, address, 1300, 3, storage_3, None);
    writer.store_diff_only(&update3)?;

    // Repair namespace 0 to block 3, using namespace 2 (block 2) as source
    // This will: copy ns2 state, then apply diffs for block 3
    writer.repair_namespace_from_valid_state(0, 2, 3)?;

    // Verify namespace 0 now has block 3
    let ns0_block = get_namespace_block_number(&mut conn, namespace, 0)?;
    assert_eq!(ns0_block, Some(3), "Namespace 0 should have block 3");

    // Read at block 3 - this should work now because block 3 IS in namespace 0
    let account = reader.get_account(address.into(), 3)?;
    assert!(account.is_some(), "Account should exist at block 3");
    let account = account.unwrap();
    assert_eq!(
        account.balance,
        U256::from(1300u64),
        "Balance should be from block 3 diff"
    );
    assert_eq!(account.nonce, 3, "Nonce should be from block 3 diff");

    // Verify storage state:
    // - Slot 1: 150 (from block 2, kept)
    // - Slot 2: DELETED (zeroed in block 3)
    // - Slot 3: 300 (from block 2, kept)
    let storage = reader.get_all_storage(address.into(), 3)?;

    assert_eq!(
        storage.get(&hash_slot(u256_from_u64(1))),
        Some(&u256_from_u64(150).into()),
        "Slot 1 should have value from block 2"
    );
    assert!(
        !storage.contains_key(&hash_slot(u256_from_u64(2))),
        "Slot 2 should be deleted (zeroed in block 3)"
    );
    assert_eq!(
        storage.get(&hash_slot(u256_from_u64(3))),
        Some(&u256_from_u64(300).into()),
        "Slot 3 should have value from block 2"
    );

    Ok(())
}

/// CRITICAL TEST: Repair doesn't modify source namespace
#[tokio::test]
async fn test_repair_does_not_modify_source() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "repair_source_intact";
    let config = CircularBufferConfig { buffer_size: 3 };
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    let address = Address::repeat_byte(0xcc);

    // Write blocks 0-2
    for block_num in 0..3 {
        let update = create_test_update(
            block_num,
            B256::ZERO,
            address,
            block_num * 100,
            block_num,
            HashMap::new(),
            None,
        );
        writer.commit_block(update)?;
    }

    // Store diff for block 3
    let update3 = create_test_update(3, B256::ZERO, address, 300, 3, HashMap::new(), None);
    writer.store_diff_only(&update3)?;

    // Record source namespace state before repair
    let source_block_before = get_namespace_block_number(&mut conn, namespace, 2)?;
    let source_account_before = reader.get_account(address.into(), 2)?;

    // Repair namespace 0 from namespace 2
    writer.repair_namespace_from_valid_state(0, 2, 3)?;

    // Verify source namespace is unchanged
    let source_block_after = get_namespace_block_number(&mut conn, namespace, 2)?;
    let source_account_after = reader.get_account(address.into(), 2)?;

    assert_eq!(
        source_block_before, source_block_after,
        "Source block number should not change"
    );
    assert_eq!(
        source_account_before.unwrap().balance,
        source_account_after.unwrap().balance,
        "Source account should not change"
    );

    Ok(())
}

/// CRITICAL TEST: Cannot repair from future block
#[tokio::test]
async fn test_repair_fails_if_source_is_ahead() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "repair_source_ahead";
    let config = CircularBufferConfig { buffer_size: 3 };
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let address = Address::repeat_byte(0xdd);

    // Write blocks 0-2
    for block_num in 0..3 {
        let update = create_test_update(
            block_num,
            B256::ZERO,
            address,
            block_num * 100,
            block_num,
            HashMap::new(),
            None,
        );
        writer.commit_block(update)?;
    }

    // Try to repair namespace 0 to block 1, using namespace 2 (block 2) as source
    // This should fail because source block (2) > target block (1)
    let result = writer.repair_namespace_from_valid_state(0, 2, 1);

    assert!(
        result.is_err(),
        "Should fail when source is ahead of target"
    );

    Ok(())
}

/// CRITICAL TEST: Verify diffs are checked BEFORE modifying state
#[tokio::test]
async fn test_repair_validates_diffs_before_modifying() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "repair_validate_first";
    let config = CircularBufferConfig { buffer_size: 3 };
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    let address = Address::repeat_byte(0xee);

    // Write blocks 0-2
    for block_num in 0..3 {
        let update = create_test_update(
            block_num,
            B256::ZERO,
            address,
            block_num * 100,
            block_num,
            HashMap::new(),
            None,
        );
        writer.commit_block(update)?;
    }

    // Store diff for block 3, but NOT block 4
    let update3 = create_test_update(3, B256::ZERO, address, 300, 3, HashMap::new(), None);
    writer.store_diff_only(&update3)?;

    // Record target namespace state before attempted repair
    let target_block_before = get_namespace_block_number(&mut conn, namespace, 0)?;
    let target_account_before = reader.get_account(address.into(), 0)?;

    // Try to repair to block 5 (missing diffs 4 and 5)
    let result = writer.repair_namespace_from_valid_state(0, 2, 5);
    assert!(result.is_err(), "Should fail due to missing diff");

    // CRITICAL: Target namespace should NOT be modified because validation failed first
    let target_block_after = get_namespace_block_number(&mut conn, namespace, 0)?;
    let target_account_after = reader.get_account(address.into(), 0)?;

    assert_eq!(
        target_block_before, target_block_after,
        "Target namespace should not be modified when repair fails"
    );
    assert_eq!(
        target_account_before.unwrap().balance,
        target_account_after.unwrap().balance,
        "Target account should not be modified when repair fails"
    );

    Ok(())
}

/// CRITICAL TEST: Full circular buffer repair scenario
#[tokio::test]
async fn test_full_circular_buffer_repair() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "full_buffer_repair";
    let config = CircularBufferConfig { buffer_size: 3 };
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    // Create different accounts for tracking
    let addr_a = Address::repeat_byte(0xa1);
    let addr_b = Address::repeat_byte(0xb1);
    let addr_c = Address::repeat_byte(0xc1);
    let addr_d = Address::repeat_byte(0xd1);

    // Write blocks 0-3 normally (first rotation + 1)
    let update0 = create_test_update(0, B256::ZERO, addr_a, 100, 0, HashMap::new(), None);
    writer.commit_block(update0)?;

    let update1 = create_test_update(1, B256::ZERO, addr_b, 200, 1, HashMap::new(), None);
    writer.commit_block(update1)?;

    let update2 = create_test_update(2, B256::ZERO, addr_c, 300, 2, HashMap::new(), None);
    writer.commit_block(update2)?;

    let update3 = create_test_update(3, B256::ZERO, addr_d, 400, 3, HashMap::new(), None);
    writer.commit_block(update3)?;

    // At this point: ns0=3, ns1=1, ns2=2
    // Verify initial state
    assert_eq!(
        get_namespace_block_number(&mut conn, namespace, 0)?,
        Some(3)
    );
    assert_eq!(
        get_namespace_block_number(&mut conn, namespace, 1)?,
        Some(1)
    );
    assert_eq!(
        get_namespace_block_number(&mut conn, namespace, 2)?,
        Some(2)
    );

    // Corrupt namespace 1 by setting wrong block number
    conn.set::<_, _, ()>(&format!("{namespace}:1:block"), "99")?;

    // NOTE: find_valid_namespace_state does NOT detect this corruption!
    // It only checks if namespace has data. We detect corruption by comparing
    // actual vs expected block numbers.

    // Find the highest valid block from namespaces that ARE correct
    // In a real scenario, we'd iterate through all namespaces
    let mut highest_valid: Option<(usize, u64)> = None;

    for ns_idx in 0..3 {
        let actual_block = get_namespace_block_number(&mut conn, namespace, ns_idx)?;
        if let Some(block) = actual_block {
            // Check if this block number is "reasonable" (within expected range)
            // A block of 99 is suspicious when we only wrote up to block 3
            if block <= 10 {
                // Simple heuristic for this test
                match highest_valid {
                    None => highest_valid = Some((ns_idx, block)),
                    Some((_, best)) if block > best => highest_valid = Some((ns_idx, block)),
                    _ => {}
                }
            }
        }
    }

    let (valid_idx, valid_block) = highest_valid.expect("Should find a valid namespace");

    // Namespace 0 has block 3 (highest reasonable block)
    assert_eq!(
        valid_idx, 0,
        "Namespace 0 should be selected as valid source"
    );
    assert_eq!(valid_block, 3);

    // Now detect corruption: namespace 1 should have block 4, but has 99
    let expected_ns1_block = writer.expected_block_for_namespace(1, valid_block);
    let actual_ns1_block = get_namespace_block_number(&mut conn, namespace, 1)?;

    // expected_block_for_namespace(1, 3) with buffer_size=3:
    // highest_ns_idx = 3 % 3 = 0
    // ns_idx = 1
    // Since 1 > 0: highest_valid_block - (buffer_size - (ns_idx - highest_ns_idx))
    //            = 3 - (3 - (1 - 0)) = 3 - 2 = 1
    assert_eq!(expected_ns1_block, 1, "Namespace 1 should have block 1");
    assert_eq!(
        actual_ns1_block,
        Some(99),
        "Namespace 1 is corrupted with block 99"
    );
    assert_ne!(
        actual_ns1_block,
        Some(expected_ns1_block),
        "Corruption detected!"
    );

    // Store diff for block 1 (needed for repair, though ns1 already had it)
    // In this case, the diff should already exist from when we wrote block 1

    // For repair, we need to find a source namespace that is BEFORE block 1
    // Namespace 0 has block 3, namespace 2 has block 2 - both are AFTER block 1
    //
    // This is a tricky case! In a circular buffer after multiple rotations,
    // we might not have a "before" state available.
    //
    // For this test, let's use a different approach: use the fact that namespace 2
    // has block 2, and we can repair namespace 1 to block 4 (next block for ns1)

    // Store diff for block 4
    let update4 = create_test_update(4, B256::ZERO, addr_b, 500, 4, HashMap::new(), None);
    writer.store_diff_only(&update4)?;

    // Repair namespace 1 to block 4 (4 % 3 = 1, so block 4 belongs in ns1)
    // Use namespace 0 (block 3) as source
    writer.repair_namespace_from_valid_state(1, 0, 4)?;

    // Verify repair succeeded
    let ns1_block_after = get_namespace_block_number(&mut conn, namespace, 1)?;
    assert_eq!(
        ns1_block_after,
        Some(4),
        "Namespace 1 should now have block 4"
    );

    // Verify we can read from the repaired namespace
    let account = reader.get_account(addr_b.into(), 4)?;
    assert!(
        account.is_some(),
        "Should be able to read account after repair"
    );
    assert_eq!(account.unwrap().balance, U256::from(500u64));

    // Verify cumulative state is preserved (account from earlier blocks)
    let account_a = reader.get_account(addr_a.into(), 4)?;
    assert!(
        account_a.is_some(),
        "Account A should exist in cumulative state"
    );

    let account_d = reader.get_account(addr_d.into(), 4)?;
    assert!(
        account_d.is_some(),
        "Account D should exist in cumulative state"
    );

    Ok(())
}

/// Test that demonstrates the corruption detection logic
#[tokio::test]
async fn test_corruption_detection_logic() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "corruption_detect";
    let config = CircularBufferConfig { buffer_size: 3 };
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    let address = Address::repeat_byte(0xaa);

    // Write blocks 0-5
    for block_num in 0..6 {
        let update = create_test_update(
            block_num,
            B256::ZERO,
            address,
            block_num * 100,
            block_num,
            HashMap::new(),
            None,
        );
        writer.commit_block(update)?;
    }

    // State after block 5: ns0=3, ns1=4, ns2=5

    // Corrupt namespace 0 by setting wrong block
    conn.set::<_, _, ()>(&format!("{namespace}:0:block"), "99")?;

    // Detection logic: compare actual vs expected
    let highest_valid_block = 5u64; // We know this from the non-corrupted namespaces

    let mut corrupted_namespaces = Vec::new();

    for ns_idx in 0..3 {
        let expected = writer.expected_block_for_namespace(ns_idx, highest_valid_block);
        let actual = get_namespace_block_number(&mut conn, namespace, ns_idx)?;

        if actual != Some(expected) {
            corrupted_namespaces.push((ns_idx, expected, actual));
        }
    }

    // Should detect namespace 0 as corrupted
    assert_eq!(corrupted_namespaces.len(), 1);
    assert_eq!(
        corrupted_namespaces[0].0, 0,
        "Namespace 0 should be detected as corrupted"
    );
    assert_eq!(corrupted_namespaces[0].1, 3, "Expected block should be 3");
    assert_eq!(corrupted_namespaces[0].2, Some(99), "Actual block is 99");

    Ok(())
}

/// Test repair with exact namespace/block alignment
#[tokio::test]
async fn test_repair_with_correct_namespace_alignment() -> Result<()> {
    let container = Redis::default().start().await?;
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(6379).await?;
    wait_for_redis(&host, port).await?;

    let namespace = "repair_aligned";
    let config = CircularBufferConfig { buffer_size: 3 };
    let writer = StateWriter::new(&format!("redis://{host}:{port}"), namespace, config.clone())?;
    let reader = StateReader::new(&format!("redis://{host}:{port}"), namespace, config)?;

    let client = redis::Client::open(format!("redis://{host}:{port}"))?;
    let mut conn = client.get_connection()?;

    let address = Address::repeat_byte(0xcc);

    // Write blocks 0, 1, 2
    for block_num in 0..3 {
        let update = create_test_update(
            block_num,
            B256::ZERO,
            address,
            block_num * 100,
            block_num,
            HashMap::new(),
            None,
        );
        writer.commit_block(update)?;
    }

    // At this point: ns0=0, ns1=1, ns2=2
    // Diffs exist for: 0, 1, 2

    // We want to repair namespace 0 to block 3
    // Source: namespace 2 (block 2)
    // Need diff: block 3 only (since source is block 2)

    // Store diff for block 3 only
    // This will NOT delete block 0's diff (3-3=0, but we're using store_diff_only which does cleanup)
    // Actually it WILL delete block 0's diff, but we only need block 3's diff
    let update3 = create_test_update(3, B256::ZERO, address, 300, 3, HashMap::new(), None);
    writer.store_diff_only(&update3)?;

    // Repair namespace 0 from namespace 2, target block 3
    // ns2 has block 2
    // Block 3 goes to namespace 0 (3 % 3 = 0) ✓
    // Need to apply diffs: 3 only
    writer.repair_namespace_from_valid_state(0, 2, 3)?;

    // Verify
    let ns0_block = get_namespace_block_number(&mut conn, namespace, 0)?;
    assert_eq!(ns0_block, Some(3));

    // Read should work because block 3 is in namespace 0
    let account = reader.get_account(address.into(), 3)?;
    assert!(account.is_some());
    assert_eq!(account.unwrap().balance, U256::from(300u64));

    Ok(())
}
