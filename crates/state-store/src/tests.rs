use crate::{
    CircularBufferConfig,
    StateReader,
    StateWriter,
    common::{
        AccountState,
        BlockStateUpdate,
        get_account_key,
        get_diff_key,
        get_storage_key,
    },
    writer::commit_block_atomic,
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
                if err.kind() != redis::ErrorKind::IoError {
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

// Helper to setup Redis connection for each test (now async!)
async fn setup_redis() -> Result<(testcontainers::ContainerAsync<Redis>, redis::Connection)> {
    let container = Redis::default()
        .start()
        .await
        .expect("Failed to start Redis container");

    let host = container
        .get_host()
        .await
        .expect("Failed to get host")
        .to_string();
    let port = container
        .get_host_port_ipv4(6379)
        .await
        .expect("Failed to get port");

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
    assert_eq!(latest, None, "Should have no blocks initially");

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
    assert_eq!(latest, Some(0), "Should have block 0");

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

    // Write blocks 0, 1, 2
    for block_num in 0..3 {
        let update = BlockStateUpdate {
            block_number: block_num,
            block_hash: B256::repeat_byte(u8::try_from(block_num).unwrap()),
            state_root: B256::repeat_byte(u8::try_from(block_num).unwrap()),
            accounts: vec![],
        };
        commit_block_atomic(&mut conn, base_namespace, buffer_size, &update)?;
    }

    // Write block 3 (should delete block 0's diff)
    let update3 = BlockStateUpdate {
        block_number: 3,
        block_hash: B256::repeat_byte(3),
        state_root: B256::repeat_byte(u8::try_from(3).unwrap()),
        accounts: vec![],
    };
    commit_block_atomic(&mut conn, base_namespace, buffer_size, &update3)?;

    // Block 0's diff should be deleted
    let diff_key_0 = get_diff_key(base_namespace, 0);
    let exists_0: bool = redis::cmd("EXISTS").arg(&diff_key_0).query(&mut conn)?;
    assert!(!exists_0, "Diff for block 0 should be deleted");

    // Blocks 1, 2, 3 diffs should exist
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

    // Verify all slots exist
    let all_storage = reader.get_all_storage(address.into(), 0)?;
    assert_eq!(all_storage.len(), 3);

    // Block 1: Set slot 2 to zero (should be deleted)
    let storage_1 = HashMap::from([(u256_from_u64(2), U256::ZERO)]);
    let update1 = create_test_update(1, B256::ZERO, address, 1000, 1, storage_1, None);
    writer.commit_block(update1)?;

    // Verify slot 2 is deleted from Redis
    let all_storage = reader.get_all_storage(address.into(), 1)?;
    assert_eq!(
        all_storage.len(),
        2,
        "Slot 2 should be deleted when set to zero"
    );
    assert!(
        all_storage.contains_key(&hash_slot(u256_from_u64(1))),
        "Slot 1 should still exist"
    );
    assert!(
        !all_storage.contains_key(&hash_slot(u256_from_u64(2))),
        "Slot 2 should be deleted"
    );
    assert!(
        all_storage.contains_key(&hash_slot(u256_from_u64(3))),
        "Slot 3 should still exist"
    );

    // Verify individual slot read returns None for deleted slot
    let slot_2_value = reader.get_storage(address.into(), hash_slot(u256_from_u64(2)), 1)?;
    assert_eq!(slot_2_value, None, "Deleted slot should return None");

    // Block 2: Set slots 1 and 3 to zero
    let storage_2 = HashMap::from([
        (u256_from_u64(1), U256::ZERO),
        (u256_from_u64(3), U256::ZERO),
    ]);
    let update2 = create_test_update(2, B256::ZERO, address, 1000, 2, storage_2, None);
    writer.commit_block(update2)?;

    // Verify all slots are deleted
    let all_storage = reader.get_all_storage(address.into(), 2)?;
    assert_eq!(all_storage.len(), 0, "All slots should be deleted");

    Ok(())
}

// ============================================================================
// NEW ROUNDTRIP TESTS WITH READER + WRITER
// ============================================================================

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

    // Create storage
    let storage = HashMap::from([
        (u256_from_u64(1), u256_from_u64(100)),
        (u256_from_u64(2), u256_from_u64(200)),
        (u256_from_u64(3), u256_from_u64(300)),
    ]);

    let update = create_test_update(
        0,
        B256::repeat_byte(0xBB),
        address,
        5000,
        10,
        storage.clone(),
        None,
    );
    writer.commit_block(update)?;

    // Read back full account
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

    // Block 3: Add slot 5
    let storage_3 = HashMap::from([(u256_from_u64(5), u256_from_u64(500))]);
    let update3 = create_test_update(3, B256::ZERO, address, 1000, 3, storage_3, None);
    writer.commit_block(update3)?;

    // Read block 3 and verify all storage
    let all_storage = reader.get_all_storage(address.into(), 3)?;
    assert_eq!(
        all_storage.len(),
        4,
        "Slot 2 should be deleted when set to zero"
    );
    assert_eq!(
        all_storage.get(&hash_slot(u256_from_u64(1))),
        Some(&u256_from_u64(150).into())
    );
    assert_eq!(all_storage.get(&hash_slot(u256_from_u64(2))), None,);
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
