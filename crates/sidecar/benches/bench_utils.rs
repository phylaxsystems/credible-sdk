#![cfg(feature = "bench-utils")]

use assertion_executor::primitives::{
    Address,
    Bytes,
    U256,
    hex,
};
use revm::{
    context::tx::TxEnvBuilder,
    primitives::TxKind,
};
use serde::Deserialize;
use sidecar::{
    execution_ids::TxExecutionId,
    utils::instance::{
        LocalInstance,
        TestTransport,
    },
};
use std::{
    fs::File,
    io::BufReader,
    path::{
        Path,
        PathBuf,
    },
    time::Duration,
};

const ERC20_TRANSFER_SELECTOR: [u8; 4] = [0xa9, 0x05, 0x9c, 0xbb];

pub const DEPLOY_GAS_LIMIT: u64 = 2_000_000;
pub const GAS_LIMIT_PER_TX: u64 = 100_000;

#[derive(Deserialize)]
struct BytecodeArtifact {
    bytecode: BytecodeObject,
}

#[derive(Deserialize)]
struct BytecodeObject {
    object: String,
}

pub fn read_erc20_bytecode(path: &Path) -> Bytes {
    let file = File::open(path).expect("Failed to open ERC20 bytecode artifact");
    let reader = BufReader::new(file);
    let artifact: BytecodeArtifact =
        serde_json::from_reader(reader).expect("Failed to parse ERC20 bytecode artifact");
    let bytecode = artifact.bytecode.object;
    let raw = hex::decode(bytecode.strip_prefix("0x").unwrap_or(&bytecode))
        .expect("Failed to decode ERC20 bytecode");
    Bytes::from(raw)
}

pub fn erc20_bytecode_path() -> PathBuf {
    let working_dir = std::env::current_dir().expect("Failed to read current directory");
    let repo_root = if working_dir.ends_with("credible-sdk") {
        working_dir
    } else if working_dir.ends_with(Path::new("crates/sidecar")) {
        working_dir
            .parent()
            .and_then(|parent| parent.parent())
            .expect("Failed to resolve repo root from crates/sidecar")
            .to_path_buf()
    } else {
        working_dir
    };
    repo_root.join("testdata/mock-protocol/out/erc20.sol/GLDToken.json")
}

pub fn encode_erc20_transfer(to: Address, amount: U256) -> Bytes {
    let mut data = Vec::with_capacity(4 + 32 + 32);
    data.extend_from_slice(&ERC20_TRANSFER_SELECTOR);
    data.extend_from_slice(&[0u8; 12]);
    data.extend_from_slice(to.as_slice());
    data.extend_from_slice(&amount.to_be_bytes::<32>());
    Bytes::from(data)
}

pub async fn deploy_erc20<T: TestTransport>(
    instance: &mut LocalInstance<T>,
    erc20_bytecode: Bytes,
) -> Address {
    instance
        .new_block()
        .await
        .expect("Failed to open deploy block");
    let block_execution_id = instance.current_block_execution_id();
    let nonce = instance.next_nonce(instance.default_account(), block_execution_id);
    let contract_address = instance.default_account().create(nonce);
    let tx_env = TxEnvBuilder::new()
        .caller(instance.default_account())
        .gas_limit(DEPLOY_GAS_LIMIT)
        .gas_price(0)
        .value(U256::ZERO)
        .nonce(nonce)
        .kind(TxKind::Create)
        .data(erc20_bytecode)
        .build()
        .expect("Failed to build deploy transaction");

    let tx_hash = LocalInstance::<T>::generate_random_tx_hash();
    let tx_execution_id = TxExecutionId::new(
        block_execution_id.block_number,
        block_execution_id.iteration_id,
        tx_hash,
        0,
    );

    instance
        .transport
        .send_transaction(tx_execution_id, tx_env)
        .await
        .expect("Failed to send deploy transaction");

    loop {
        match instance.is_transaction_successful(&tx_execution_id).await {
            Ok(success) => {
                if success {
                    break;
                }
                panic!("ERC20 deploy transaction failed");
            }
            Err(e) => {
                if e.to_string().contains("Timeout") {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    continue;
                }
                panic!("error getting deploy result {tx_execution_id:?}: {}", e);
            }
        }
    }

    contract_address
}
