//! # `linea`
//!
//! Contains functions for initializing a linea spec of revm.

use revm::Database;
use alloy_evm::EvmEnv;
use alloy_evm::eth::EthEvmContext;
use crate::primitives::{
	Journal,
	BlockEnv,
	TxEnv,
	SpecId,

};

use alloy_evm::precompiles::PrecompilesMap;
use revm::{
    Context,
    Inspector,
    MainnetEvm,
    context::{
        CfgEnv,
        Evm,
        JournalTr,
        LocalContext,
    },
    handler::{
        EthPrecompiles,
        instructions::EthInstructions,
    },
    interpreter::{
        interpreter::EthInterpreter,
    },
    precompile::{
        PrecompileSpecId,
        Precompiles,
    },
};

pub type LineaCtx<'db, DB> =
    Context<BlockEnv, TxEnv, CfgEnv<SpecId>, &'db mut DB, Journal<&'db mut DB>, ()>;
type LineaIns<'db, DB> = EthInstructions<EthInterpreter, LineaCtx<'db, DB>>;
type LineaEvm<'db, DB, I> = Evm<LineaCtx<'db, DB>, I, LineaIns<'db, DB>, PrecompilesMap>;


/// Builds a Linea V3 spec EVM. Identical compared to a London spec of the EVM.
/// The following is a list of changes compared to the `SpecId::LATEST` version of the EVM:
/// ## Opcodes
/// 
/// | Opcode | Ethereum | Linea: current state |
/// |--------|----------|---------------------|
/// | BLOBBASEFEE | Returns the value of the blob base-fee of the current block. | Not supported. |
/// | BLOBHASH | Given an an input in the form of an index, indicating the position of a particular blob within an array of blobs associated with a transaction, returns the hash of the corresponding blob.<br><br>If the index is out of bounds, returns 0. | Not supported. |
/// | BLOCKHASH | Returns the hash of a requested block from the 256 most recent blocks. | Not supported. |
/// | MCOPY | Copies memory areas, allowing the destination and source to overlap. | Not supported. |
/// | PREVRANDAO | Returns the RANDAO value from the previous block. | Not supported. |
/// | PUSH0 | Pushes the constant value 0 onto the stack. | Not supported. |
/// | TLOAD | Load word from transient storage. | Not supported. |
/// | TSTORE | Save word to transient storage. | Not supported |
/// 
/// ## Precompiles
/// 
/// | Precompile | Ethereum | Linea: current state |
/// |------------|----------|---------------------|
/// | BLAKE2f | Compression function F used in the BLAKE2 cryptographic hashing algorithm. | Not supported. |
/// | MODEXP | Arbitrary-precision exponentiation under modulo. | Only supports arguments (base, exponent, modulus) that do not exceed 512-byte integers. |
/// | Point evaluation | Verify that specific points in a polynomial (blobs) match the expected values. | Not supported. |
/// | Precompiles as transaction recipients | Applicable to various use cases. | Not supported. A transaction to address cannot be a precompile, i.e. an address in the range 0x01-0x09. |
/// | RIPEMD-160 | A hash function. | Not supported. |
///
/// ## Unsuported behaviour
/// 
/// Calls to unsuported opcodes will error with invalid opcode errors. Calls to precompiles (except the `0x1`-`0x9` range) will error as if you were
/// calling an empty account
pub fn build_linea_evm<'db, DB, I>(db: &'db mut DB, env: &EvmEnv, inspector: I) -> LineaEvm<'db, DB, I>
where
    DB: Database,
    I: Inspector<LineaCtx<'db, DB>>,
{
    let spec = env.cfg_env.spec;
    let eth_context = EthEvmContext {
        journaled_state: {
            let mut journal = Journal::new(db);
            journal.set_spec_id(spec);
            journal
        },
        block: env.block_env.clone(),
        cfg: env.cfg_env.clone(),
        tx: Default::default(),
        chain: (),
        local: LocalContext::default(),
        error: Ok(()),
    };
    let eth_precompiles = EthPrecompiles {
        precompiles: Precompiles::new(PrecompileSpecId::from_spec_id(spec)),
        spec,
    };
    let precompiles = PrecompilesMap::from_static(eth_precompiles.precompiles);

    MainnetEvm::new_with_inspector(
        eth_context,
        inspector,
        EthInstructions::default(),
        eth_precompiles,
    )
    .with_precompiles(precompiles)
}
