//! Shared helpers for transport decoding/parsing across HTTP and gRPC.

#[derive(thiserror::Error, Debug, Clone, PartialEq)]
pub enum HttpDecoderError {
    #[error("Invalid block number format: {0}")]
    InvalidBlockNumber(String),
    #[error("Invalid beneficiary format: {0}")]
    InvalidBeneficiary(String),
    #[error("Invalid timestamp format: {0}")]
    InvalidTimestamp(String),
    #[error("Invalid block gas limit format: {0}")]
    InvalidBlockGasLimit(String),
    #[error("Invalid base fee format: {0}")]
    InvalidBasefee(String),
    #[error("Invalid difficulty format: {0}")]
    InvalidDifficulty(String),
    #[error("Invalid prevrandao format: {0}")]
    InvalidPrevrandao(String),
    #[error("Invalid blob excess gas format: {0}")]
    InvalidBlobExcessGas(String),
    #[error("Invalid blob hashes format")]
    InvalidBlobHashes,
    #[error("Invalid last tx hash format: {0}")]
    InvalidLastTxHash(String),
    #[error("Invalid gas priority fee format")]
    InvalidGasPriorityFee,
    #[error("Invalid max fee per blob gas format: {0}")]
    InvalidMaxFeePerBlobGas(String),
    #[error("Invalid tx type format: {0}")]
    InvalidTxType(String),
    #[error("Invalid authorization list format")]
    InvalidAuthorizationList,
    #[error("Invalid n transaction format: {0}")]
    InvalidNTransactions(String),
    #[error("Block env validation error: {0}")]
    BlockEnvValidation(String),
    #[error("Reorg validation error: {0}")]
    ReorgValidation(String),
    #[error("Invalid address format: {0}")]
    InvalidAddress(String),
    #[error("Invalid hash format: {0}")]
    InvalidHash(String),
    #[error("Invalid hex value: {0}")]
    InvalidHex(String),
    #[error("Missing transaction parameters")]
    MissingParams,
    #[error("No transactions found in request")]
    NoTransactions,
    #[error("Missing field 'transactions'")]
    MissingTransactionsField,
    #[error("Missing field 'events'")]
    MissingEventsField,
    #[error("Missing field 'txExecutionId' in transaction")]
    MissingTxExecutionId,
    #[error("Missing field 'txEnv' in transaction")]
    MissingTxEnv,
    #[error("Missing field 'hash' in transaction")]
    MissingHashField,
    #[error("Missing field 'selected_iteration_id' in commit head event")]
    MissingSelectedIterationId,
    #[error("Invalid kind: {0}")]
    InvalidKind(String),
    #[error("Invalid access list")]
    InvalidAccessList,
    #[error("Invalid caller address: {0}")]
    InvalidCaller(String),
    #[error("Invalid recipient address: {0}")]
    InvalidRecipient(String),
    #[error("Invalid value")]
    InvalidValue,
    #[error("Invalid data field")]
    InvalidData,
    #[error("Invalid nonce: {0}")]
    InvalidNonce(String),
    #[error("Invalid gas_limit: {0}")]
    InvalidGasLimit(String),
    #[error("Invalid gas_price: {0}")]
    InvalidGasPrice(String),
    #[error("Invalid signature")]
    InvalidSignature,
    #[error("Invalid Y parity: {0}")]
    InvalidYParity(String),
    #[error("Invalid chain_id: {0}")]
    InvalidChainId(String),
    #[error("Invalid transaction format: {0}")]
    InvalidTransaction(String),
    #[error("Invalid sendEvents payload: {0}")]
    InvalidEvent(String),
    #[error("Missing field 'number' in BlockEnv")]
    MissingBlockNumber,
    #[error("Missing field 'beneficiary' in BlockEnv")]
    MissingBeneficiary,
    #[error("Missing field 'timestamp' in BlockEnv")]
    MissingTimestamp,
    #[error("Missing field 'gas_limit' in BlockEnv")]
    MissingBlockGasLimit,
    #[error("Missing field 'basefee' in BlockEnv")]
    MissingBasefee,
    #[error("Missing field 'difficulty' in BlockEnv")]
    MissingDifficulty,
    #[error("No events found in request")]
    NoEvents,
}
