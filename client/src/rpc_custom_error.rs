//! Implementation defined RPC server errors

use crate::rpc_response::RpcSimulateTransactionResult;
use jsonrpsee::{core::Error, types::ErrorObject};
use solana_sdk::clock::Slot;
use solana_transaction_status::EncodeError;

// Keep in sync with https://github.com/solana-labs/solana-web3.js/blob/master/src/errors.ts
pub const JSON_RPC_SERVER_ERROR_BLOCK_CLEANED_UP: i32 = -32001;
pub const JSON_RPC_SERVER_ERROR_SEND_TRANSACTION_PREFLIGHT_FAILURE: i32 = -32002;
pub const JSON_RPC_SERVER_ERROR_TRANSACTION_SIGNATURE_VERIFICATION_FAILURE: i32 = -32003;
pub const JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE: i32 = -32004;
pub const JSON_RPC_SERVER_ERROR_NODE_UNHEALTHY: i32 = -32005;
pub const JSON_RPC_SERVER_ERROR_TRANSACTION_PRECOMPILE_VERIFICATION_FAILURE: i32 = -32006;
pub const JSON_RPC_SERVER_ERROR_SLOT_SKIPPED: i32 = -32007;
pub const JSON_RPC_SERVER_ERROR_NO_SNAPSHOT: i32 = -32008;
pub const JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED: i32 = -32009;
pub const JSON_RPC_SERVER_ERROR_KEY_EXCLUDED_FROM_SECONDARY_INDEX: i32 = -32010;
pub const JSON_RPC_SERVER_ERROR_TRANSACTION_HISTORY_NOT_AVAILABLE: i32 = -32011;
pub const JSON_RPC_SCAN_ERROR: i32 = -32012;
pub const JSON_RPC_SERVER_ERROR_TRANSACTION_SIGNATURE_LEN_MISMATCH: i32 = -32013;
pub const JSON_RPC_SERVER_ERROR_BLOCK_STATUS_NOT_AVAILABLE_YET: i32 = -32014;
pub const JSON_RPC_SERVER_ERROR_UNSUPPORTED_TRANSACTION_VERSION: i32 = -32015;
pub const JSON_RPC_SERVER_ERROR_MIN_CONTEXT_SLOT_NOT_REACHED: i32 = -32016;

#[derive(thiserror::Error, Debug)]
pub enum RpcCustomError {
    #[error("BlockCleanedUp")]
    BlockCleanedUp {
        slot: Slot,
        first_available_block: Slot,
    },
    #[error("SendTransactionPreflightFailure")]
    SendTransactionPreflightFailure {
        message: String,
        result: RpcSimulateTransactionResult,
    },
    #[error("TransactionSignatureVerificationFailure")]
    TransactionSignatureVerificationFailure,
    #[error("BlockNotAvailable")]
    BlockNotAvailable { slot: Slot },
    #[error("NodeUnhealthy")]
    NodeUnhealthy { num_slots_behind: Option<Slot> },
    #[error("TransactionPrecompileVerificationFailure")]
    TransactionPrecompileVerificationFailure(solana_sdk::transaction::TransactionError),
    #[error("SlotSkipped")]
    SlotSkipped { slot: Slot },
    #[error("NoSnapshot")]
    NoSnapshot,
    #[error("LongTermStorageSlotSkipped")]
    LongTermStorageSlotSkipped { slot: Slot },
    #[error("KeyExcludedFromSecondaryIndex")]
    KeyExcludedFromSecondaryIndex { index_key: String },
    #[error("TransactionHistoryNotAvailable")]
    TransactionHistoryNotAvailable,
    #[error("ScanError")]
    ScanError { message: String },
    #[error("TransactionSignatureLenMismatch")]
    TransactionSignatureLenMismatch,
    #[error("BlockStatusNotAvailableYet")]
    BlockStatusNotAvailableYet { slot: Slot },
    #[error("UnsupportedTransactionVersion")]
    UnsupportedTransactionVersion(u8),
    #[error("MinContextSlotNotReached")]
    MinContextSlotNotReached { context_slot: Slot },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeUnhealthyErrorData {
    pub num_slots_behind: Option<Slot>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MinContextSlotNotReachedErrorData {
    pub context_slot: Slot,
}

impl From<EncodeError> for RpcCustomError {
    fn from(err: EncodeError) -> Self {
        match err {
            EncodeError::UnsupportedTransactionVersion(version) => {
                Self::UnsupportedTransactionVersion(version)
            }
        }
    }
}

impl From<RpcCustomError> for Error {
    fn from(e: RpcCustomError) -> Self {
        e.into()
    }
}

impl From<RpcCustomError> for ErrorObject<'_> {
    fn from(e: RpcCustomError) -> Self {
        match e {
            RpcCustomError::BlockCleanedUp {
                slot,
                first_available_block,
            } => Self::owned(
                JSON_RPC_SERVER_ERROR_BLOCK_CLEANED_UP,
                format!(
                    "Block {slot} cleaned up, does not exist on node. First available block: {first_available_block}",
                ),
                None::<()>,
            ),  
            RpcCustomError::SendTransactionPreflightFailure { message, result } => Self::owned(
                JSON_RPC_SERVER_ERROR_SEND_TRANSACTION_PREFLIGHT_FAILURE,
                message,
                Some(serde_json::json!(result)),
            ),
            RpcCustomError::TransactionSignatureVerificationFailure => Self::owned(
                JSON_RPC_SERVER_ERROR_TRANSACTION_SIGNATURE_VERIFICATION_FAILURE,
                "Transaction signature verification failure".to_string(),
                None::<()>,
            ),  
            RpcCustomError::BlockNotAvailable { slot } => Self::owned(
                JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE,
                format!("Block not available for slot {slot}"),
                None::<()>,
            ),  
            RpcCustomError::NodeUnhealthy { num_slots_behind } => Self::owned(
                JSON_RPC_SERVER_ERROR_NODE_UNHEALTHY,
                if let Some(num_slots_behind) = num_slots_behind {
                    format!("Node is behind by {num_slots_behind} slots")
                } else {
                    "Node is unhealthy".to_string()
                },
                Some(serde_json::json!(NodeUnhealthyErrorData {
                    num_slots_behind
                })),
            ),
            RpcCustomError::TransactionPrecompileVerificationFailure(e) => Self::owned(
                JSON_RPC_SERVER_ERROR_TRANSACTION_PRECOMPILE_VERIFICATION_FAILURE,
                format!("Transaction precompile verification failure {e:?}"),
                None::<()>,
            ),  
            RpcCustomError::SlotSkipped { slot } => Self::owned(
                JSON_RPC_SERVER_ERROR_SLOT_SKIPPED,
                format!(
                    "Slot {slot} was skipped, or missing due to ledger jump to recent snapshot"
                ),
                None::<()>,
            ),  
            RpcCustomError::NoSnapshot => Self::owned(
                JSON_RPC_SERVER_ERROR_NO_SNAPSHOT,
                "No snapshot".to_string(),
                None::<()>,
            ),  
            RpcCustomError::LongTermStorageSlotSkipped { slot } => Self::owned(
                JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED,
                format!("Slot {slot} was skipped, or missing in long-term storage"),
                None::<()>,
            ),  
            RpcCustomError::KeyExcludedFromSecondaryIndex { index_key } => Self::owned(
                JSON_RPC_SERVER_ERROR_KEY_EXCLUDED_FROM_SECONDARY_INDEX,
                format!(
                    "{index_key} excluded from account secondary indexes; \
                    this RPC method unavailable for key"
                ),
                None::<()>,
            ),  
            RpcCustomError::TransactionHistoryNotAvailable => Self::owned(
                JSON_RPC_SERVER_ERROR_TRANSACTION_HISTORY_NOT_AVAILABLE,
                "Transaction history is not available from this node".to_string(),
                None::<()>,
            ),  
            RpcCustomError::ScanError { message } => Self::owned(
                JSON_RPC_SCAN_ERROR,
                message,
                None::<()>,
            ),  
            RpcCustomError::TransactionSignatureLenMismatch => Self::owned(
                JSON_RPC_SERVER_ERROR_TRANSACTION_SIGNATURE_LEN_MISMATCH,
                "Transaction signature length mismatch".to_string(),
                None::<()>,
            ),  
            RpcCustomError::BlockStatusNotAvailableYet { slot } => Self::owned(
                JSON_RPC_SERVER_ERROR_BLOCK_STATUS_NOT_AVAILABLE_YET,
                format!("Block status not yet available for slot {slot}"),
                None::<()>,
            ),  
            RpcCustomError::UnsupportedTransactionVersion(version) => Self::owned(
                JSON_RPC_SERVER_ERROR_UNSUPPORTED_TRANSACTION_VERSION,
                format!(
                    "Transaction version ({version}) is not supported"
                ),
                None::<()>,
            ),  
            RpcCustomError::MinContextSlotNotReached { context_slot } => Self::owned(
                JSON_RPC_SERVER_ERROR_MIN_CONTEXT_SLOT_NOT_REACHED,
                "Minimum context slot has not been reached".to_string(),
                Some(serde_json::json!(MinContextSlotNotReachedErrorData {
                    context_slot,
                })),
            ),
        }
    }
}
