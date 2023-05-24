//! The `pubsub` module implements a threaded subscription service on client RPC request

use {
    crate::{
        rpc::{check_is_at_least_confirmed, Result},
        rpc_pubsub_service::PubSubConfig,
        rpc_subscription_tracker::{
            AccountSubscriptionParams, BlockSubscriptionKind, BlockSubscriptionParams,
            LogsSubscriptionKind, LogsSubscriptionParams, ProgramSubscriptionParams,
            SignatureSubscriptionParams, SubscriptionControl, SubscriptionId, SubscriptionParams,
            SubscriptionToken,
        },
    },
    dashmap::DashMap,
    jsonrpsee::types::error::ErrorCode,
    solana_account_decoder::UiAccountEncoding,
    solana_client::{
        rpc_config::{
            RpcAccountInfoConfig, RpcBlockSubscribeConfig, RpcBlockSubscribeFilter,
            RpcProgramAccountsConfig, RpcSignatureSubscribeConfig, RpcTransactionLogsConfig,
            RpcTransactionLogsFilter,
        },
        rpc_response::RpcVersionInfo,
    },
    solana_sdk::{pubkey::Pubkey, signature::Signature},
    solana_transaction_status::UiTransactionEncoding,
    std::{str::FromStr, sync::Arc},
};

// We have to keep both of the following traits to not break backwards compatibility.
// `RpcSolPubSubInternal` is actually used by the current PubSub API implementation.
// `RpcSolPubSub` and the corresponding `gen_client` module are preserved
// so the clients reliant on `gen_client::Client` do not break after this implementation is released.
//
// There are no compile-time checks that ensure coherence between traits
// so extra attention is required when adding a new method to the API.

// Suppress needless_return due to
//   https://github.com/paritytech/jsonrpc/blob/2d38e6424d8461cdf72e78425ce67d51af9c6586/derive/src/lib.rs#L204
// Once https://github.com/paritytech/jsonrpc/issues/418 is resolved, try to remove this clippy allow
#[rpc(server)]
pub trait RpcSolPubSub {
    // Get notification every time account data is changed
    // Accepts pubkey parameter as base-58 encoded string
    #[subscription(
        name = "accountSubscribe" => "accountNotification",
        item = RpcResponse<UiAccount>,
        unsubscribe = "accountUnsubscribe"
    )]
    async fn account_subscribe(&self, pubkey_str: String, config: Option<RpcAccountInfoConfig>);

    // Get notification every time account data owned by a particular program is changed
    // Accepts pubkey parameter as base-58 encoded string
    #[subscription(
        name = "programSubscribe" => "programNotification",
        item = RpcResponse<RpcKeyedAccount>,
        unsubscribe = "programUnsubscribe"
    )]
    async fn program_subscribe(&self, pubkey_str: String, config: Option<RpcProgramAccountsConfig>);

    // Get logs for all transactions that reference the specified address
    #[subscription(
        name= "logsSubscribe" => "logsNotification",
        item = RpcResponse<RpcLogsResponse>,
        unsubscribe = "logsUnsubscribe"
    )]
    async fn logs_subscribe(
        &self,
        filter: RpcTransactionLogsFilter,
        config: Option<RpcTransactionLogsConfig>,
    );

    // Get notification when signature is verified
    // Accepts signature parameter as base-58 encoded string
    #[subscription(
        name = "signatureSubscribe" => "signatureNotification", 
        unsubscribe = "signatureUnsubscribe",
        item = RpcResponse<RpcSignatureResult>
    )]
    async fn signature_subscribe(
        &self,
        signature_str: String,
        config: Option<RpcSignatureSubscribeConfig>,
    );

    // Get notification when slot is encountered
    #[subscription(
        name = "slotSubscribe" => "slotNotification",
        unsubscribe = "slotUnsubscribe",
        item = SlotInfo
    )]
    async fn slot_subscribe(&self);

    // Get series of updates for all slots
    #[subscription(
        name = "slotsUpdatesSubscribe" => "slotsUpdatesNotification",
        unsubscribe = "slotsUpdatesUnsubscribe",
        item = Arc<SlotUpdate>,
    )]
    async fn slots_updates_subscribe(&self);

    // Subscribe to block data and content
    #[subscription(
        name = "blockSubscribe" => "blockNotification",
        unsubscribe = "blockUnsubscribe",
        item = Arc<RpcBlockUpdate>
    )]
    async fn block_subscribe(
        &self,
        filter: RpcBlockSubscribeFilter,
        config: Option<RpcBlockSubscribeConfig>,
    );

    // Get notification when vote is encountered
    #[subscription(
        name = "voteSubscribe" => "voteNotification",
        unsubscribe = "voteUnsubscribe",
        item = RpcVote
    )]
    async fn vote_subscribe(&self);

    // Get notification when a new root is set
    #[subscription(
        name = "rootSubscribe" => "rootNotification",
        unsubscribe = "rootUnsubscribe",
        item = Slot
    )]
    async fn root_subscribe(&self);
}

pub use internal::RpcSolPubSubInternalServer;
use jsonrpsee::{
    core::SubscriptionResult, proc_macros::rpc, types::ErrorObject, PendingSubscriptionSink,
};

// We have to use a separate module so the code generated by different `rpc` macro invocations do not interfere with each other.
mod internal {
    use super::*;

    #[rpc(server)]
    pub trait RpcSolPubSubInternal {
        // Get the current solana version running on the node
        #[method(name = "getVersion")]
        fn get_version(&self) -> Result<RpcVersionInfo>;

        // Get notification every time account data is changed
        // Accepts pubkey parameter as base-58 encoded string
        #[subscription(
            name = "accountSubscribe" => "accountNotification",
            item = RpcResponse<UiAccount>,
            unsubscribe = "accountUnsubscribe"
        )]
        async fn account_subscribe(&self, pubkey_str: String, config: Option<RpcAccountInfoConfig>);

        // Get notification every time account data owned by a particular program is changed
        // Accepts pubkey parameter as base-58 encoded string
        #[subscription(
            name = "programSubscribe" => "programNotification",
            item = RpcResponse<RpcKeyedAccount>,
            unsubscribe = "programUnsubscribe"
        )]
        async fn program_subscribe(
            &self,
            pubkey_str: String,
            config: Option<RpcProgramAccountsConfig>,
        );

        // Get logs for all transactions that reference the specified address
        #[subscription(
            name= "logsSubscribe" => "logsNotification",
            item = RpcResponse<RpcLogsResponse>,
            unsubscribe = "logsUnsubscribe"
        )]
        async fn logs_subscribe(
            &self,
            filter: RpcTransactionLogsFilter,
            config: Option<RpcTransactionLogsConfig>,
        );

        // Get notification when signature is verified
        // Accepts signature parameter as base-58 encoded string
        #[subscription(
            name = "signatureSubscribe" => "signatureNotification", 
            unsubscribe = "signatureUnsubscribe",
            item = RpcResponse<RpcSignatureResult>
        )]
        async fn signature_subscribe(
            &self,
            signature_str: String,
            config: Option<RpcSignatureSubscribeConfig>,
        );

        // Get notification when slot is encountered
        #[subscription(
            name = "slotSubscribe" => "slotNotification",
            unsubscribe = "slotUnsubscribe",
            item = SlotInfo
        )]
        async fn slot_subscribe(&self);

        // Get series of updates for all slots
        #[subscription(
            name = "slotsUpdatesSubscribe" => "slotsUpdatesNotification",
            unsubscribe = "slotsUpdatesUnsubscribe",
            item = Arc<SlotUpdate>,
        )]
        async fn slots_updates_subscribe(&self);

        // Subscribe to block data and content
        #[subscription(
            name = "blockSubscribe" => "blockNotification",
            unsubscribe = "blockUnsubscribe",
            item = Arc<RpcBlockUpdate>
        )]
        async fn block_subscribe(
            &self,
            filter: RpcBlockSubscribeFilter,
            config: Option<RpcBlockSubscribeConfig>,
        );

        // Get notification when vote is encountered
        #[subscription(
            name = "voteSubscribe" => "voteNotification",
            unsubscribe = "voteUnsubscribe",
            item = RpcVote
        )]
        async fn vote_subscribe(&self);

        // Get notification when a new root is set
        #[subscription(
            name = "rootSubscribe" => "rootNotification",
            unsubscribe = "rootUnsubscribe",
            item = Slot
        )]
        async fn root_subscribe(&self);
    }
}

pub struct RpcSolPubSubImpl {
    config: PubSubConfig,
    subscription_control: SubscriptionControl,
    current_subscriptions: Arc<DashMap<SubscriptionId, SubscriptionToken>>,
}

impl RpcSolPubSubImpl {
    pub fn new(
        config: PubSubConfig,
        subscription_control: SubscriptionControl,
        current_subscriptions: Arc<DashMap<SubscriptionId, SubscriptionToken>>,
    ) -> Self {
        Self {
            config,
            subscription_control,
            current_subscriptions,
        }
    }

    async fn subscribe(
        &self,
        params: SubscriptionParams,
        sink: PendingSubscriptionSink,
    ) -> SubscriptionResult {
        let sink = sink.accept().await?;

        let token = self.subscription_control.subscribe(params, sink).map_err(|_| {
            ErrorObject::owned(
                ErrorCode::InternalError.code(),
                "Internal Error: Subscription refused. Node subscription limit reached".to_string(),
                None::<()>,
            )
        })?;

        let id = token.id();
        self.current_subscriptions.insert(id, token);

        Ok(())
    }

    fn unsubscribe(&self, id: SubscriptionId) -> Result<bool> {
        if self.current_subscriptions.remove(&id).is_some() {
            Ok(true)
        } else {
            Err(ErrorObject::owned(
                ErrorCode::InvalidParams.code(),
                "Invalid subscription id.".to_string(),
                None::<()>,
            ))
        }
    }
}

fn param<T: FromStr>(param_str: &str, thing: &str) -> Result<T> {
    param_str.parse::<T>().map_err(|_e| {
        ErrorObject::owned(
            ErrorCode::InternalError.code(),
            format!("Invalid Request: Invalid {thing} provided"),
            None::<()>,
        )
    })
}

impl RpcSolPubSubInternalServer for RpcSolPubSubImpl {
    fn account_subscribe(
        &self,
        sink: PendingSubscriptionSink,
        pubkey_str: String,
        config: Option<RpcAccountInfoConfig>,
    ) -> SubscriptionResult {
        let RpcAccountInfoConfig {
            encoding,
            data_slice,
            commitment,
            min_context_slot: _, // ignored
        } = config.unwrap_or_default();
        let params = AccountSubscriptionParams {
            pubkey: param::<Pubkey>(&pubkey_str, "pubkey")?,
            commitment: commitment.unwrap_or_default(),
            data_slice,
            encoding: encoding.unwrap_or(UiAccountEncoding::Binary),
        };

        self.subscribe(SubscriptionParams::Account(params), sink).await
    }

    async fn program_subscribe(
        &self,
        sink: PendingSubscriptionSink,
        pubkey_str: String,
        config: Option<RpcProgramAccountsConfig>,
    ) -> SubscriptionResult {
        let config = config.unwrap_or_default();
        let params = ProgramSubscriptionParams {
            pubkey: param::<Pubkey>(&pubkey_str, "pubkey")?,
            filters: config.filters.unwrap_or_default(),
            encoding: config
                .account_config
                .encoding
                .unwrap_or(UiAccountEncoding::Binary),
            data_slice: config.account_config.data_slice,
            commitment: config.account_config.commitment.unwrap_or_default(),
            with_context: config.with_context.unwrap_or_default(),
        };

        self.subscribe(SubscriptionParams::Program(params), sink).await
    }

    async fn logs_subscribe(
        &self,
        sink: PendingSubscriptionSink,
        filter: RpcTransactionLogsFilter,
        config: Option<RpcTransactionLogsConfig>,
    ) -> SubscriptionResult {
        let params = LogsSubscriptionParams {
            kind: match filter {
                RpcTransactionLogsFilter::All => LogsSubscriptionKind::All,
                RpcTransactionLogsFilter::AllWithVotes => LogsSubscriptionKind::AllWithVotes,
                RpcTransactionLogsFilter::Mentions(keys) => {
                    if keys.len() != 1 {
                        return Err(ErrorObject::owned(
                            ErrorCode::InvalidParams.code(),
                            "Invalid Request: Only 1 address supported".to_string(),
                            None::<()>,
                        ));
                    }
                    LogsSubscriptionKind::Single(param::<Pubkey>(&keys[0], "mentions")?)
                }
            },
            commitment: config.and_then(|c| c.commitment).unwrap_or_default(),
        };

        self.subscribe(SubscriptionParams::Logs(params), sink).await
    }

    async fn signature_subscribe(
        &self,
        sink: PendingSubscriptionSink,
        signature_str: String,
        config: Option<RpcSignatureSubscribeConfig>,
    ) -> SubscriptionResult {
        let config = config.unwrap_or_default();
        let params = SignatureSubscriptionParams {
            signature: param::<Signature>(&signature_str, "signature")?,
            commitment: config.commitment.unwrap_or_default(),
            enable_received_notification: config.enable_received_notification.unwrap_or_default(),
        };

        self.subscribe(SubscriptionParams::Signature(params), sink).await
    }

    async fn slot_subscribe(&self, sink: PendingSubscriptionSink) -> SubscriptionResult {
        self.subscribe(SubscriptionParams::Slot, sink).await
    }

    async fn slots_updates_subscribe(&self, sink: PendingSubscriptionSink) -> SubscriptionResult {
        self.subscribe(SubscriptionParams::SlotsUpdates, sink).await
    }

    async fn block_subscribe(
        &self,
        sink: PendingSubscriptionSink,
        filter: RpcBlockSubscribeFilter,
        config: Option<RpcBlockSubscribeConfig>,
    ) -> SubscriptionResult {
        if !self.config.enable_block_subscription {
            return Err(ErrorObject::from(ErrorCode::MethodNotFound));
        }
        let config = config.unwrap_or_default();
        let commitment = config.commitment.unwrap_or_default();
        check_is_at_least_confirmed(commitment)?;
        let params = BlockSubscriptionParams {
            commitment: config.commitment.unwrap_or_default(),
            encoding: config.encoding.unwrap_or(UiTransactionEncoding::Base64),
            kind: match filter {
                RpcBlockSubscribeFilter::All => BlockSubscriptionKind::All,
                RpcBlockSubscribeFilter::MentionsAccountOrProgram(key) => {
                    BlockSubscriptionKind::MentionsAccountOrProgram(param::<Pubkey>(
                        &key,
                        "mentions_account_or_program",
                    )?)
                }
            },
            transaction_details: config.transaction_details.unwrap_or_default(),
            show_rewards: config.show_rewards.unwrap_or_default(),
            max_supported_transaction_version: config.max_supported_transaction_version,
        };

        self.subscribe(SubscriptionParams::Block, sink).await
    }

    async fn vote_subscribe(&self, sink: PendingSubscriptionSink) -> Result<SubscriptionId> {
        if !self.config.enable_vote_subscription {
            return Err(ErrorObject::from(ErrorCode::MethodNotFound));
        }
        self.subscribe(SubscriptionParams::Vote, sink).await
    }

    async fn root_subscribe(&self, sink: PendingSubscriptionSink) -> Result<SubscriptionId> {
        self.subscribe(SubscriptionParams::Root, sink).await
    }

    fn get_version(&self, sink: PendingSubscriptionSink) -> Result<RpcVersionInfo> {
        let version = solana_version::Version::default();
        Ok(RpcVersionInfo {
            solana_core: version.to_string(),
            feature_set: Some(version.feature_set),
        })
    }
}

#[cfg(test)]
mod tests {

    use jsonrpsee::{types::response::Response, RpcModule};
    use serde_json::Value;
    use {
        super::*,
        crate::{
            optimistically_confirmed_bank_tracker::OptimisticallyConfirmedBank, rpc_pubsub_service,
            rpc_subscriptions::RpcSubscriptions,
        },
        serial_test::serial,
        solana_account_decoder::{parse_account_data::parse_account_data, UiAccountEncoding},
        solana_client::rpc_response::{
            ProcessedSignatureResult, ReceivedSignatureResult, RpcSignatureResult, SlotInfo,
        },
        solana_runtime::{
            bank::Bank,
            bank_forks::BankForks,
            commitment::{BlockCommitmentCache, CommitmentSlots},
            genesis_utils::{
                activate_all_features, create_genesis_config,
                create_genesis_config_with_vote_accounts, GenesisConfigInfo, ValidatorVoteKeypairs,
            },
            vote_transaction::VoteTransaction,
        },
        solana_sdk::{
            account::ReadableAccount,
            clock::Slot,
            commitment_config::CommitmentConfig,
            hash::Hash,
            message::Message,
            pubkey::Pubkey,
            rent::Rent,
            signature::{Keypair, Signer},
            stake::{
                self, instruction as stake_instruction,
                state::{Authorized, Lockup, StakeAuthorize, StakeState},
            },
            system_instruction, system_program, system_transaction,
            transaction::{self, Transaction},
        },
        solana_stake_program::stake_state,
        solana_vote_program::vote_state::Vote,
        std::{
            sync::{
                atomic::{AtomicBool, AtomicU64},
                RwLock,
            },
            thread::sleep,
            time::Duration,
        },
    };

    fn process_transaction_and_notify(
        bank_forks: &RwLock<BankForks>,
        tx: &Transaction,
        subscriptions: &RpcSubscriptions,
        current_slot: Slot,
    ) -> transaction::Result<()> {
        bank_forks
            .write()
            .unwrap()
            .get(current_slot)
            .unwrap()
            .process_transaction(tx)?;
        let commitment_slots = CommitmentSlots {
            slot: current_slot,
            ..CommitmentSlots::default()
        };
        subscriptions.notify_subscribers(commitment_slots);
        Ok(())
    }

    #[test]
    #[serial]
    fn test_signature_subscribe() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair: alice,
            ..
        } = create_genesis_config(10_000);
        let bob = Keypair::new();
        let bob_pubkey = bob.pubkey();
        let bank = Bank::new_for_tests(&genesis_config);
        let blockhash = bank.last_blockhash();
        let bank_forks = Arc::new(RwLock::new(BankForks::new(bank)));
        let max_complete_transaction_status_slot = Arc::new(AtomicU64::default());
        let max_complete_rewards_slot = Arc::new(AtomicU64::default());
        let rpc_subscriptions = Arc::new(RpcSubscriptions::new_for_tests(
            &Arc::new(AtomicBool::new(false)),
            max_complete_transaction_status_slot,
            max_complete_rewards_slot,
            bank_forks.clone(),
            Arc::new(RwLock::new(BlockCommitmentCache::new_for_tests())),
            OptimisticallyConfirmedBank::locked_from_bank_forks_root(&bank_forks),
        ));

        // Test signature subscriptions
        let tx = system_transaction::transfer(&alice, &bob_pubkey, 20, blockhash);

        let (rpc, mut receiver) = rpc_pubsub_service::test_connection(&rpc_subscriptions);

        rpc.signature_subscribe(
            tx.signatures[0].to_string(),
            Some(RpcSignatureSubscribeConfig {
                commitment: Some(CommitmentConfig::finalized()),
                ..RpcSignatureSubscribeConfig::default()
            }),
        )
        .unwrap();

        process_transaction_and_notify(&bank_forks, &tx, &rpc_subscriptions, 0).unwrap();

        // Test signature confirmation notification
        let response = receiver.recv();
        let expected_res =
            RpcSignatureResult::ProcessedSignature(ProcessedSignatureResult { err: None });
        let expected = json!({
            "jsonrpc": "2.0",
            "method": "signatureNotification",
            "params": {
                "result": {
                    "context": { "slot": 0 },
                    "value": expected_res,
                },
                "subscription": 0,
            }
        });

        assert_eq!(
            expected,
            serde_json::from_str::<serde_json::Value>(&response).unwrap(),
        );

        // Test "received"
        let (rpc, mut receiver) = rpc_pubsub_service::test_connection(&rpc_subscriptions);

        rpc.signature_subscribe(
            tx.signatures[0].to_string(),
            Some(RpcSignatureSubscribeConfig {
                commitment: Some(CommitmentConfig::finalized()),
                enable_received_notification: Some(true),
            }),
        )
        .unwrap();
        let received_slot = 1;
        rpc_subscriptions.notify_signatures_received((received_slot, vec![tx.signatures[0]]));

        // Test signature confirmation notification
        let response = receiver.recv();
        let expected_res =
            RpcSignatureResult::ReceivedSignature(ReceivedSignatureResult::ReceivedSignature);
        let expected = json!({
            "jsonrpc": "2.0",
            "method": "signatureNotification",
            "params": {
                "result": {
                    "context": { "slot": received_slot },
                    "value": expected_res,
                },
                "subscription": 1,
            }
        });
        assert_eq!(
            expected,
            serde_json::from_str::<serde_json::Value>(&response).unwrap(),
        );

        // Test "received" for gossip subscription
        let (rpc, mut receiver) = rpc_pubsub_service::test_connection(&rpc_subscriptions);

        rpc.signature_subscribe(
            tx.signatures[0].to_string(),
            Some(RpcSignatureSubscribeConfig {
                commitment: Some(CommitmentConfig::confirmed()),
                enable_received_notification: Some(true),
            }),
        )
        .unwrap();
        let received_slot = 2;
        rpc_subscriptions.notify_signatures_received((received_slot, vec![tx.signatures[0]]));

        // Test signature confirmation notification
        let response = receiver.recv();
        let expected_res =
            RpcSignatureResult::ReceivedSignature(ReceivedSignatureResult::ReceivedSignature);
        let expected = json!({
            "jsonrpc": "2.0",
             "method": "signatureNotification",
             "params": {
                 "result": {
                     "context": { "slot": received_slot },
                     "value": expected_res,
                 },
                 "subscription": 2,
             }
        });
        assert_eq!(
            expected,
            serde_json::from_str::<serde_json::Value>(&response).unwrap(),
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_signature_unsubscribe() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair: alice,
            ..
        } = create_genesis_config(10_000);
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let bank = Bank::new_for_tests(&genesis_config);
        let blockhash = bank.last_blockhash();
        let bank_forks = Arc::new(RwLock::new(BankForks::new(bank)));

        let mut io = RpcModule::new(());
        let max_complete_transaction_status_slot = Arc::new(AtomicU64::default());
        let max_complete_rewards_slot = Arc::new(AtomicU64::default());
        let subscriptions = Arc::new(RpcSubscriptions::default_with_bank_forks(
            max_complete_transaction_status_slot,
            max_complete_rewards_slot,
            bank_forks,
        ));
        let (rpc, _receiver) = rpc_pubsub_service::test_connection(&subscriptions);

        io.merge(rpc.into_rpc()).unwrap();

        let tx = system_transaction::transfer(&alice, &bob_pubkey, 20, blockhash);
        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"signatureSubscribe","params":["{}"]}}"#,
            tx.signatures[0]
        );
        let _ = io.raw_json_request(&req, 1).await.unwrap();

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"signatureUnsubscribe","params":[0]}"#;
        let (res, _) = io.raw_json_request(req, 1).await.unwrap();

        let expected = r#"{"jsonrpc":"2.0","result":true,"id":1}"#;
        let expected: Response<bool> = serde_json::from_str(expected).unwrap();

        let result: Response<bool> = serde_json::from_str(&res.result).unwrap();
        assert_eq!(result.payload, expected.payload);

        // Test bad parameter
        let req = r#"{"jsonrpc":"2.0","id":1,"method":"signatureUnsubscribe","params":[1]}"#;
        let (res, _) = io.raw_json_request(req, 1).await.unwrap();
        let expected = r#"{"jsonrpc":"2.0","error":{"code":-32602,"message":"Invalid subscription id."},"id":1}"#;
        let expected: Response<()> = serde_json::from_str(expected).unwrap();

        let result: Response<()> = serde_json::from_str(&res.result).unwrap();
        assert_eq!(result.payload, expected.payload);
    }

    #[test]
    #[serial]
    fn test_account_subscribe() {
        let GenesisConfigInfo {
            mut genesis_config,
            mint_keypair: alice,
            ..
        } = create_genesis_config(10_000_000_000);
        genesis_config.rent = Rent::default();
        activate_all_features(&mut genesis_config);

        let new_stake_authority = solana_sdk::pubkey::new_rand();
        let stake_authority = Keypair::new();
        let from = Keypair::new();
        let stake_account = Keypair::new();
        let stake_program_id = stake::program::id();
        let bank = Bank::new_for_tests(&genesis_config);
        let blockhash = bank.last_blockhash();
        let bank_forks = Arc::new(RwLock::new(BankForks::new(bank)));
        let bank0 = bank_forks.read().unwrap().get(0).unwrap();
        let bank1 = Bank::new_from_parent(&bank0, &Pubkey::default(), 1);
        bank_forks.write().unwrap().insert(bank1);
        let max_complete_transaction_status_slot = Arc::new(AtomicU64::default());
        let max_complete_rewards_slot = Arc::new(AtomicU64::default());
        let rpc_subscriptions = Arc::new(RpcSubscriptions::new_for_tests(
            &Arc::new(AtomicBool::new(false)),
            max_complete_transaction_status_slot,
            max_complete_rewards_slot,
            bank_forks.clone(),
            Arc::new(RwLock::new(BlockCommitmentCache::new_for_tests_with_slots(
                1, 1,
            ))),
            OptimisticallyConfirmedBank::locked_from_bank_forks_root(&bank_forks),
        ));

        let (rpc, mut receiver) = rpc_pubsub_service::test_connection(&rpc_subscriptions);

        let encoding = UiAccountEncoding::Base64;

        rpc.account_subscribe(
            stake_account.pubkey().to_string(),
            Some(RpcAccountInfoConfig {
                commitment: Some(CommitmentConfig::processed()),
                encoding: Some(encoding),
                data_slice: None,
                min_context_slot: None,
            }),
        )
        .unwrap();

        // Make sure the subscription is processed before continuing.
        let (rpc2, mut receiver2) = rpc_pubsub_service::test_connection(&rpc_subscriptions);
        rpc2.slot_subscribe().unwrap();
        rpc_subscriptions.notify_slot(1, 0, 0);
        receiver2.recv();

        let balance = {
            let bank = bank_forks.read().unwrap().working_bank();
            let rent = &bank.rent_collector().rent;
            rent.minimum_balance(StakeState::size_of())
        };

        let tx = system_transaction::transfer(&alice, &from.pubkey(), balance, blockhash);
        process_transaction_and_notify(&bank_forks, &tx, &rpc_subscriptions, 1).unwrap();
        let authorized = Authorized::auto(&stake_authority.pubkey());
        let ixs = stake_instruction::create_account(
            &from.pubkey(),
            &stake_account.pubkey(),
            &authorized,
            &Lockup::default(),
            balance,
        );
        let message = Message::new(&ixs, Some(&from.pubkey()));
        let tx = Transaction::new(&[&from, &stake_account], message, blockhash);
        process_transaction_and_notify(&bank_forks, &tx, &rpc_subscriptions, 1).unwrap();

        // Test signature confirmation notification #1
        let account = bank_forks
            .read()
            .unwrap()
            .get(1)
            .unwrap()
            .get_account(&stake_account.pubkey())
            .unwrap();
        let expected_data = account.data();
        let expected = json!({
           "jsonrpc": "2.0",
           "method": "accountNotification",
           "params": {
               "result": {
                   "context": { "slot": 1 },
                   "value": {
                       "owner": stake_program_id.to_string(),
                       "lamports": balance,
                       "data": [base64::encode(expected_data), encoding],
                       "executable": false,
                       "rentEpoch": 0,
                   },
               },
               "subscription": 0,
           }
        });

        let response = receiver.recv();
        assert_eq!(
            expected,
            serde_json::from_str::<serde_json::Value>(&response).unwrap(),
        );

        let balance = {
            let bank = bank_forks.read().unwrap().working_bank();
            let rent = &bank.rent_collector().rent;
            rent.minimum_balance(0)
        };
        let tx =
            system_transaction::transfer(&alice, &stake_authority.pubkey(), balance, blockhash);
        process_transaction_and_notify(&bank_forks, &tx, &rpc_subscriptions, 1).unwrap();
        sleep(Duration::from_millis(200));
        let ix = stake_instruction::authorize(
            &stake_account.pubkey(),
            &stake_authority.pubkey(),
            &new_stake_authority,
            StakeAuthorize::Staker,
            None,
        );
        let message = Message::new(&[ix], Some(&stake_authority.pubkey()));
        let tx = Transaction::new(&[&stake_authority], message, blockhash);
        process_transaction_and_notify(&bank_forks, &tx, &rpc_subscriptions, 1).unwrap();
        sleep(Duration::from_millis(200));

        let bank = bank_forks.read().unwrap()[1].clone();
        let account = bank.get_account(&stake_account.pubkey()).unwrap();
        assert_eq!(
            stake_state::authorized_from(&account).unwrap().staker,
            new_stake_authority
        );
    }

    #[test]
    #[serial]
    fn test_account_subscribe_with_encoding() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair: alice,
            ..
        } = create_genesis_config(10_000);

        let nonce_account = Keypair::new();
        let bank = Bank::new_for_tests(&genesis_config);
        let blockhash = bank.last_blockhash();
        let bank_forks = Arc::new(RwLock::new(BankForks::new(bank)));
        let bank0 = bank_forks.read().unwrap().get(0).unwrap();
        let bank1 = Bank::new_from_parent(&bank0, &Pubkey::default(), 1);
        bank_forks.write().unwrap().insert(bank1);
        let max_complete_transaction_status_slot = Arc::new(AtomicU64::default());
        let max_complete_rewards_slot = Arc::new(AtomicU64::default());
        let rpc_subscriptions = Arc::new(RpcSubscriptions::new_for_tests(
            &Arc::new(AtomicBool::new(false)),
            max_complete_transaction_status_slot,
            max_complete_rewards_slot,
            bank_forks.clone(),
            Arc::new(RwLock::new(BlockCommitmentCache::new_for_tests_with_slots(
                1, 1,
            ))),
            OptimisticallyConfirmedBank::locked_from_bank_forks_root(&bank_forks),
        ));

        let (rpc, mut receiver) = rpc_pubsub_service::test_connection(&rpc_subscriptions);

        rpc.account_subscribe(
            nonce_account.pubkey().to_string(),
            Some(RpcAccountInfoConfig {
                commitment: Some(CommitmentConfig::processed()),
                encoding: Some(UiAccountEncoding::JsonParsed),
                data_slice: None,
                min_context_slot: None,
            }),
        )
        .unwrap();

        // Make sure the subscription is processed before continuing.
        let (rpc2, mut receiver2) = rpc_pubsub_service::test_connection(&rpc_subscriptions);
        rpc2.slot_subscribe().unwrap();
        rpc_subscriptions.notify_slot(1, 0, 0);
        receiver2.recv();

        let ixs = system_instruction::create_nonce_account(
            &alice.pubkey(),
            &nonce_account.pubkey(),
            &alice.pubkey(),
            100,
        );
        let message = Message::new(&ixs, Some(&alice.pubkey()));
        let tx = Transaction::new(&[&alice, &nonce_account], message, blockhash);
        process_transaction_and_notify(&bank_forks, &tx, &rpc_subscriptions, 1).unwrap();

        // Test signature confirmation notification #1
        let account = bank_forks
            .read()
            .unwrap()
            .get(1)
            .unwrap()
            .get_account(&nonce_account.pubkey())
            .unwrap();
        let expected_data = account.data();
        let expected_data = parse_account_data(
            &nonce_account.pubkey(),
            &system_program::id(),
            expected_data,
            None,
        )
        .unwrap();
        let expected = json!({
           "jsonrpc": "2.0",
           "method": "accountNotification",
           "params": {
               "result": {
                   "context": { "slot": 1 },
                   "value": {
                       "owner": system_program::id().to_string(),
                       "lamports": 100,
                       "data": expected_data,
                       "executable": false,
                       "rentEpoch": 0,
                   },
               },
               "subscription": 0,
           }
        });

        let response = receiver.recv();
        assert_eq!(
            expected,
            serde_json::from_str::<serde_json::Value>(&response).unwrap(),
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_account_unsubscribe() {
        let bob_pubkey = solana_sdk::pubkey::new_rand();

        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(10_000);
        let bank_forks = Arc::new(RwLock::new(BankForks::new(Bank::new_for_tests(
            &genesis_config,
        ))));

        let max_complete_transaction_status_slot = Arc::new(AtomicU64::default());
        let max_complete_rewards_slot = Arc::new(AtomicU64::default());
        let subscriptions = Arc::new(RpcSubscriptions::default_with_bank_forks(
            max_complete_transaction_status_slot,
            max_complete_rewards_slot,
            bank_forks,
        ));
        let (rpc, _receiver) = rpc_pubsub_service::test_connection(&subscriptions);

        let io = rpc.into_rpc();

        let req = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"accountSubscribe","params":["{}"]}}"#,
            bob_pubkey
        );
        let _ = io.raw_json_request(&req, 1).await.unwrap();

        let req = r#"{"jsonrpc":"2.0","id":1,"method":"accountUnsubscribe","params":[0]}"#;
        let (res, _) = io.raw_json_request(req, 1).await.unwrap();

        let expected = r#"{"jsonrpc":"2.0","result":true,"id":1}"#;
        let expected: Response<bool> = serde_json::from_str(expected).unwrap();

        let result: Response<bool> = serde_json::from_str(&res.result).unwrap();
        assert_eq!(result.payload, expected.payload);

        // Test bad parameter
        let req = r#"{"jsonrpc":"2.0","id":1,"method":"accountUnsubscribe","params":[1]}"#;
        let (res, _) = io.raw_json_request(req, 1).await.unwrap();
        let expected = r#"{"jsonrpc":"2.0","error":{"code":-32602,"message":"Invalid subscription id."},"id":1}"#;
        let expected: Response<()> = serde_json::from_str(expected).unwrap();

        let result: Response<()> = serde_json::from_str(&res.result).unwrap();
        assert_eq!(result.payload, expected.payload);
    }

    #[test]
    #[should_panic]
    fn test_account_commitment_not_fulfilled() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair: alice,
            ..
        } = create_genesis_config(10_000);
        let bank = Bank::new_for_tests(&genesis_config);
        let blockhash = bank.last_blockhash();
        let bank_forks = Arc::new(RwLock::new(BankForks::new(bank)));
        let bob = Keypair::new();

        let exit = Arc::new(AtomicBool::new(false));
        let max_complete_transaction_status_slot = Arc::new(AtomicU64::default());
        let max_complete_rewards_slot = Arc::new(AtomicU64::default());
        let rpc_subscriptions = Arc::new(RpcSubscriptions::new_for_tests(
            &exit,
            max_complete_transaction_status_slot,
            max_complete_rewards_slot,
            bank_forks.clone(),
            Arc::new(RwLock::new(BlockCommitmentCache::new_for_tests())),
            OptimisticallyConfirmedBank::locked_from_bank_forks_root(&bank_forks),
        ));

        let (rpc, mut receiver) = rpc_pubsub_service::test_connection(&rpc_subscriptions);

        rpc.account_subscribe(
            bob.pubkey().to_string(),
            Some(RpcAccountInfoConfig {
                commitment: Some(CommitmentConfig::finalized()),
                encoding: None,
                data_slice: None,
                min_context_slot: None,
            }),
        )
        .unwrap();

        let tx = system_transaction::transfer(&alice, &bob.pubkey(), 100, blockhash);
        bank_forks
            .write()
            .unwrap()
            .get(1)
            .unwrap()
            .process_transaction(&tx)
            .unwrap();
        rpc_subscriptions.notify_subscribers(CommitmentSlots::default());

        // allow 200ms for notification thread to wake
        std::thread::sleep(Duration::from_millis(200));
        let _panic = receiver.recv();
    }

    #[test]
    fn test_account_commitment() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair: alice,
            ..
        } = create_genesis_config(10_000);
        let bank = Bank::new_for_tests(&genesis_config);
        let blockhash = bank.last_blockhash();
        let bank_forks = Arc::new(RwLock::new(BankForks::new(bank)));
        let bank0 = bank_forks.read().unwrap().get(0).unwrap();
        let bank1 = Bank::new_from_parent(&bank0, &Pubkey::default(), 1);
        bank_forks.write().unwrap().insert(bank1);
        let bob = Keypair::new();

        let exit = Arc::new(AtomicBool::new(false));
        let block_commitment_cache = Arc::new(RwLock::new(BlockCommitmentCache::new_for_tests()));
        let max_complete_transaction_status_slot = Arc::new(AtomicU64::default());
        let max_complete_rewards_slot = Arc::new(AtomicU64::default());
        let subscriptions = Arc::new(RpcSubscriptions::new_for_tests(
            &exit,
            max_complete_transaction_status_slot,
            max_complete_rewards_slot,
            bank_forks.clone(),
            block_commitment_cache,
            OptimisticallyConfirmedBank::locked_from_bank_forks_root(&bank_forks),
        ));
        let (rpc, mut receiver) = rpc_pubsub_service::test_connection(&subscriptions);

        rpc.account_subscribe(
            bob.pubkey().to_string(),
            Some(RpcAccountInfoConfig {
                commitment: Some(CommitmentConfig::finalized()),
                encoding: None,
                data_slice: None,
                min_context_slot: None,
            }),
        )
        .unwrap();

        let tx = system_transaction::transfer(&alice, &bob.pubkey(), 100, blockhash);
        bank_forks
            .write()
            .unwrap()
            .get(1)
            .unwrap()
            .process_transaction(&tx)
            .unwrap();
        let commitment_slots = CommitmentSlots {
            slot: 1,
            ..CommitmentSlots::default()
        };
        subscriptions.notify_subscribers(commitment_slots);

        let commitment_slots = CommitmentSlots {
            slot: 2,
            root: 1,
            highest_confirmed_slot: 1,
            highest_confirmed_root: 1,
        };
        subscriptions.notify_subscribers(commitment_slots);
        let expected = json!({
           "jsonrpc": "2.0",
           "method": "accountNotification",
           "params": {
               "result": {
                   "context": { "slot": 1 },
                   "value": {
                       "owner": system_program::id().to_string(),
                       "lamports": 100,
                       "data": "",
                       "executable": false,
                       "rentEpoch": 0,
                   },
               },
               "subscription": 0,
           }
        });
        let response = receiver.recv();
        assert_eq!(
            expected,
            serde_json::from_str::<serde_json::Value>(&response).unwrap(),
        );
    }

    #[test]
    #[serial]
    fn test_slot_subscribe() {
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(10_000);
        let bank = Bank::new_for_tests(&genesis_config);
        let bank_forks = Arc::new(RwLock::new(BankForks::new(bank)));
        let max_complete_transaction_status_slot = Arc::new(AtomicU64::default());
        let max_complete_rewards_slot = Arc::new(AtomicU64::default());
        let rpc_subscriptions = Arc::new(RpcSubscriptions::default_with_bank_forks(
            max_complete_transaction_status_slot,
            max_complete_rewards_slot,
            bank_forks,
        ));
        let (rpc, mut receiver) = rpc_pubsub_service::test_connection(&rpc_subscriptions);
        rpc.slot_subscribe().unwrap();

        rpc_subscriptions.notify_slot(0, 0, 0);

        // Test slot confirmation notification
        let response: Value = serde_json::from_str(&receiver.recv()).unwrap();

        let expected = json! {{
            "jsonrpc":"2.0",
            "method":"slotNotification",
            "params":{
                "result": SlotInfo {
                    parent: 0,
                    slot: 0,
                    root: 0,
                },
                "subscription": 0
            }
        }};

        assert_eq!(expected, response);
    }

    #[test]
    #[serial]
    fn test_slot_unsubscribe() {
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(10_000);
        let bank = Bank::new_for_tests(&genesis_config);
        let bank_forks = Arc::new(RwLock::new(BankForks::new(bank)));
        let max_complete_transaction_status_slot = Arc::new(AtomicU64::default());
        let max_complete_rewards_slot = Arc::new(AtomicU64::default());
        let rpc_subscriptions = Arc::new(RpcSubscriptions::default_with_bank_forks(
            max_complete_transaction_status_slot,
            max_complete_rewards_slot,
            bank_forks,
        ));
        let (rpc, mut receiver) = rpc_pubsub_service::test_connection(&rpc_subscriptions);
        let sub_id = rpc.slot_subscribe().unwrap();

        rpc_subscriptions.notify_slot(0, 0, 0);

        let response: Value = serde_json::from_str(&receiver.recv()).unwrap();

        let expected = json! {{
            "jsonrpc": "2.0",
            "method":"slotNotification",
            "params": {
                "result": {
                    "parent": 0,
                    "slot": 0,
                    "root": 0,
                },
                "subscription": 0
            }
        }};

        assert_eq!(expected, response);

        assert!(rpc.slot_unsubscribe(42.into()).is_err());
        assert!(rpc.slot_unsubscribe(sub_id).is_ok());
    }

    #[test]
    #[serial]
    fn test_vote_subscribe() {
        let block_commitment_cache = Arc::new(RwLock::new(BlockCommitmentCache::new_for_tests()));

        let validator_voting_keypairs: Vec<_> =
            (0..10).map(|_| ValidatorVoteKeypairs::new_rand()).collect();
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config_with_vote_accounts(
            10_000,
            &validator_voting_keypairs,
            vec![100; validator_voting_keypairs.len()],
        );
        let exit = Arc::new(AtomicBool::new(false));
        let bank = Bank::new_for_tests(&genesis_config);
        let bank_forks = Arc::new(RwLock::new(BankForks::new(bank)));

        // Setup Subscriptions
        let optimistically_confirmed_bank =
            OptimisticallyConfirmedBank::locked_from_bank_forks_root(&bank_forks);
        let max_complete_transaction_status_slot = Arc::new(AtomicU64::default());
        let max_complete_rewards_slot = Arc::new(AtomicU64::default());
        let subscriptions = Arc::new(RpcSubscriptions::new_for_tests(
            &exit,
            max_complete_transaction_status_slot,
            max_complete_rewards_slot,
            bank_forks,
            block_commitment_cache,
            optimistically_confirmed_bank,
        ));
        // Setup RPC
        let (rpc, mut receiver) = rpc_pubsub_service::test_connection(&subscriptions);
        rpc.vote_subscribe().unwrap();

        let vote = Vote {
            slots: vec![1, 2],
            hash: Hash::default(),
            timestamp: None,
        };
        subscriptions.notify_vote(
            Pubkey::default(),
            VoteTransaction::from(vote),
            Signature::default(),
        );

        let response: Value = serde_json::from_str(&receiver.recv()).unwrap();
        let expected = json! {{
            "jsonrpc":"2.0",
            "method":"voteNotification",
            "params": {
                "result": {
                    "votePubkey":"11111111111111111111111111111111",
                    "slots":[1,2],
                    "hash":"11111111111111111111111111111111",
                    "timestamp":null,
                    "signature":"1111111111111111111111111111111111111111111111111111111111111111"
                },
                "subscription":0
            }
        }};

        assert_eq!(response, expected);
    }

    #[test]
    #[serial]
    fn test_vote_unsubscribe() {
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(10_000);
        let bank = Bank::new_for_tests(&genesis_config);
        let bank_forks = Arc::new(RwLock::new(BankForks::new(bank)));
        let max_complete_transaction_status_slot = Arc::new(AtomicU64::default());
        let max_complete_rewards_slot = Arc::new(AtomicU64::default());
        let rpc_subscriptions = Arc::new(RpcSubscriptions::default_with_bank_forks(
            max_complete_transaction_status_slot,
            max_complete_rewards_slot,
            bank_forks,
        ));
        let (rpc, _receiver) = rpc_pubsub_service::test_connection(&rpc_subscriptions);
        let sub_id = rpc.vote_subscribe().unwrap();

        assert!(rpc.vote_unsubscribe(42.into()).is_err());
        assert!(rpc.vote_unsubscribe(sub_id).is_ok());
    }

    #[test]
    fn test_get_version() {
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(10_000);
        let bank = Bank::new_for_tests(&genesis_config);
        let bank_forks = Arc::new(RwLock::new(BankForks::new(bank)));
        let max_complete_transaction_status_slot = Arc::new(AtomicU64::default());
        let max_complete_rewards_slot = Arc::new(AtomicU64::default());
        let rpc_subscriptions = Arc::new(RpcSubscriptions::default_with_bank_forks(
            max_complete_transaction_status_slot,
            max_complete_rewards_slot,
            bank_forks,
        ));
        let (rpc, _receiver) = rpc_pubsub_service::test_connection(&rpc_subscriptions);
        let version = rpc.get_version().unwrap();
        let expected_version = solana_version::Version::default();
        assert_eq!(version.to_string(), expected_version.to_string());
        assert_eq!(version.feature_set.unwrap(), expected_version.feature_set);
    }
}
