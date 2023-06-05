//! The `rpc_service` module implements the Solana JSON RPC service.

use std::{str::FromStr, time::Duration};

use futures_util::TryStreamExt;
use jsonrpc_actix::types::error::{code::ErrorCode, object::ErrorObject};
use jsonrpsee::{
    server::{ServerBuilder, ServerHandle},
    RpcModule,
};
use solana_account_decoder::{parse_token::is_known_spl_token_id, UiAccountEncoding};
use solana_rpc_client_api::{
    config::{
        RpcAccountInfoConfig, RpcBlockConfig, RpcBlocksConfigWrapper, RpcContextConfig,
        RpcEncodingConfigWrapper, RpcProgramAccountsConfig, RpcSendTransactionConfig,
        RpcSimulateTransactionConfig,
    },
    custom_error::RpcCustomError,
    filter::RpcFilterType,
    request::MAX_GET_PROGRAM_ACCOUNT_FILTERS,
    response::{OptionalContext, RpcBlockhash, RpcKeyedAccount, RpcSimulateTransactionResult},
};
use solana_runtime::bank::TransactionSimulationResult;
use solana_sdk::{
    clock::MAX_RECENT_BLOCKHASHES,
    commitment_config::CommitmentConfig,
    slot_history::Slot,
    transaction::{TransactionError, VersionedTransaction},
};
use solana_transaction_status::{UiConfirmedBlock, UiTransactionEncoding};
use tower_http::cors::MaxAge;

use crate::{
    parsed_token_accounts::get_parsed_token_accounts,
    request_middleware::{RequestMiddleware, RequestMiddlewareAction, RequestMiddlewareLayer},
};

use {
    crate::{
        cluster_tpu_info::ClusterTpuInfo,
        max_slots::MaxSlots,
        optimistically_confirmed_bank_tracker::OptimisticallyConfirmedBank,
        rpc::{
            rpc_accounts::*, rpc_accounts_scan::*, rpc_bank::*, rpc_deprecated_v1_7::*,
            rpc_deprecated_v1_9::*, rpc_full::*, rpc_minimal::*, rpc_obsolete_v1_7::*, *,
        },
        rpc_cache::LargestAccountsCache,
        rpc_health::*,
    },
    crossbeam_channel::unbounded,
    regex::Regex,
    solana_client::connection_cache::ConnectionCache,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::{
        bigtable_upload::ConfirmedBlockUploadConfig,
        bigtable_upload_service::BigTableUploadService, blockstore::Blockstore,
        leader_schedule_cache::LeaderScheduleCache,
    },
    solana_metrics::inc_new_counter_info,
    solana_perf::thread::renice_this_thread,
    solana_poh::poh_recorder::PohRecorder,
    solana_runtime::{
        bank_forks::BankForks, commitment::BlockCommitmentCache,
        prioritization_fee_cache::PrioritizationFeeCache,
        snapshot_archive_info::SnapshotArchiveInfoGetter, snapshot_config::SnapshotConfig,
        snapshot_utils,
    },
    solana_sdk::{
        exit::Exit, genesis_config::DEFAULT_GENESIS_DOWNLOAD_PATH, hash::Hash,
        native_token::lamports_to_sol, pubkey::Pubkey,
    },
    solana_send_transaction_service::send_transaction_service::{self, SendTransactionService},
    solana_storage_bigtable::CredentialType,
    std::{
        collections::HashSet,
        net::SocketAddr,
        path::{Path, PathBuf},
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc, RwLock,
        },
        thread,
    },
    tokio_util::codec::{BytesCodec, FramedRead},
};

const FULL_SNAPSHOT_REQUEST_PATH: &str = "/snapshot.tar.bz2";
const INCREMENTAL_SNAPSHOT_REQUEST_PATH: &str = "/incremental-snapshot.tar.bz2";
const LARGEST_ACCOUNTS_CACHE_DURATION: u64 = 60 * 60 * 2;

pub struct JsonRpcService {
    thread_hdl: thread::JoinHandle<()>,

    #[cfg(test)]
    pub request_processor: JsonRpcRequestProcessor, // Used only by test_rpc_new()...

    close_handle: Option<()>,
}

struct RpcRequestMiddleware {
    ledger_path: PathBuf,
    full_snapshot_archive_path_regex: Regex,
    incremental_snapshot_archive_path_regex: Regex,
    snapshot_config: Option<SnapshotConfig>,
    bank_forks: Arc<RwLock<BankForks>>,
    health: Arc<RpcHealth>,
}

impl RpcRequestMiddleware {
    pub fn new(
        ledger_path: PathBuf,
        snapshot_config: Option<SnapshotConfig>,
        bank_forks: Arc<RwLock<BankForks>>,
        health: Arc<RpcHealth>,
    ) -> Self {
        Self {
            ledger_path,
            full_snapshot_archive_path_regex: Regex::new(
                snapshot_utils::FULL_SNAPSHOT_ARCHIVE_FILENAME_REGEX,
            )
            .unwrap(),
            incremental_snapshot_archive_path_regex: Regex::new(
                snapshot_utils::INCREMENTAL_SNAPSHOT_ARCHIVE_FILENAME_REGEX,
            )
            .unwrap(),
            snapshot_config,
            bank_forks,
            health,
        }
    }

    fn redirect(location: &str) -> hyper::Response<hyper::Body> {
        hyper::Response::builder()
            .status(hyper::StatusCode::SEE_OTHER)
            .header(hyper::header::LOCATION, location)
            .body(hyper::Body::from(String::from(location)))
            .unwrap()
    }

    fn not_found() -> hyper::Response<hyper::Body> {
        hyper::Response::builder()
            .status(hyper::StatusCode::NOT_FOUND)
            .body(hyper::Body::empty())
            .unwrap()
    }

    #[allow(dead_code)]
    fn internal_server_error() -> hyper::Response<hyper::Body> {
        hyper::Response::builder()
            .status(hyper::StatusCode::INTERNAL_SERVER_ERROR)
            .body(hyper::Body::empty())
            .unwrap()
    }

    fn strip_leading_slash(path: &str) -> Option<&str> {
        path.strip_prefix('/')
    }

    fn is_file_get_path(&self, path: &str) -> bool {
        if path == DEFAULT_GENESIS_DOWNLOAD_PATH {
            return true;
        }

        if self.snapshot_config.is_none() {
            return false;
        }

        let Some(path) = Self::strip_leading_slash(path) else {
            return false;
        };

        self.full_snapshot_archive_path_regex.is_match(path)
            || self.incremental_snapshot_archive_path_regex.is_match(path)
    }

    #[cfg(unix)]
    async fn open_no_follow(path: impl AsRef<Path>) -> std::io::Result<tokio::fs::File> {
        tokio::fs::OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
            .await
    }

    #[cfg(not(unix))]
    async fn open_no_follow(path: impl AsRef<Path>) -> std::io::Result<tokio::fs::File> {
        // TODO: Is there any way to achieve the same on Windows?
        tokio::fs::File::open(path).await
    }

    fn find_snapshot_file<P>(&self, stem: P) -> PathBuf
    where
        P: AsRef<Path>,
    {
        let root = if self
            .full_snapshot_archive_path_regex
            .is_match(Path::new("").join(&stem).to_str().unwrap())
        {
            &self
                .snapshot_config
                .as_ref()
                .unwrap()
                .full_snapshot_archives_dir
        } else {
            &self
                .snapshot_config
                .as_ref()
                .unwrap()
                .incremental_snapshot_archives_dir
        };
        let local_path = root.join(&stem);
        if local_path.exists() {
            local_path
        } else {
            // remote snapshot archive path
            snapshot_utils::build_snapshot_archives_remote_dir(root).join(stem)
        }
    }

    fn process_file_get(&self, path: &str) -> RequestMiddlewareAction {
        let filename = {
            let stem = Self::strip_leading_slash(path).expect("path already verified");
            match path {
                DEFAULT_GENESIS_DOWNLOAD_PATH => {
                    inc_new_counter_info!("rpc-get_genesis", 1);
                    self.ledger_path.join(stem)
                }
                _ => {
                    inc_new_counter_info!("rpc-get_snapshot", 1);
                    self.find_snapshot_file(stem)
                }
            }
        };

        let file_length = std::fs::metadata(&filename)
            .map(|m| m.len())
            .unwrap_or(0)
            .to_string();

        info!("get {} -> {:?} ({} bytes)", path, filename, file_length);

        RequestMiddlewareAction::Respond(Box::pin(async {
            match Self::open_no_follow(filename).await {
                Err(err) => Ok(if err.kind() == std::io::ErrorKind::NotFound {
                    Self::not_found()
                } else {
                    Self::internal_server_error()
                }),
                Ok(file) => {
                    let stream = FramedRead::new(file, BytesCodec::new()).map_ok(|b| b.freeze());
                    let body = hyper::Body::wrap_stream(stream);

                    Ok(hyper::Response::builder()
                        .header(hyper::header::CONTENT_LENGTH, file_length)
                        .body(body)
                        .unwrap())
                }
            }
        }))
    }

    fn health_check(&self) -> &'static str {
        let response = match self.health.check() {
            RpcHealthStatus::Ok => "ok",
            RpcHealthStatus::Behind { .. } => "behind",
            RpcHealthStatus::Unknown => "unknown",
        };
        info!("health check: {}", response);
        response
    }
}

impl RequestMiddleware for RpcRequestMiddleware {
    fn on_request(&self, req: hyper::Request<hyper::Body>) -> RequestMiddlewareAction {
        trace!("request uri: {}", req.uri());

        if let Some(ref snapshot_config) = self.snapshot_config {
            if req.uri().path() == FULL_SNAPSHOT_REQUEST_PATH
                || req.uri().path() == INCREMENTAL_SNAPSHOT_REQUEST_PATH
            {
                // Convenience redirect to the latest snapshot
                let full_snapshot_archive_info =
                    snapshot_utils::get_highest_full_snapshot_archive_info(
                        &snapshot_config.full_snapshot_archives_dir,
                    );
                let snapshot_archive_info =
                    if let Some(full_snapshot_archive_info) = full_snapshot_archive_info {
                        if req.uri().path() == FULL_SNAPSHOT_REQUEST_PATH {
                            Some(full_snapshot_archive_info.snapshot_archive_info().clone())
                        } else {
                            snapshot_utils::get_highest_incremental_snapshot_archive_info(
                                &snapshot_config.incremental_snapshot_archives_dir,
                                full_snapshot_archive_info.slot(),
                            )
                            .map(|incremental_snapshot_archive_info| {
                                incremental_snapshot_archive_info
                                    .snapshot_archive_info()
                                    .clone()
                            })
                        }
                    } else {
                        None
                    };
                return if let Some(snapshot_archive_info) = snapshot_archive_info {
                    RpcRequestMiddleware::redirect(&format!(
                        "/{}",
                        snapshot_archive_info
                            .path
                            .file_name()
                            .unwrap_or_else(|| std::ffi::OsStr::new(""))
                            .to_str()
                            .unwrap_or("")
                    ))
                } else {
                    RpcRequestMiddleware::not_found()
                }
                .into();
            }
        }

        if let Some(result) = process_rest(&self.bank_forks, req.uri().path()) {
            hyper::Response::builder()
                .status(hyper::StatusCode::OK)
                .body(hyper::Body::from(result))
                .unwrap()
                .into()
        } else if self.is_file_get_path(req.uri().path()) {
            self.process_file_get(req.uri().path())
        } else if req.uri().path() == "/health" {
            hyper::Response::builder()
                .status(hyper::StatusCode::OK)
                .body(hyper::Body::from(self.health_check()))
                .unwrap()
                .into()
        } else {
            req.into()
        }
    }
}

fn process_rest(bank_forks: &Arc<RwLock<BankForks>>, path: &str) -> Option<String> {
    match path {
        "/v0/circulating-supply" => {
            let bank = bank_forks.read().unwrap().root_bank();
            let total_supply = bank.capitalization();
            let non_circulating_supply =
                solana_runtime::non_circulating_supply::calculate_non_circulating_supply(&bank)
                    .expect("Scan should not error on root banks")
                    .lamports;
            Some(format!(
                "{}",
                lamports_to_sol(total_supply - non_circulating_supply)
            ))
        }
        "/v0/total-supply" => {
            let bank = bank_forks.read().unwrap().root_bank();
            let total_supply = bank.capitalization();
            Some(format!("{}", lamports_to_sol(total_supply)))
        }
        _ => None,
    }
}

impl JsonRpcService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        rpc_addr: SocketAddr,
        config: JsonRpcConfig,
        snapshot_config: Option<SnapshotConfig>,
        bank_forks: Arc<RwLock<BankForks>>,
        block_commitment_cache: Arc<RwLock<BlockCommitmentCache>>,
        blockstore: Arc<Blockstore>,
        cluster_info: Arc<ClusterInfo>,
        poh_recorder: Option<Arc<RwLock<PohRecorder>>>,
        genesis_hash: Hash,
        ledger_path: &Path,
        validator_exit: Arc<RwLock<Exit>>,
        exit: Arc<AtomicBool>,
        known_validators: Option<HashSet<Pubkey>>,
        override_health_check: Arc<AtomicBool>,
        startup_verification_complete: Arc<AtomicBool>,
        optimistically_confirmed_bank: Arc<RwLock<OptimisticallyConfirmedBank>>,
        send_transaction_service_config: send_transaction_service::Config,
        max_slots: Arc<MaxSlots>,
        leader_schedule_cache: Arc<LeaderScheduleCache>,
        connection_cache: Arc<ConnectionCache>,
        max_complete_transaction_status_slot: Arc<AtomicU64>,
        max_complete_rewards_slot: Arc<AtomicU64>,
        prioritization_fee_cache: Arc<PrioritizationFeeCache>,
    ) -> std::result::Result<Self, String> {
        info!("rpc bound to {:?}", rpc_addr);
        info!("rpc configuration: {:?}", config);
        let rpc_threads = 1.max(config.rpc_threads);
        let rpc_niceness_adj = config.rpc_niceness_adj;

        let health = Arc::new(RpcHealth::new(
            cluster_info.clone(),
            known_validators,
            config.health_check_slot_distance,
            override_health_check,
            startup_verification_complete,
        ));

        let largest_accounts_cache = Arc::new(RwLock::new(LargestAccountsCache::new(
            LARGEST_ACCOUNTS_CACHE_DURATION,
        )));

        let tpu_address = cluster_info
            .my_contact_info()
            .tpu(connection_cache.protocol())
            .map_err(|err| format!("{err}"))?;

        // sadly, some parts of our current rpc implemention block the jsonrpc's
        // _socket-listening_ event loop for too long, due to (blocking) long IO or intesive CPU,
        // causing no further processing of incoming requests and ultimatily innocent clients timing-out.
        // So create a (shared) multi-threaded event_loop for jsonrpc and set its .threads() to 1,
        // so that we avoid the single-threaded event loops from being created automatically by
        // jsonrpc for threads when .threads(N > 1) is given.
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(rpc_threads)
                .on_thread_start(move || renice_this_thread(rpc_niceness_adj).unwrap())
                .thread_name("solRpcEl")
                .enable_all()
                .build()
                .expect("Runtime"),
        );

        let exit_bigtable_ledger_upload_service = Arc::new(AtomicBool::new(false));

        let (bigtable_ledger_storage, _bigtable_ledger_upload_service) =
            if let Some(RpcBigtableConfig {
                enable_bigtable_ledger_upload,
                ref bigtable_instance_name,
                ref bigtable_app_profile_id,
                timeout,
            }) = config.rpc_bigtable_config
            {
                let bigtable_config = solana_storage_bigtable::LedgerStorageConfig {
                    read_only: !enable_bigtable_ledger_upload,
                    timeout,
                    credential_type: CredentialType::Filepath(None),
                    instance_name: bigtable_instance_name.clone(),
                    app_profile_id: bigtable_app_profile_id.clone(),
                };
                runtime
                    .block_on(solana_storage_bigtable::LedgerStorage::new_with_config(
                        bigtable_config,
                    ))
                    .map(|bigtable_ledger_storage| {
                        info!("BigTable ledger storage initialized");

                        let bigtable_ledger_upload_service = if enable_bigtable_ledger_upload {
                            Some(Arc::new(BigTableUploadService::new_with_config(
                                runtime.clone(),
                                bigtable_ledger_storage.clone(),
                                blockstore.clone(),
                                block_commitment_cache.clone(),
                                max_complete_transaction_status_slot.clone(),
                                max_complete_rewards_slot.clone(),
                                ConfirmedBlockUploadConfig::default(),
                                exit_bigtable_ledger_upload_service.clone(),
                            )))
                        } else {
                            None
                        };

                        (
                            Some(bigtable_ledger_storage),
                            bigtable_ledger_upload_service,
                        )
                    })
                    .unwrap_or_else(|err| {
                        error!("Failed to initialize BigTable ledger storage: {:?}", err);
                        (None, None)
                    })
            } else {
                (None, None)
            };

        let full_api = config.full_api;
        let obsolete_v1_7_api = config.obsolete_v1_7_api;
        let max_request_body_size = config
            .max_request_body_size
            .unwrap_or(MAX_REQUEST_BODY_SIZE);

        let (request_processor, receiver) = JsonRpcRequestProcessor::new(
            config,
            snapshot_config.clone(),
            bank_forks.clone(),
            block_commitment_cache,
            blockstore,
            validator_exit.clone(),
            health.clone(),
            cluster_info.clone(),
            genesis_hash,
            bigtable_ledger_storage,
            optimistically_confirmed_bank,
            largest_accounts_cache,
            max_slots,
            leader_schedule_cache,
            max_complete_transaction_status_slot,
            max_complete_rewards_slot,
            prioritization_fee_cache,
        );

        let leader_info =
            poh_recorder.map(|recorder| ClusterTpuInfo::new(cluster_info.clone(), recorder));
        let _send_transaction_service = Arc::new(SendTransactionService::new_with_config(
            tpu_address,
            &bank_forks,
            leader_info,
            receiver,
            &connection_cache,
            send_transaction_service_config,
            exit,
        ));

        #[cfg(test)]
        let test_request_processor = request_processor.clone();

        let ledger_path = ledger_path.to_path_buf();

        let (close_handle_sender, close_handle_receiver) = unbounded();

        let thread_hdl = thread::Builder::new()
            .name("solJsonRpcSvc".to_string())
            .spawn(move || {
                renice_this_thread(rpc_niceness_adj).unwrap();

                runtime.block_on(async move {
                    let meta = request_processor.clone();

                    {
                        use actix_web::{web, App, HttpServer};
                        use jsonrpc_actix::{
                            handle::rpc_handler,
                            methods::{RpcModule, RpcResult},
                        };
                        use solana_rpc_client_api::response::{
                            Response as RpcResponse, RpcVersionInfo,
                        };

                        async fn get_version(
                            _ctx: JsonRpcRequestProcessor,
                        ) -> RpcResult<RpcVersionInfo> {
                            debug!("get_version rpc request received");
                            let version = solana_version::Version::default();
                            Ok(RpcVersionInfo {
                                solana_core: version.to_string(),
                                feature_set: Some(version.feature_set),
                            })
                        }

                        async fn get_slot(
                            ctx: JsonRpcRequestProcessor,
                            config: RpcContextConfig,
                        ) -> RpcResult<Slot> {
                            let bank = ctx.get_bank_with_config(config)?;
                            Ok(bank.slot())
                        }

                        async fn get_block_height(
                            ctx: JsonRpcRequestProcessor,
                            config: RpcContextConfig,
                        ) -> RpcResult<u64> {
                            let bank = ctx.get_bank_with_config(config)?;
                            Ok(bank.block_height())
                        }

                        async fn get_latest_blockhash(
                            ctx: JsonRpcRequestProcessor,
                            config: Option<RpcContextConfig>,
                        ) -> RpcResult<RpcResponse<RpcBlockhash>> {
                            debug!("get_latest_blockhash rpc request received");
                            Ok(ctx.get_latest_blockhash(config.unwrap_or_default())?)
                        }

                        async fn get_block(
                            ctx: JsonRpcRequestProcessor,
                            slot: Slot,
                            config: Option<RpcEncodingConfigWrapper<RpcBlockConfig>>,
                        ) -> RpcResult<Option<UiConfirmedBlock>> {
                            debug!("get_block rpc request received: {:?}", slot);
                            Ok(ctx.get_block(slot, config).await?)
                        }

                        async fn get_blocks(
                            ctx: JsonRpcRequestProcessor,
                            start_slot: Slot,
                            config: Option<RpcBlocksConfigWrapper>,
                            commitment: Option<CommitmentConfig>,
                        ) -> RpcResult<Vec<Slot>> {
                            let (end_slot, maybe_commitment) =
                                config.map(|config| config.unzip()).unwrap_or_default();
                            debug!(
                                "get_blocks rpc request received: {}-{:?}",
                                start_slot, end_slot
                            );

                            Ok(ctx
                                .get_blocks(start_slot, end_slot, commitment.or(maybe_commitment))
                                .await?)
                        }

                        async fn send_transaction(
                            ctx: JsonRpcRequestProcessor,
                            data: String,
                            config: Option<RpcSendTransactionConfig>,
                        ) -> RpcResult<String> {
                            debug!("send_transaction rpc request received");
                            let RpcSendTransactionConfig {
                                skip_preflight,
                                preflight_commitment,
                                encoding,
                                max_retries,
                                min_context_slot,
                            } = config.unwrap_or_default();
                            let tx_encoding = encoding.unwrap_or(UiTransactionEncoding::Base58);
                            let binary_encoding =
                                tx_encoding.into_binary_encoding().ok_or_else(|| {
                                    invalid_params(format!(
                    "unsupported encoding: {tx_encoding}. Supported encodings: base58, base64"
                ))
                                })?;
                            let (wire_transaction, unsanitized_tx) = decode_and_deserialize::<
                                VersionedTransaction,
                            >(
                                data, binary_encoding
                            )?;

                            let preflight_commitment = preflight_commitment
                                .map(|commitment| CommitmentConfig { commitment });
                            let preflight_bank = &*ctx.get_bank_with_config(RpcContextConfig {
                                commitment: preflight_commitment,
                                min_context_slot,
                            })?;

                            let transaction = sanitize_transaction(unsanitized_tx, preflight_bank)?;
                            let signature = *transaction.signature();

                            let mut last_valid_block_height = preflight_bank
                                .get_blockhash_last_valid_block_height(
                                    transaction.message().recent_blockhash(),
                                )
                                .unwrap_or(0);

                            let durable_nonce_info = transaction
                                .get_durable_nonce()
                                .map(|&pubkey| (pubkey, *transaction.message().recent_blockhash()));
                            if durable_nonce_info.is_some() {
                                // While it uses a defined constant, this last_valid_block_height value is chosen arbitrarily.
                                // It provides a fallback timeout for durable-nonce transaction retries in case of
                                // malicious packing of the retry queue. Durable-nonce transactions are otherwise
                                // retried until the nonce is advanced.
                                last_valid_block_height =
                                    preflight_bank.block_height() + MAX_RECENT_BLOCKHASHES as u64;
                            }

                            if !skip_preflight {
                                verify_transaction(&transaction, &preflight_bank.feature_set)?;

                                match ctx.health.check() {
                                    RpcHealthStatus::Ok => (),
                                    RpcHealthStatus::Unknown => {
                                        inc_new_counter_info!("rpc-send-tx_health-unknown", 1);
                                        return Err(RpcCustomError::NodeUnhealthy {
                                            num_slots_behind: None,
                                        }
                                        .into());
                                    }
                                    RpcHealthStatus::Behind { num_slots } => {
                                        inc_new_counter_info!("rpc-send-tx_health-behind", 1);
                                        return Err(RpcCustomError::NodeUnhealthy {
                                            num_slots_behind: Some(num_slots),
                                        }
                                        .into());
                                    }
                                }

                                if let TransactionSimulationResult {
                                    result: Err(err),
                                    logs,
                                    post_simulation_accounts: _,
                                    units_consumed,
                                    return_data,
                                } = preflight_bank.simulate_transaction(transaction)
                                {
                                    match err {
                                        TransactionError::BlockhashNotFound => {
                                            inc_new_counter_info!(
                                                "rpc-send-tx_err-blockhash-not-found",
                                                1
                                            );
                                        }
                                        _ => {
                                            inc_new_counter_info!("rpc-send-tx_err-other", 1);
                                        }
                                    }
                                    return Err(RpcCustomError::SendTransactionPreflightFailure {
                                        message: format!("Transaction simulation failed: {err}"),
                                        result: RpcSimulateTransactionResult {
                                            err: Some(err),
                                            logs: Some(logs),
                                            accounts: None,
                                            units_consumed: Some(units_consumed),
                                            return_data: return_data
                                                .map(|return_data| return_data.into()),
                                        },
                                    }
                                    .into());
                                }
                            }

                            Ok(_send_transaction(
                                &ctx,
                                signature,
                                wire_transaction,
                                last_valid_block_height,
                                durable_nonce_info,
                                max_retries,
                            )?)
                        }

                        async fn simulate_transaction(
                            ctx: JsonRpcRequestProcessor,
                            data: String,
                            config: Option<RpcSimulateTransactionConfig>,
                        ) -> RpcResult<RpcResponse<RpcSimulateTransactionResult>>
                        {
                            debug!("simulate_transaction rpc request received");
                            let RpcSimulateTransactionConfig {
                                sig_verify,
                                replace_recent_blockhash,
                                commitment,
                                encoding,
                                accounts: config_accounts,
                                min_context_slot,
                            } = config.unwrap_or_default();
                            let tx_encoding = encoding.unwrap_or(UiTransactionEncoding::Base58);
                            let binary_encoding =
                                tx_encoding.into_binary_encoding().ok_or_else(|| {
                                    invalid_params(format!(
                    "unsupported encoding: {tx_encoding}. Supported encodings: base58, base64"
                ))
                                })?;
                            let (_, mut unsanitized_tx) = decode_and_deserialize::<
                                VersionedTransaction,
                            >(
                                data, binary_encoding
                            )?;

                            let bank = &*ctx.get_bank_with_config(RpcContextConfig {
                                commitment,
                                min_context_slot,
                            })?;
                            if replace_recent_blockhash {
                                if sig_verify {
                                    return Err(ErrorObject::new(
                                        ErrorCode::InvalidParams,
                                        "sigVerify may not be used with replaceRecentBlockhash"
                                            .to_string(),
                                    )
                                    .into());
                                }
                                unsanitized_tx
                                    .message
                                    .set_recent_blockhash(bank.last_blockhash());
                            }

                            let transaction = sanitize_transaction(unsanitized_tx, bank)?;
                            if sig_verify {
                                verify_transaction(&transaction, &bank.feature_set)?;
                            }
                            let number_of_accounts = transaction.message().account_keys().len();

                            let TransactionSimulationResult {
                                result,
                                logs,
                                post_simulation_accounts,
                                units_consumed,
                                return_data,
                            } = bank.simulate_transaction(transaction);

                            let accounts = if let Some(config_accounts) = config_accounts {
                                let accounts_encoding = config_accounts
                                    .encoding
                                    .unwrap_or(UiAccountEncoding::Base64);

                                if accounts_encoding == UiAccountEncoding::Binary
                                    || accounts_encoding == UiAccountEncoding::Base58
                                {
                                    return Err(ErrorObject::new(
                                        ErrorCode::InvalidParams,
                                        "base58 encoding not supported".to_string(),
                                    )
                                    .into());
                                }

                                if config_accounts.addresses.len() > number_of_accounts {
                                    return Err(ErrorObject::new(
                                        ErrorCode::InvalidParams,
                                        format!(
                                            "Too many accounts provided; max {number_of_accounts}"
                                        ),
                                    )
                                    .into());
                                }

                                if result.is_err() {
                                    Some(vec![None; config_accounts.addresses.len()])
                                } else {
                                    Some(
                                        config_accounts
                                            .addresses
                                            .iter()
                                            .map(|address_str| {
                                                let address = verify_pubkey(address_str)?;
                                                post_simulation_accounts
                                                    .iter()
                                                    .find(|(key, _account)| key == &address)
                                                    .map(|(pubkey, account)| {
                                                        encode_account(
                                                            account,
                                                            pubkey,
                                                            accounts_encoding,
                                                            None,
                                                        )
                                                    })
                                                    .transpose()
                                            })
                                            .collect::<Result<Vec<_>>>()?,
                                    )
                                }
                            } else {
                                None
                            };

                            Ok(new_response(
                                bank,
                                RpcSimulateTransactionResult {
                                    err: result.err(),
                                    logs: Some(logs),
                                    accounts,
                                    units_consumed: Some(units_consumed),
                                    return_data: return_data.map(|return_data| return_data.into()),
                                },
                            ))
                        }

                        async fn get_balance(
                            ctx: JsonRpcRequestProcessor,
                            pubkey: String,
                            config: RpcContextConfig,
                        ) -> RpcResult<RpcResponse<u64>> {
                            let pubkey = Pubkey::from_str(&pubkey)?;
                            let bank = ctx.get_bank_with_config(config)?;
                            Ok(new_response(&bank, bank.get_balance(&pubkey)))
                        }


                        async fn get_program_accounts(
                            ctx: JsonRpcRequestProcessor,
                            program_id_str: String,
                            config: Option<RpcProgramAccountsConfig>,
                        ) -> RpcResult<OptionalContext<Vec<RpcKeyedAccount>>> {
                            debug!(
                                "get_program_accounts rpc request received: {:?}",
                                program_id_str
                            );
                            let program_id = verify_pubkey(&program_id_str)?;
                            let (config, filters, with_context) = if let Some(config) = config {
                                (
                                    Some(config.account_config),
                                    config.filters.unwrap_or_default(),
                                    config.with_context.unwrap_or_default(),
                                )
                            } else {
                                (None, vec![], false)
                            };
                            if filters.len() > MAX_GET_PROGRAM_ACCOUNT_FILTERS {
                                    return Err(ErrorObject::new(
                                        ErrorCode::InvalidParams,
                                        format!("Too many filters provided; max {MAX_GET_PROGRAM_ACCOUNT_FILTERS}")
                                    )
                                    .into());
                            }
                            for filter in &filters {
                                verify_filter(filter)?;
                            }

                            Ok(ctx.get_program_accounts(&program_id, config, filters, with_context)?)
                        }


                        HttpServer::new(move || {
                            let mut app_state = RpcModule::new(meta.clone());
                            app_state.register("getSlot", get_slot);
                            app_state.register("getVersion", get_version);
                            app_state.register("getBlockHeight", get_block_height);
                            app_state.register("getLatestBlockhash", get_latest_blockhash);
                            app_state.register("getBlock", get_block);
                            app_state.register("getBlocks", get_blocks);
                            app_state.register("SendTransaction", send_transaction);
                            app_state.register("simulateTransaction", simulate_transaction);
                            app_state.register("getBalance", get_balance);
                            app_state.register("getProgramAccounts", get_program_accounts);

                            App::new().app_data(web::Data::new(app_state)).service(
                                web::resource("/")
                                    .route(web::to(rpc_handler::<JsonRpcRequestProcessor>)),
                            )
                        })
                        .bind(rpc_addr)
                        .unwrap()
                        .run()
                        .await
                        .unwrap();
                    }

                    //if let Err(err) = server {
                    //    warn!(
                    //        "JSON RPC service unavailable error: {err:?}. \n\
                    //       Also, check that port {} is not already in use by another application",
                    //        rpc_addr.port()
                    //    );
                    //    close_handle_sender.send(Err(err.to_string())).unwrap();
                    //    return;
                    //}

                    //let server = server.unwrap();

                    close_handle_sender.send(()).unwrap();

                    //server.stopped().await;
                    exit_bigtable_ledger_upload_service.store(true, Ordering::Relaxed);
                });
            })
            .unwrap();

        //let close_handle = close_handle_receiver.recv().unwrap()?;
        //let close_handle_ = close_handle.clone();

        validator_exit
            .write()
            .unwrap()
            .register_exit(Box::new(move || {
                //close_handle_.stop().unwrap();
            }));

        Ok(Self {
            thread_hdl,
            #[cfg(test)]
            request_processor: test_request_processor,
            close_handle: Some(()),
        })
    }

    pub fn exit(&mut self) {
        if let Some(c) = self.close_handle.take() {
            //c.stop().unwrap()
        }
    }

    pub fn join(mut self) -> thread::Result<()> {
        self.exit();
        self.thread_hdl.join()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::rpc::{create_validator_exit, tests::new_test_cluster_info},
        solana_gossip::{
            crds::GossipRoute,
            crds_value::{AccountsHashes, CrdsData, CrdsValue},
        },
        solana_ledger::{
            genesis_utils::{create_genesis_config, GenesisConfigInfo},
            get_tmp_ledger_path,
        },
        solana_rpc_client_api::config::RpcContextConfig,
        solana_runtime::bank::Bank,
        solana_sdk::{
            genesis_config::{ClusterType, DEFAULT_GENESIS_ARCHIVE},
            signature::Signer,
        },
        std::{
            io::Write,
            net::{IpAddr, Ipv4Addr},
        },
        tokio::runtime::Runtime,
    };

    #[test]
    fn test_rpc_new() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let exit = Arc::new(AtomicBool::new(false));
        let validator_exit = create_validator_exit(&exit);
        let bank = Bank::new_for_tests(&genesis_config);
        let cluster_info = Arc::new(new_test_cluster_info());
        let ip_addr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
        let rpc_addr = SocketAddr::new(
            ip_addr,
            solana_net_utils::find_available_port_in_range(ip_addr, (10000, 65535)).unwrap(),
        );
        let bank_forks = Arc::new(RwLock::new(BankForks::new(bank)));
        let ledger_path = get_tmp_ledger_path!();
        let blockstore = Arc::new(Blockstore::open(&ledger_path).unwrap());
        let block_commitment_cache = Arc::new(RwLock::new(BlockCommitmentCache::default()));
        let optimistically_confirmed_bank =
            OptimisticallyConfirmedBank::locked_from_bank_forks_root(&bank_forks);
        let connection_cache = Arc::new(ConnectionCache::new("connection_cache_test"));
        let mut rpc_service = JsonRpcService::new(
            rpc_addr,
            JsonRpcConfig::default(),
            None,
            bank_forks,
            block_commitment_cache,
            blockstore,
            cluster_info,
            None,
            Hash::default(),
            &PathBuf::from("farf"),
            validator_exit,
            exit,
            None,
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(true)),
            optimistically_confirmed_bank,
            send_transaction_service::Config {
                retry_rate_ms: 1000,
                leader_forward_count: 1,
                ..send_transaction_service::Config::default()
            },
            Arc::new(MaxSlots::default()),
            Arc::new(LeaderScheduleCache::default()),
            connection_cache,
            Arc::new(AtomicU64::default()),
            Arc::new(AtomicU64::default()),
            Arc::new(PrioritizationFeeCache::default()),
        )
        .expect("assume successful JsonRpcService start");
        let thread = rpc_service.thread_hdl.thread();
        assert_eq!(thread.name().unwrap(), "solJsonRpcSvc");

        assert_eq!(
            10_000,
            rpc_service
                .request_processor
                .get_balance(&mint_keypair.pubkey(), RpcContextConfig::default())
                .unwrap()
                .value
        );
        rpc_service.exit();
        rpc_service.join().unwrap();
    }

    fn create_bank_forks() -> Arc<RwLock<BankForks>> {
        let GenesisConfigInfo {
            mut genesis_config, ..
        } = create_genesis_config(10_000);
        genesis_config.cluster_type = ClusterType::MainnetBeta;
        let bank = Bank::new_for_tests(&genesis_config);
        Arc::new(RwLock::new(BankForks::new(bank)))
    }

    #[test]
    fn test_process_rest_api() {
        let bank_forks = create_bank_forks();

        assert_eq!(None, process_rest(&bank_forks, "not-a-supported-rest-api"));
        assert_eq!(
            process_rest(&bank_forks, "/v0/circulating-supply"),
            process_rest(&bank_forks, "/v0/total-supply")
        );
    }

    #[test]
    fn test_strip_prefix() {
        assert_eq!(RpcRequestMiddleware::strip_leading_slash("/"), Some(""));
        assert_eq!(RpcRequestMiddleware::strip_leading_slash("//"), Some("/"));
        assert_eq!(
            RpcRequestMiddleware::strip_leading_slash("/abc"),
            Some("abc")
        );
        assert_eq!(
            RpcRequestMiddleware::strip_leading_slash("//abc"),
            Some("/abc")
        );
        assert_eq!(
            RpcRequestMiddleware::strip_leading_slash("/./abc"),
            Some("./abc")
        );
        assert_eq!(
            RpcRequestMiddleware::strip_leading_slash("/../abc"),
            Some("../abc")
        );

        assert_eq!(RpcRequestMiddleware::strip_leading_slash(""), None);
        assert_eq!(RpcRequestMiddleware::strip_leading_slash("./"), None);
        assert_eq!(RpcRequestMiddleware::strip_leading_slash("../"), None);
        assert_eq!(RpcRequestMiddleware::strip_leading_slash("."), None);
        assert_eq!(RpcRequestMiddleware::strip_leading_slash(".."), None);
        assert_eq!(RpcRequestMiddleware::strip_leading_slash("abc"), None);
    }

    #[test]
    fn test_is_file_get_path() {
        let bank_forks = create_bank_forks();
        let rrm = RpcRequestMiddleware::new(
            PathBuf::from("/"),
            None,
            bank_forks.clone(),
            RpcHealth::stub(),
        );
        let rrm_with_snapshot_config = RpcRequestMiddleware::new(
            PathBuf::from("/"),
            Some(SnapshotConfig::default()),
            bank_forks,
            RpcHealth::stub(),
        );

        assert!(rrm.is_file_get_path(DEFAULT_GENESIS_DOWNLOAD_PATH));
        assert!(!rrm.is_file_get_path(DEFAULT_GENESIS_ARCHIVE));
        assert!(!rrm.is_file_get_path("//genesis.tar.bz2"));
        assert!(!rrm.is_file_get_path("/../genesis.tar.bz2"));

        assert!(!rrm.is_file_get_path("/snapshot.tar.bz2")); // This is a redirect

        assert!(!rrm.is_file_get_path(
            "/snapshot-100-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar.bz2"
        ));
        assert!(!rrm.is_file_get_path(
            "/incremental-snapshot-100-200-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar.bz2"
        ));

        assert!(rrm_with_snapshot_config.is_file_get_path(
            "/snapshot-100-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar.bz2"
        ));
        assert!(rrm_with_snapshot_config.is_file_get_path(
            "/snapshot-100-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar.zst"
        ));
        assert!(rrm_with_snapshot_config
            .is_file_get_path("/snapshot-100-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar.gz"));
        assert!(rrm_with_snapshot_config
            .is_file_get_path("/snapshot-100-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar"));

        assert!(rrm_with_snapshot_config.is_file_get_path(
            "/incremental-snapshot-100-200-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar.bz2"
        ));
        assert!(rrm_with_snapshot_config.is_file_get_path(
            "/incremental-snapshot-100-200-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar.zst"
        ));
        assert!(rrm_with_snapshot_config.is_file_get_path(
            "/incremental-snapshot-100-200-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar.gz"
        ));
        assert!(rrm_with_snapshot_config.is_file_get_path(
            "/incremental-snapshot-100-200-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar"
        ));

        assert!(!rrm_with_snapshot_config.is_file_get_path(
            "/snapshot-notaslotnumber-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar.bz2"
        ));
        assert!(!rrm_with_snapshot_config.is_file_get_path(
            "/incremental-snapshot-notaslotnumber-200-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar.bz2"
        ));
        assert!(!rrm_with_snapshot_config.is_file_get_path(
            "/incremental-snapshot-100-notaslotnumber-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar.bz2"
        ));

        assert!(!rrm_with_snapshot_config.is_file_get_path("../../../test/snapshot-123-xxx.tar"));
        assert!(!rrm_with_snapshot_config
            .is_file_get_path("../../../test/incremental-snapshot-123-456-xxx.tar"));

        assert!(!rrm.is_file_get_path("/"));
        assert!(!rrm.is_file_get_path("//"));
        assert!(!rrm.is_file_get_path("/."));
        assert!(!rrm.is_file_get_path("/./"));
        assert!(!rrm.is_file_get_path("/.."));
        assert!(!rrm.is_file_get_path("/../"));
        assert!(!rrm.is_file_get_path("."));
        assert!(!rrm.is_file_get_path("./"));
        assert!(!rrm.is_file_get_path(".//"));
        assert!(!rrm.is_file_get_path(".."));
        assert!(!rrm.is_file_get_path("../"));
        assert!(!rrm.is_file_get_path("..//"));
        assert!(!rrm.is_file_get_path("🎣"));

        assert!(!rrm_with_snapshot_config
            .is_file_get_path("//snapshot-100-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar"));
        assert!(!rrm_with_snapshot_config
            .is_file_get_path("/./snapshot-100-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar"));
        assert!(!rrm_with_snapshot_config
            .is_file_get_path("/../snapshot-100-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar"));
        assert!(!rrm_with_snapshot_config.is_file_get_path(
            "//incremental-snapshot-100-200-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar"
        ));
        assert!(!rrm_with_snapshot_config.is_file_get_path(
            "/./incremental-snapshot-100-200-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar"
        ));
        assert!(!rrm_with_snapshot_config.is_file_get_path(
            "/../incremental-snapshot-100-200-AvFf9oS8A8U78HdjT9YG2sTTThLHJZmhaMn2g8vkWYnr.tar"
        ));
    }

    #[test]
    fn test_process_file_get() {
        let runtime = Runtime::new().unwrap();

        let ledger_path = get_tmp_ledger_path!();
        std::fs::create_dir(&ledger_path).unwrap();

        let genesis_path = ledger_path.join(DEFAULT_GENESIS_ARCHIVE);
        let rrm = RpcRequestMiddleware::new(
            ledger_path.clone(),
            None,
            create_bank_forks(),
            RpcHealth::stub(),
        );

        // File does not exist => request should fail.
        let action = rrm.process_file_get(DEFAULT_GENESIS_DOWNLOAD_PATH);
        if let RequestMiddlewareAction::Respond(response) = action {
            let response = runtime.block_on(response);
            let response = response.unwrap();
            assert_ne!(response.status(), 200);
        } else {
            panic!("Unexpected RequestMiddlewareAction variant");
        }

        {
            let mut file = std::fs::File::create(&genesis_path).unwrap();
            file.write_all(b"should be ok").unwrap();
        }

        // Normal file exist => request should succeed.
        let action = rrm.process_file_get(DEFAULT_GENESIS_DOWNLOAD_PATH);
        if let RequestMiddlewareAction::Respond(response) = action {
            let response = runtime.block_on(response);
            let response = response.unwrap();
            assert_eq!(response.status(), 200);
        } else {
            panic!("Unexpected RequestMiddlewareAction variant");
        }

        #[cfg(unix)]
        {
            std::fs::remove_file(&genesis_path).unwrap();
            {
                let mut file = std::fs::File::create(ledger_path.join("wrong")).unwrap();
                file.write_all(b"wrong file").unwrap();
            }
            symlink::symlink_file("wrong", &genesis_path).unwrap();

            // File is a symbolic link => request should fail.
            let action = rrm.process_file_get(DEFAULT_GENESIS_DOWNLOAD_PATH);
            if let RequestMiddlewareAction::Respond(response) = action {
                let response = runtime.block_on(response);
                let response = response.unwrap();
                assert_ne!(response.status(), 200);
            } else {
                panic!("Unexpected RequestMiddlewareAction variant");
            }
        }
    }

    #[test]
    fn test_health_check_with_no_known_validators() {
        let rm = RpcRequestMiddleware::new(
            PathBuf::from("/"),
            None,
            create_bank_forks(),
            RpcHealth::stub(),
        );
        assert_eq!(rm.health_check(), "ok");
    }

    #[test]
    fn test_health_check_with_known_validators() {
        let cluster_info = Arc::new(new_test_cluster_info());
        let health_check_slot_distance = 123;
        let override_health_check = Arc::new(AtomicBool::new(false));
        let startup_verification_complete = Arc::new(AtomicBool::new(true));
        let known_validators = vec![
            solana_sdk::pubkey::new_rand(),
            solana_sdk::pubkey::new_rand(),
            solana_sdk::pubkey::new_rand(),
        ];

        let health = Arc::new(RpcHealth::new(
            cluster_info.clone(),
            Some(known_validators.clone().into_iter().collect()),
            health_check_slot_distance,
            override_health_check.clone(),
            startup_verification_complete,
        ));

        let rm = RpcRequestMiddleware::new(PathBuf::from("/"), None, create_bank_forks(), health);

        // No account hashes for this node or any known validators
        assert_eq!(rm.health_check(), "unknown");

        // No account hashes for any known validators
        cluster_info.push_accounts_hashes(vec![(1000, Hash::default()), (900, Hash::default())]);
        cluster_info.flush_push_queue();
        assert_eq!(rm.health_check(), "unknown");

        // Override health check
        override_health_check.store(true, Ordering::Relaxed);
        assert_eq!(rm.health_check(), "ok");
        override_health_check.store(false, Ordering::Relaxed);

        // This node is ahead of the known validators
        cluster_info
            .gossip
            .crds
            .write()
            .unwrap()
            .insert(
                CrdsValue::new_unsigned(CrdsData::AccountsHashes(AccountsHashes::new(
                    known_validators[0],
                    vec![
                        (1, Hash::default()),
                        (1001, Hash::default()),
                        (2, Hash::default()),
                    ],
                ))),
                1,
                GossipRoute::LocalMessage,
            )
            .unwrap();
        assert_eq!(rm.health_check(), "ok");

        // Node is slightly behind the known validators
        cluster_info
            .gossip
            .crds
            .write()
            .unwrap()
            .insert(
                CrdsValue::new_unsigned(CrdsData::AccountsHashes(AccountsHashes::new(
                    known_validators[1],
                    vec![(1000 + health_check_slot_distance - 1, Hash::default())],
                ))),
                1,
                GossipRoute::LocalMessage,
            )
            .unwrap();
        assert_eq!(rm.health_check(), "ok");

        // Node is far behind the known validators
        cluster_info
            .gossip
            .crds
            .write()
            .unwrap()
            .insert(
                CrdsValue::new_unsigned(CrdsData::AccountsHashes(AccountsHashes::new(
                    known_validators[2],
                    vec![(1000 + health_check_slot_distance, Hash::default())],
                ))),
                1,
                GossipRoute::LocalMessage,
            )
            .unwrap();
        assert_eq!(rm.health_check(), "behind");
    }
}
