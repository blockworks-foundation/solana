use jsonrpsee::{
    core::Error,
    http_client::{transport::HttpBackend, HttpClient, HttpClientBuilder},
    proc_macros::rpc,
    server::ServerBuilder,
    types::{ErrorObject},
};
use solana_rpc::rpc::{invalid_params, Result};

use {
    log::*,
    serde::{Deserialize, Serialize},
    solana_core::{
        consensus::Tower, tower_storage::TowerStorage, validator::ValidatorStartProgress,
    },
    solana_gossip::{
        cluster_info::ClusterInfo, legacy_contact_info::LegacyContactInfo as ContactInfo,
    },
    solana_runtime::bank_forks::BankForks,
    solana_sdk::{
        exit::Exit,
        pubkey::Pubkey,
        signature::{read_keypair_file, Keypair, Signer},
    },
    std::{
        fmt::{self, Display},
        net::SocketAddr,
        path::{Path, PathBuf},
        sync::{Arc, RwLock},
        thread::{self, Builder},
        time::{Duration, SystemTime},
    },
    tokio::sync::oneshot::channel as oneshot_channel,
};

#[derive(Clone)]
pub struct AdminRpcRequestMetadataPostInit {
    pub cluster_info: Arc<ClusterInfo>,
    pub bank_forks: Arc<RwLock<BankForks>>,
    pub vote_account: Pubkey,
}

#[derive(Clone)]
pub struct AdminRpcRequestMetadata {
    pub rpc_addr: Option<SocketAddr>,
    pub start_time: SystemTime,
    pub start_progress: Arc<RwLock<ValidatorStartProgress>>,
    pub validator_exit: Arc<RwLock<Exit>>,
    pub authorized_voter_keypairs: Arc<RwLock<Vec<Arc<Keypair>>>>,
    pub tower_storage: Arc<dyn TowerStorage>,
    pub post_init: Arc<RwLock<Option<AdminRpcRequestMetadataPostInit>>>,
}

impl AdminRpcRequestMetadata {
    fn with_post_init<F, R>(&self, func: F) -> Result<R>
    where
        F: FnOnce(&AdminRpcRequestMetadataPostInit) -> Result<R>,
    {
        if let Some(post_init) = self.post_init.read().unwrap().as_ref() {
            func(post_init)
        } else {
            Err(invalid_params("Retry once validator start up is complete"))
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AdminRpcContactInfo {
    pub id: String,
    pub gossip: SocketAddr,
    pub tvu: SocketAddr,
    pub tvu_forwards: SocketAddr,
    pub repair: SocketAddr,
    pub tpu: SocketAddr,
    pub tpu_forwards: SocketAddr,
    pub tpu_vote: SocketAddr,
    pub rpc: SocketAddr,
    pub rpc_pubsub: SocketAddr,
    pub serve_repair: SocketAddr,
    pub last_updated_timestamp: u64,
    pub shred_version: u16,
}

impl From<ContactInfo> for AdminRpcContactInfo {
    fn from(contact_info: ContactInfo) -> Self {
        let ContactInfo {
            id,
            gossip,
            tvu,
            tvu_forwards,
            repair,
            tpu,
            tpu_forwards,
            tpu_vote,
            rpc,
            rpc_pubsub,
            serve_repair,
            wallclock,
            shred_version,
        } = contact_info;
        Self {
            id: id.to_string(),
            last_updated_timestamp: wallclock,
            gossip,
            tvu,
            tvu_forwards,
            repair,
            tpu,
            tpu_forwards,
            tpu_vote,
            rpc,
            rpc_pubsub,
            serve_repair,
            shred_version,
        }
    }
}

impl Display for AdminRpcContactInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "Identity: {}", self.id)?;
        writeln!(f, "Gossip: {}", self.gossip)?;
        writeln!(f, "TVU: {}", self.tvu)?;
        writeln!(f, "TVU Forwards: {}", self.tvu_forwards)?;
        writeln!(f, "Repair: {}", self.repair)?;
        writeln!(f, "TPU: {}", self.tpu)?;
        writeln!(f, "TPU Forwards: {}", self.tpu_forwards)?;
        writeln!(f, "TPU Votes: {}", self.tpu_vote)?;
        writeln!(f, "RPC: {}", self.rpc)?;
        writeln!(f, "RPC Pubsub: {}", self.rpc_pubsub)?;
        writeln!(f, "Serve Repair: {}", self.serve_repair)?;
        writeln!(f, "Last Updated Timestamp: {}", self.last_updated_timestamp)?;
        writeln!(f, "Shred Version: {}", self.shred_version)
    }
}

#[rpc(server, client)]
pub trait AdminRpc {
    #[method(name = "exit")]
    fn exit(&self) -> std::result::Result<(), ErrorObject<'static>>;

    #[method(name = "rpcAddress")]
    fn rpc_addr(&self) -> std::result::Result<Option<SocketAddr>, ErrorObject<'static>>;

    #[method(name = "setLogFilter")]
    fn set_log_filter(&self, filter: String) -> std::result::Result<(), ErrorObject<'static>>;

    #[method(name = "startTime")]
    fn start_time(&self) -> std::result::Result<SystemTime, ErrorObject<'static>>;

    #[method(name = "startProgress")]
    fn start_progress(&self) -> std::result::Result<ValidatorStartProgress, ErrorObject<'static>>;

    #[method(name = "addAuthorizedVoter")]
    fn add_authorized_voter(
        &self,
        keypair_file: String,
    ) -> std::result::Result<(), ErrorObject<'static>>;

    #[method(name = "addAuthorizedVoterFromBytes")]
    fn add_authorized_voter_from_bytes(
        &self,
        keypair: Vec<u8>,
    ) -> std::result::Result<(), ErrorObject<'static>>;

    #[method(name = "removeAllAuthorizedVoters")]
    fn remove_all_authorized_voters(&self) -> std::result::Result<(), ErrorObject<'static>>;

    #[method(name = "setIdentity")]
    fn set_identity(
        &self,
        keypair_file: String,
        require_tower: bool,
    ) -> std::result::Result<(), ErrorObject<'static>>;

    #[method(name = "setIdentityFromBytes")]
    fn set_identity_from_bytes(
        &self,
        identity_keypair: Vec<u8>,
        require_tower: bool,
    ) -> std::result::Result<(), ErrorObject<'static>>;

    #[method(name = "contactInfo")]
    fn contact_info(&self) -> std::result::Result<AdminRpcContactInfo, ErrorObject<'static>>;
}

pub struct AdminRpcImpl {
    pub meta: AdminRpcRequestMetadata,
}

#[jsonrpsee::core::async_trait]
impl AdminRpcServer for AdminRpcImpl {
    fn exit(&self) -> Result<()> {
        debug!("exit admin rpc request received");

        let validator_exit = self.meta.validator_exit.clone();
        thread::Builder::new()
            .name("solProcessExit".into())
            .spawn(move || {
                // Delay exit signal until this RPC request completes, otherwise the caller of `exit` might
                // receive a confusing error as the validator shuts down before a response is sent back.
                thread::sleep(Duration::from_millis(100));

                warn!("validator exit requested");
                validator_exit.write().unwrap().exit();

                // TODO: Debug why Exit doesn't always cause the validator to fully exit
                // (rocksdb background processing or some other stuck thread perhaps?).
                //
                // If the process is still alive after five seconds, exit harder
                thread::sleep(Duration::from_secs(5));
                warn!("validator exit timeout");
                std::process::exit(0);
            })
            .unwrap();
        Ok(())
    }

    fn rpc_addr(&self) -> Result<Option<SocketAddr>> {
        debug!("rpc_addr admin rpc request received");
        Ok(self.meta.rpc_addr)
    }

    fn set_log_filter(&self, filter: String) -> Result<()> {
        debug!("set_log_filter admin rpc request received");
        solana_logger::setup_with(&filter);
        Ok(())
    }

    fn start_time(&self) -> Result<SystemTime> {
        debug!("start_time admin rpc request received");
        Ok(self.meta.start_time)
    }

    fn start_progress(&self) -> Result<ValidatorStartProgress> {
        debug!("start_progress admin rpc request received");
        Ok(*self.meta.start_progress.read().unwrap())
    }

    fn add_authorized_voter(&self, keypair_file: String) -> Result<()> {
        debug!("add_authorized_voter request received");

        let authorized_voter =
            read_keypair_file(keypair_file).map_err(|err| invalid_params(format!("{err}")))?;

        AdminRpcImpl::add_authorized_voter_keypair(&self.meta, authorized_voter)
    }

    fn add_authorized_voter_from_bytes(&self, keypair: Vec<u8>) -> Result<()> {
        debug!("add_authorized_voter_from_bytes request received");

        let authorized_voter = Keypair::from_bytes(&keypair).map_err(|err| {
            invalid_params(format!(
                "Failed to read authorized voter keypair from provided byte array: {err}"
            ))
        })?;

        AdminRpcImpl::add_authorized_voter_keypair(&self.meta, authorized_voter)
    }

    fn remove_all_authorized_voters(&self) -> Result<()> {
        debug!("remove_all_authorized_voters received");
        self.meta.authorized_voter_keypairs.write().unwrap().clear();
        Ok(())
    }

    fn set_identity(&self, keypair_file: String, require_tower: bool) -> Result<()> {
        debug!("set_identity request received");

        let identity_keypair = read_keypair_file(&keypair_file).map_err(|err| {
            invalid_params(format!(
                "Failed to read identity keypair from {keypair_file}: {err}"
            ))
        })?;

        AdminRpcImpl::set_identity_keypair(&self.meta, identity_keypair, require_tower)
    }

    fn set_identity_from_bytes(
        &self,
        identity_keypair: Vec<u8>,
        require_tower: bool,
    ) -> Result<()> {
        debug!("set_identity_from_bytes request received");

        let identity_keypair = Keypair::from_bytes(&identity_keypair).map_err(|err| {
            invalid_params(format!(
                "Failed to read identity keypair from provided byte array: {err}"
            ))
        })?;

        AdminRpcImpl::set_identity_keypair(&self.meta, identity_keypair, require_tower)
    }

    fn contact_info(&self) -> Result<AdminRpcContactInfo> {
        self.meta.with_post_init(|post_init| Ok(post_init.cluster_info.my_contact_info().into()))
    }
}

impl AdminRpcImpl {
    fn add_authorized_voter_keypair(
        meta: &AdminRpcRequestMetadata,
        authorized_voter: Keypair,
    ) -> Result<()> {
        let mut authorized_voter_keypairs = meta.authorized_voter_keypairs.write().unwrap();

        if authorized_voter_keypairs
            .iter()
            .any(|x| x.pubkey() == authorized_voter.pubkey())
        {
            Err(invalid_params("Authorized voter already present"))
        } else {
            authorized_voter_keypairs.push(Arc::new(authorized_voter));
            Ok(())
        }
    }

    fn set_identity_keypair(
        meta: &AdminRpcRequestMetadata,
        identity_keypair: Keypair,
        require_tower: bool,
    ) -> Result<()> {
        meta.with_post_init(|post_init| {
            if require_tower {
                let _ = Tower::restore(meta.tower_storage.as_ref(), &identity_keypair.pubkey())
                    .map_err(|err| {
                        invalid_params(format!(
                            "Unable to load tower file for identity {}: {}",
                            identity_keypair.pubkey(),
                            err
                        ))
                    })?;
            }

            solana_metrics::set_host_id(identity_keypair.pubkey().to_string());
            post_init
                .cluster_info
                .set_keypair(Arc::new(identity_keypair));
            warn!("Identity set to {}", post_init.cluster_info.id());
            Ok(())
        })
    }
}

// Start the Admin RPC interface
pub fn run(ledger_path: &Path, meta: AdminRpcRequestMetadata) {
    return;
    
    let admin_rpc_path = admin_rpc_path(ledger_path);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_name("solAdminRpcEl")
        .worker_threads(3) // Three still seems like a lot, and better than the default of available core count
        .enable_all()
        .build()
        .expect("AdminRpc Runtime");

    Builder::new()
        .name("solAdminRpc".to_string())
        .spawn(move || {
            runtime.block_on(async move {
                let validator_exit = meta.validator_exit.clone();

                let rpc = AdminRpcImpl { meta: meta.clone() }.into_rpc();
                let server = ServerBuilder::default()
                    .build(format!("{}", admin_rpc_path.display()))
                    .await
                    .expect("Error building admin rpc server")
                    .start(rpc);

                if let Err(err) = server {
                    warn!("Unable to start admin rpc service: {:?}", err);
                    return;
                }

                let server_handle = server.unwrap();
                info!("Started admin rpc service!");

                {
                    let server_handle = server_handle.clone();
                    validator_exit
                        .write()
                        .unwrap()
                        .register_exit(Box::new(move || {
                            server_handle.stop().unwrap();
                        }));
                }

                server_handle.stopped().await;
            });
        })
        .unwrap();
}

fn admin_rpc_path(ledger_path: &Path) -> PathBuf {
    #[cfg(target_family = "windows")]
    {
        // More information about the wackiness of pipe names over at
        // https://docs.microsoft.com/en-us/windows/win32/ipc/pipe-names
        if let Some(ledger_filename) = ledger_path.file_name() {
            PathBuf::from(format!(
                "\\\\.\\pipe\\{}-admin.rpc",
                ledger_filename.to_string_lossy()
            ))
        } else {
            PathBuf::from("\\\\.\\pipe\\admin.rpc")
        }
    }
    #[cfg(not(target_family = "windows"))]
    {
        ledger_path.join("admin.rpc")
    }
}

// Connect to the Admin RPC interface
pub async fn connect(ledger_path: &Path) -> std::result::Result<HttpClient<HttpBackend>, Error> {
    let admin_rpc_path = admin_rpc_path(ledger_path);
    if !admin_rpc_path.exists() {
        Err(Error::Custom(format!(
            "{} does not exist",
            admin_rpc_path.display()
        )))
    } else {
        Ok(HttpClientBuilder::default().build(format!("{}", admin_rpc_path.display()))?)
    }
}

pub fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().expect("new tokio runtime")
}
