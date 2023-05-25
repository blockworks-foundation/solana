//! The `pubsub` module implements a threaded subscription service on client RPC request

use jsonrpsee::server::ServerBuilder;

use crate::rpc_pubsub::RpcSolPubSubInternalServer;

use {
    crate::{rpc_pubsub::RpcSolPubSubImpl, rpc_subscriptions::RpcSubscriptions},
    std::{
        net::SocketAddr,
        sync::Arc,
        thread::{self, Builder, JoinHandle},
    },
    thiserror::Error,
    tokio::sync::broadcast,
};

pub const MAX_ACTIVE_SUBSCRIPTIONS: u32 = 1_000_000;
pub const DEFAULT_QUEUE_CAPACITY_ITEMS: usize = 10_000_000;
pub const DEFAULT_TEST_QUEUE_CAPACITY_ITEMS: usize = 100;
pub const DEFAULT_QUEUE_CAPACITY_BYTES: usize = 256 * 1024 * 1024;
pub const DEFAULT_WORKER_THREADS: usize = 1;

#[derive(Debug, Clone)]
pub struct PubSubConfig {
    pub enable_block_subscription: bool,
    pub enable_vote_subscription: bool,
    pub max_active_subscriptions: u32,
    pub queue_capacity_items: usize,
    pub queue_capacity_bytes: usize,
    pub worker_threads: usize,
    pub notification_threads: Option<usize>,
}

impl Default for PubSubConfig {
    fn default() -> Self {
        Self {
            enable_block_subscription: false,
            enable_vote_subscription: false,
            max_active_subscriptions: MAX_ACTIVE_SUBSCRIPTIONS,
            queue_capacity_items: DEFAULT_QUEUE_CAPACITY_ITEMS,
            queue_capacity_bytes: DEFAULT_QUEUE_CAPACITY_BYTES,
            worker_threads: DEFAULT_WORKER_THREADS,
            notification_threads: None,
        }
    }
}

impl PubSubConfig {
    pub fn default_for_tests() -> Self {
        Self {
            enable_block_subscription: false,
            enable_vote_subscription: false,
            max_active_subscriptions: MAX_ACTIVE_SUBSCRIPTIONS,
            queue_capacity_items: DEFAULT_TEST_QUEUE_CAPACITY_ITEMS,
            queue_capacity_bytes: DEFAULT_QUEUE_CAPACITY_BYTES,
            worker_threads: DEFAULT_WORKER_THREADS,
            notification_threads: Some(2),
        }
    }
}

pub struct PubSubService {
    thread_hdl: JoinHandle<()>,
}

impl PubSubService {
    pub fn new(
        config: PubSubConfig,
        subscriptions: &Arc<RpcSubscriptions>,
        pubsub_addr: SocketAddr,
    ) -> Self {
        let subscription_control = subscriptions.control().clone();
        info!("rpc_pubsub bound to {:?}", pubsub_addr);

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(config.worker_threads)
            .enable_all()
            .build()
            .expect("runtime creation failed");

        let thread_hdl = Builder::new()
            .name("solRpcPubSub".to_string())
            .spawn(move || {
                runtime.block_on(async move {
                    let server = ServerBuilder::default()
                        .max_connections(config.max_active_subscriptions)
                        .ws_only()
                        .build(pubsub_addr)
                        .await
                        .expect("Error building server")
                        .start(
                            RpcSolPubSubImpl {
                                config,
                                subscription_control: subscription_control.clone(),
                            }
                            .into_rpc(),
                        );

                    if let Err(err) = server {
                        warn!(
                            "JSON Pub Sub RPC service unavailable error: {err:?}. \n\
                           Also, check that port {} is not already in use by another application",
                            pubsub_addr
                        );
                        return;
                    }

                    tokio::select! {
                        _ = server.unwrap().stopped() => {},
                        _ = subscription_control.listen_to_broadcast() => {}
                    }
                });
            })
            .expect("thread spawn failed");

        Self { thread_hdl }
    }

    pub fn close(self) -> thread::Result<()> {
        self.join()
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}

#[derive(Debug, Error)]
enum Error {
    #[error("handshake error: {0}")]
    Handshake(#[from] soketto::handshake::Error),
    #[error("connection error: {0}")]
    Connection(#[from] soketto::connection::Error),
    #[error("broadcast queue error: {0}")]
    Broadcast(#[from] broadcast::error::RecvError),
    #[error("client has lagged behind (notification is gone)")]
    NotificationIsGone,
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::optimistically_confirmed_bank_tracker::OptimisticallyConfirmedBank,
        solana_runtime::{
            bank::Bank,
            bank_forks::BankForks,
            commitment::BlockCommitmentCache,
            genesis_utils::{create_genesis_config, GenesisConfigInfo},
        },
        std::{
            net::{IpAddr, Ipv4Addr},
            sync::{
                atomic::{AtomicBool, AtomicU64},
                RwLock,
            },
        },
    };

    #[test]
    fn test_pubsub_new() {
        let pubsub_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0);
        let exit = Arc::new(AtomicBool::new(false));
        let max_complete_transaction_status_slot = Arc::new(AtomicU64::default());
        let max_complete_rewards_slot = Arc::new(AtomicU64::default());
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(10_000);
        let bank = Bank::new_for_tests(&genesis_config);
        let bank_forks = Arc::new(RwLock::new(BankForks::new(bank)));
        let optimistically_confirmed_bank =
            OptimisticallyConfirmedBank::locked_from_bank_forks_root(&bank_forks);
        let subscriptions = Arc::new(RpcSubscriptions::new_for_tests(
            &exit,
            max_complete_transaction_status_slot,
            max_complete_rewards_slot,
            bank_forks,
            Arc::new(RwLock::new(BlockCommitmentCache::new_for_tests())),
            optimistically_confirmed_bank,
        ));
        let (_trigger, pubsub_service) =
            PubSubService::new(PubSubConfig::default(), &subscriptions, pubsub_addr);
        let thread = pubsub_service.thread_hdl.thread();
        assert_eq!(thread.name().unwrap(), "solRpcPubSub");
    }
}
