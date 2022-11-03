use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::RwLock;

use solana_client::connection_cache::ConnectionCache;

pub struct TransactionToRetry {
    pub wire_transaction: Vec<u8>,
    pub max_retries: usize,
}

#[derive(Clone)]
pub struct LightRpcWorker {
    transaction_que: Arc<RwLock<VecDeque<TransactionToRetry>>>,
    connection_cache: Arc<ConnectionCache>,
}

impl LightRpcWorker {
    pub async fn enque(&self, transaction_to_retry: TransactionToRetry) {
        self.transaction_que
            .write()
            .await
            .push_back(transaction_to_retry);
    }
}

impl From<Arc<ConnectionCache>> for LightRpcWorker {
    fn from(connection_cache: Arc<ConnectionCache>) -> Self {
        Self {
            transaction_que: Default::default(),
            connection_cache,
        }
    }
}

impl Future for LightRpcWorker {
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::task::Poll::Pending
    }
}
