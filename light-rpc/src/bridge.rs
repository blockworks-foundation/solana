use std::net::SocketAddr;
use std::sync::Arc;

use solana_client::connection_cache::ConnectionCache;
use solana_client::thin_client::ThinClient;

pub struct LightBridge {
    pub connection_cache: Arc<ConnectionCache>,
    pub thin_client: ThinClient,
}

impl LightBridge {
    pub fn new(rpc_addr: SocketAddr, tpu_addr: SocketAddr, connection_pool_size: usize) -> Self {
        let connection_cache = Arc::new(ConnectionCache::new(connection_pool_size));
        let thin_client = ThinClient::new(rpc_addr, tpu_addr, connection_cache.clone());

        Self {
            connection_cache,
            thin_client,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::LightBridge;

    const RPC_ADDR: &str = "127.0.0.1:8899";
    const TPU_ADDR: &str = "127.0.0.1:1027";
    const CONNECTION_POOL_SIZE: usize = 1;

    #[test]
    fn initialize_light_bridge() {
        let _light_rpc = LightBridge::new(
            RPC_ADDR.parse().unwrap(),
            TPU_ADDR.parse().unwrap(),
            CONNECTION_POOL_SIZE,
        );
    }
}
