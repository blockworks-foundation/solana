use crate::configs::SendTransactionConfig;
use crate::encoding::BinaryEncoding;
use crate::rpc::{JsonRpcError, JsonRpcRes, RpcMethod};
use actix_web::{web, App, HttpServer};
use solana_client::{
    connection_cache::ConnectionCache, thin_client::ThinClient, tpu_connection::TpuConnection,
};
use solana_sdk::transaction::Transaction;
use std::{
    net::{SocketAddr, ToSocketAddrs},
    sync::Arc,
};

/// A bridge between clients and tpu
pub struct LightBridge {
    #[allow(dead_code)]
    thin_client: ThinClient,
    tpu_addr: SocketAddr,
    connection_cache: Arc<ConnectionCache>,
}

impl LightBridge {
    pub fn new(rpc_addr: SocketAddr, tpu_addr: SocketAddr, connection_pool_size: usize) -> Self {
        let connection_cache = Arc::new(ConnectionCache::new(connection_pool_size));
        let thin_client = ThinClient::new(rpc_addr, tpu_addr, connection_cache.clone());

        Self {
            thin_client,
            tpu_addr,
            connection_cache,
        }
    }

    fn send_transaction(
        &self,
        transaction: String,
        config: SendTransactionConfig,
    ) -> Result<String, JsonRpcError> {
        let transaction = config.encoding.decode(transaction)?;

        let signature = bincode::deserialize::<Transaction>(&transaction)?.signatures[0];

        let conn = self.connection_cache.get_connection(&self.tpu_addr);
        conn.send_wire_transaction_async(transaction)?;

        Ok(String::from_utf8(BinaryEncoding::Base58.encode(signature)).unwrap())
    }

    /// Serialize params and execute the specified method
    pub async fn execute_rpc_request(
        &self,
        method: RpcMethod,
    ) -> Result<serde_json::Value, JsonRpcError> {
        match method {
            RpcMethod::SendTransaction(transaction, config) => {
                Ok(self.send_transaction(transaction, config)?.into())
            }
            RpcMethod::RequestAirdrop(_) => todo!(),
        }
    }

    /// List for `JsonRpc` requests
    pub async fn start_server(self, addr: impl ToSocketAddrs) -> Result<(), std::io::Error> {
        let bridge = Arc::new(self);

        HttpServer::new(move || {
            App::new()
                .app_data(web::Data::new(bridge.clone()))
                .route("/", web::post().to(Self::rpc_route))
        })
        .bind(addr)?
        .run()
        .await
    }

    async fn rpc_route(
        json_rpc_req: web::Json<RpcMethod>,
        state: web::Data<Arc<LightBridge>>,
    ) -> JsonRpcRes {
        state
            .execute_rpc_request(json_rpc_req.0)
            .await
            .try_into()
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::bridge::LightBridge,
        borsh::{BorshDeserialize, BorshSerialize},
    };

    const RPC_ADDR: &str = "127.0.0.1:8899";
    const TPU_ADDR: &str = "127.0.0.1:1027";
    const CONNECTION_POOL_SIZE: usize = 1;

    #[test]
    fn test_send_transaction() {
        let light_bridge = LightBridge::new(
            RPC_ADDR.parse().unwrap(),
            TPU_ADDR.parse().unwrap(),
            CONNECTION_POOL_SIZE,
        );

        let program_id = Pubkey::new_unique();
        let payer = Keypair::new();
        let bankins = BankInstruction::Initialize;
        let instruction = Instruction::new_with_borsh(program_id, &bankins, vec![]);

        let message = Message::new(&[instruction], Some(&payer.pubkey()));
        let blockhash = light_bridge
            .thin_client
            .rpc_client()
            .get_latest_blockhash()
            .unwrap();

        let tx = Transaction::new(&[&payer], message, blockhash);
        let x = light_bridge
            .send_transaction((tx, RpcSendTransactionConfig::default()))
            .unwrap();

        println!("{}", x);
    }
}
