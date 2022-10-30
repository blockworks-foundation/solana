use {
    solana_client::{connection_cache::ConnectionCache, thin_client::ThinClient},
    solana_sdk::{
        client::AsyncClient, signature::Signature, transaction::Transaction,
        transport::TransportError,
    },
    std::{net::SocketAddr, sync::Arc},
};

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

    pub fn forward_transaction(
        &self,
        transaction: Transaction,
    ) -> Result<Signature, TransportError> {
        self.thin_client.async_send_transaction(transaction)
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::LightBridge,
        borsh::{BorshDeserialize, BorshSerialize},
        solana_sdk::{
            instruction::Instruction, message::Message, pubkey::Pubkey, signature::Signer,
            signer::keypair::Keypair, transaction::Transaction,
        },
    };

    const RPC_ADDR: &str = "127.0.0.1:8899";
    const TPU_ADDR: &str = "127.0.0.1:1027";
    const CONNECTION_POOL_SIZE: usize = 1;
    #[derive(BorshSerialize, BorshDeserialize)]
    enum BankInstruction {
        Initialize,
        Deposit { lamports: u64 },
        Withdraw { lamports: u64 },
    }

    #[test]
    fn initialize_light_bridge() {
        let _light_rpc = LightBridge::new(
            RPC_ADDR.parse().unwrap(),
            TPU_ADDR.parse().unwrap(),
            CONNECTION_POOL_SIZE,
        );
    }
    #[test]
    fn test_forward_transaction() {
        let light_rpc = LightRpc::new(
            RPC_ADDR.parse().unwrap(),
            TPU_ADDR.parse().unwrap(),
            CONNECTION_POOL_SIZE,
        );
        let program_id = Pubkey::new_unique();
        let payer = Keypair::new();
        let bankins = BankInstruction::Initialize;
        let instruction = Instruction::new_with_borsh(program_id, &bankins, vec![]);

        let message = Message::new(&[instruction], Some(&payer.pubkey()));
        let blockhash = light_rpc
            .thin_client
            .rpc_client()
            .get_latest_blockhash()
            .unwrap();
        let tx = Transaction::new(&[&payer], message, blockhash);
        let x = light_rpc.forward_transaction(tx).unwrap();
        println!("{}", x);
    }
}
