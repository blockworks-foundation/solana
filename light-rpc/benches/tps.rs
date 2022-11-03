use std::str::FromStr;
use std::time::{Duration, Instant};

use light_rpc::bridge::LightBridge;
use light_rpc::configs::SendTransactionConfig;
use light_rpc::encoding::BinaryEncoding;
use solana_client::rpc_client::RpcClient;
use solana_sdk::message::Message;
use solana_sdk::native_token::LAMPORTS_PER_SOL;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signature};
use solana_sdk::signer::Signer;
use solana_sdk::system_instruction;
use solana_sdk::transaction::Transaction;

const TRANSACTION_COUNT: usize = 2500;
const RPC_ADDR: &str = "127.0.0.1:8899";
const TPU_ADDR: &str = "127.0.0.1:1027";
const CONNECTION_POOL_SIZE: usize = 1;

#[tokio::main]
async fn main() {
    let light_bridge = LightBridge::new(
        RPC_ADDR.parse().unwrap(),
        TPU_ADDR.parse().unwrap(),
        CONNECTION_POOL_SIZE,
    );

    let rpc_client = light_bridge.thin_client.rpc_client();

    eprintln!("version {}", rpc_client.get_version().unwrap());

    let payer = Keypair::new();
    let payer_pub_key = payer.pubkey();

    eprintln!("Payer: {payer_pub_key:?}");

    let airdrop_signature = rpc_client
        .request_airdrop(&payer_pub_key, LAMPORTS_PER_SOL * 20)
        .expect("error requesting airdrop");

    confirm_transactions(rpc_client, vec![airdrop_signature]).await;

    println!("{}", rpc_client.get_balance(&payer_pub_key).unwrap());

    let mut transaction_signatures = Vec::new();

    // send transactions
    for iter in 0..TRANSACTION_COUNT {
        // fetch the latest blockhash
        let blockhash = rpc_client
            .get_latest_blockhash()
            .expect("error getting latest blockhash");

        let to_pubkey = Pubkey::new_unique();
        let instruction = system_instruction::transfer(&payer.pubkey(), &to_pubkey, 5_000_000);

        let message = Message::new(&[instruction], Some(&payer.pubkey()));

        let tx = Transaction::new(&[&payer], message, blockhash);
        let tx = BinaryEncoding::Base58.encode(bincode::serialize(&tx).unwrap());

        let signature = light_bridge
            .send_transaction(tx, SendTransactionConfig::default())
            .unwrap();

        transaction_signatures.push(Signature::from_str(&signature).unwrap());
    }

    eprintln!("confirming transactions");

    let start_insntant = Instant::now();
    confirm_transactions(rpc_client, transaction_signatures).await;

    let time_elasped = (Instant::now() - start_insntant).as_secs();

    eprintln!(
        "Confirmed {TRANSACTION_COUNT} transaction(s) as finalized in {} seconds",
        time_elasped,
    );

    eprintln!(
        "TPS: {}",
        (TRANSACTION_COUNT as f64 / time_elasped as f64).round()
    );
}

async fn confirm_transactions(rpc_client: &RpcClient, mut signatures: Vec<Signature>) {
    let mut retry_signarues = Vec::with_capacity(signatures.len());

    loop {
        for signature in signatures {
            eprintln!("confirming {}", signature);
            if rpc_client
                .confirm_transaction(&signature)
                .expect("Error confirming trsnsaction")
            {
                eprintln!("signature finalized: {signature}");
            } else {
                retry_signarues.push(signature);
            }
        }

        if retry_signarues.is_empty() {
            return;
        }

        signatures = retry_signarues.clone();
        retry_signarues.clear();
        tokio::time::sleep(Duration::from_millis(1000)).await;
    }
}
