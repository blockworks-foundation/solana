use std::str::FromStr;
use std::time::{Duration, Instant};

use light_rpc::bridge::LightBridge;
use light_rpc::configs::SendTransactionConfig;
use light_rpc::encoding::BinaryEncoding;
use solana_client::rpc_client::RpcClient;
use solana_sdk::native_token::LAMPORTS_PER_SOL;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signature};
use solana_sdk::signer::Signer;
use solana_sdk::system_instruction;
use solana_sdk::transaction::Transaction;

const RPC_ADDR: &str = "127.0.0.1:8899";
const TPU_ADDR: &str = "127.0.0.1:1027";
const CONNECTION_POOL_SIZE: usize = 1;

const LAMPORTS_TO_SEND_PER_TX: u64 = 1_000_000;
const PAYER_BANK_BALANCE: u64 = LAMPORTS_PER_SOL * 20;
const NUMBER_OF_TXS: u64 = 2000;
const NUMBER_OF_RUNS: u64 = 5;

#[derive(serde::Serialize)]
struct Metrics {
    #[serde(rename = "Number of TX(s)")]
    number_of_transactions: u64,
    #[serde(rename = "Time To Send TX(s) ms")]
    time_to_send_txs: u128,
    #[serde(rename = "Time To Confirm TX(s) ms")]
    time_to_confirm_txs: u128,
    #[serde(rename = "Total duration(ms)")]
    duration: u128,
    #[serde(rename = "TPS")]
    tps: u64,
}

#[tokio::main]
async fn main() {
    let light_bridge = LightBridge::new(
        RPC_ADDR.parse().unwrap(),
        TPU_ADDR.parse().unwrap(),
        CONNECTION_POOL_SIZE,
    );

    let rpc_client = light_bridge.thin_client.rpc_client();

    let mut wtr = csv::Writer::from_path("metrics.csv").unwrap();
    let mut metrics = Vec::new();

    let payer = create_and_confirm_new_account_with_funds(rpc_client, PAYER_BANK_BALANCE);

    // send transactions
    for _ in 0..NUMBER_OF_RUNS {
        let lastest_block_hash = rpc_client.get_latest_blockhash().unwrap();

        //
        // Send TX's
        //

        let send_start_time = Instant::now();

        let signatures = send_funds_to_random_accounts(
            &light_bridge,
            &payer,
            lastest_block_hash,
            NUMBER_OF_TXS,
            LAMPORTS_TO_SEND_PER_TX,
        );

        let time_to_send_txs = send_start_time.elapsed().as_millis();

        //
        // Confirm TX's
        //

        let confirm_start_time = Instant::now();

        confirm_transactions(rpc_client, signatures);

        let time_to_confirm_txs = confirm_start_time.elapsed().as_millis();

        metrics.push(Metrics {
            number_of_transactions: NUMBER_OF_TXS,
            time_to_send_txs,
            time_to_confirm_txs,
            duration: send_start_time.elapsed().as_millis(),
            tps: (NUMBER_OF_TXS / send_start_time.elapsed().as_secs()),
        });
    }

    for metric in metrics {
        wtr.serialize(metric).unwrap();
    }
}

fn create_and_confirm_new_account_with_funds(rpc_client: &RpcClient, funds: u64) -> Keypair {
    let payer = Keypair::new();
    let payer_pub_key = payer.pubkey();

    let airdrop_signature = rpc_client
        .request_airdrop(&payer_pub_key, funds)
        .expect("error requesting airdrop");

    confirm_transactions(rpc_client, vec![airdrop_signature]);

    payer
}

fn send_funds_to_random_accounts(
    light_bridge: &LightBridge,
    payer: &Keypair,
    latest_blockhash: solana_sdk::hash::Hash,
    number_of_accounts: u64,
    amount_to_send: u64,
) -> Vec<Signature> {
    let mut transaction_signatures = Vec::with_capacity(number_of_accounts as usize);

    for _ in 0..number_of_accounts {
        let to_pubkey = Pubkey::new_unique();
        let ix = system_instruction::transfer(&payer.pubkey(), &to_pubkey, amount_to_send);

        let tx = Transaction::new_signed_with_payer(
            &[ix],
            Some(&payer.pubkey()),
            &[payer],
            latest_blockhash,
        );

        let tx = BinaryEncoding::Base58.encode(bincode::serialize(&tx).unwrap());

        let signature = light_bridge
            .send_transaction(tx, SendTransactionConfig::default())
            .unwrap();

        transaction_signatures.push(Signature::from_str(&signature).unwrap());
    }

    transaction_signatures
}

fn confirm_transactions(rpc_client: &RpcClient, mut signatures: Vec<Signature>) {
    let mut signatures_to_retry = Vec::with_capacity(signatures.len());

    loop {
        for signature in signatures {
            eprintln!("confirming {}", signature);
            if rpc_client
                .confirm_transaction(&signature)
                .expect("Error confirming TX")
            {
                eprintln!("signature finalized: {signature}");
            } else {
                signatures_to_retry.push(signature);
            }
        }

        if signatures_to_retry.is_empty() {
            return;
        }

        signatures = signatures_to_retry.clone();
        signatures_to_retry.clear();

        std::thread::sleep(Duration::from_millis(1000));
    }
}
