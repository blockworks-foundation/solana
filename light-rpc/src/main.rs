use light_rpc::bridge::LightBridge;

const RPC_ADDR: &str = "127.0.0.1:8899";
const TPU_ADDR: &str = "127.0.0.1:1027";
const CONNECTION_POOL_SIZE: usize = 2000;

#[tokio::main]
pub async fn main() -> Result<(), std::io::Error> {
    let light_bridge = LightBridge::new(
        RPC_ADDR.parse().unwrap(),
        TPU_ADDR.parse().unwrap(),
        CONNECTION_POOL_SIZE,
    );

    light_bridge.start_server("127.0.0.1:8890").await
}
