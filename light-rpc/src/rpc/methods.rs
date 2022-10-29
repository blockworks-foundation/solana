use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RpcMethod {
    SendTransaction,
    RequestAirdrop
}

impl RpcMethod {
    
}
