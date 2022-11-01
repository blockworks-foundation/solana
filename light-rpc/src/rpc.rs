use {
    actix_web::{http::StatusCode, web, App, HttpResponse, HttpServer, Responder},
    serde::{Deserialize, Serialize},
    serde_json::json,
    solana_client::{
        connection_cache::ConnectionCache, rpc_config::RpcSendTransactionConfig,
        thin_client::ThinClient,
    },
    solana_sdk::{client::AsyncClient, signature::Signature, transport::TransportError},
    std::{
        net::{SocketAddr, ToSocketAddrs},
        sync::Arc,
    },
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RpcMethod {
    SendTransaction,
    RequestAirdrop,
}

/// According to <https://www.jsonrpc.org/specification#overview>
#[derive(Debug, Deserialize, Serialize)]
pub struct JsonRpcReq {
    pub method: RpcMethod,
    pub params: serde_json::Value,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum JsonRpcRes {
    Err(serde_json::Value),
    Ok(serde_json::Value),
}

impl Responder for JsonRpcRes {
    type Body = String;

    fn respond_to(self, _: &actix_web::HttpRequest) -> HttpResponse<Self::Body> {
        let mut res = json!({
            "jsonrpc" : "2.0",
            // TODO: add id
        });

        match self {
            Self::Err(error) => {
                res["error"] = error;
                HttpResponse::new(StatusCode::from_u16(500).unwrap()).set_body(res.to_string())
            }
            Self::Ok(result) => {
                res["result"] = result;
                HttpResponse::new(StatusCode::OK).set_body(res.to_string())
            }
        }
    }
}

impl<T: serde::Serialize> TryFrom<Result<T, JsonRpcError>> for JsonRpcRes {
    type Error = serde_json::Error;

    fn try_from(result: Result<T, JsonRpcError>) -> Result<Self, Self::Error> {
        Ok(match result {
            Ok(value) => Self::Ok(serde_json::to_value(value)?),
            // TODO: add custom handle
            Err(error) => Self::Err(serde_json::Value::String(format!("{error:?}"))),
        })
    }
}

#[derive(thiserror::Error, Debug)]
pub enum JsonRpcError {
    #[error("data store disconnected")]
    TransportError(#[from] TransportError),
    #[error("data store disconnected")]
    Base58DecodeError(#[from] bs58::decode::Error),
    #[error("data store disconnected")]
    BincodeDeserializeError(#[from] bincode::Error),
    #[error("data store disconnected")]
    SerdeError(#[from] serde_json::Error),
}
