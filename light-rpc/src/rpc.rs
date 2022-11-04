use crate::{configs::SendTransactionConfig, encoding::BinaryCodecError};

use actix_web::error::JsonPayloadError;
use actix_web::{http::StatusCode, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use serde_json::json;
use solana_sdk::transport::TransportError;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "method", content = "params")]
pub enum RpcMethod {
    SendTransaction(String, #[serde(default)] SendTransactionConfig),
    GetVersion,
}

/// According to <https://www.jsonrpc.org/specification#overview>
#[derive(Debug, Deserialize, Serialize)]
pub struct JsonRpcReq {
    pub method: RpcMethod,
    pub params: Vec<serde_json::Value>,
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
    #[error("TransportError {0}")]
    TransportError(#[from] TransportError),
    #[error("BinaryCodecError {0}")]
    BinaryCodecError(#[from] BinaryCodecError),
    #[error("BincodeDeserializeError {0}")]
    BincodeDeserializeError(#[from] bincode::Error),
    #[error("SerdeError {0}")]
    SerdeError(#[from] serde_json::Error),
    #[error("JsonPayloadError {0}")]
    JsonPayloadError(#[from] JsonPayloadError),
}
