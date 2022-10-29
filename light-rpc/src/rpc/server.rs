use crate::bridge::LightBridge;
use actix_web::{post, web, App, HttpResponse, HttpServer, Responder};
use serde::{Deserialize, Serialize};
use std::net::ToSocketAddrs;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::methods::RpcMethod;

pub type Params = Vec<serde_json::Value>;

/// According to <https://www.jsonrpc.org/specification#overview>
#[derive(Debug, Deserialize, Serialize)]
pub struct JsonRpcReq {
    pub method: RpcMethod,
    pub params: Params,
}

impl JsonRpcReq {
    pub async run(&self) {
    }
}

#[post("/")]
async fn index(json_rpc_req: web::Json<JsonRpcReq>) -> impl Responder {
    let JsonRpcReq { method, params } = json_rpc_req.0;
    println!("{method:?}");
    HttpResponse::Ok()
}

pub struct Rpc {
    pub light_bridge: LightBridge,
}

pub type RpcState = Arc<RwLock<Rpc>>;

impl Rpc {
    pub async fn run(self, addr: impl ToSocketAddrs) -> Result<(), std::io::Error> {
        let rc = Arc::new(tokio::sync::RwLock::new(self));

        HttpServer::new(move || App::new().app_data(rc.clone()).service(index))
            .bind(addr)?
            .run()
            .await
    }
}
