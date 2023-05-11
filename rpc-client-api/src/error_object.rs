#[derive(Deserialize, Debug)]
pub struct RpcErrorObject {
    pub code: i32,
    pub message: String,
}
