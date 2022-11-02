use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum BinaryEncoding {
    #[default]
    Base58,
    Base64,
}

#[derive(thiserror::Error, Debug)]
pub enum BinaryCodecError {
    #[error("data store disconnected")]
    Base58DecodeError(#[from] bs58::decode::Error),
    #[error("data store disconnected")]
    Base58EncodeError(#[from] bs58::encode::Error),
}

impl BinaryEncoding {
    pub fn decode<D: AsRef<[u8]>>(&self, to_decode: D) -> Result<Vec<u8>, BinaryCodecError> {
        match self {
            Self::Base58 => Ok(bs58::decode(to_decode).into_vec()?),
            Self::Base64 => todo!(),
        }
    }

    pub fn encode<E: AsRef<[u8]>>(&self, to_encode: E) -> Vec<u8> {
        match self {
            Self::Base58 => bs58::encode(to_encode).into_vec(),
            Self::Base64 => todo!(),
        }
    }
}
