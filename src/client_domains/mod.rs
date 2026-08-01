pub mod kv;
pub mod lease;
pub mod notice;
pub mod queue;
pub mod rpc;
pub mod schedule;
pub mod stream;

use crate::codec::PayloadDecoder;
use crate::{FitzError, Result};

fn decode_ok(response: &[u8]) -> Result<()> {
    let mut decoder = PayloadDecoder::new(response);
    match decoder.get_u8()? {
        0 => Ok(()),
        1 => Err(FitzError::Domain {
            code: decoder.get_u32()?,
            message: decoder.get_string()?,
        }),
        status => Err(FitzError::Protocol(format!(
            "unknown response status {status}"
        ))),
    }
}
