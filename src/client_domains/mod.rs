pub mod kv;
pub mod lease;
pub mod notice;
pub mod queue;
pub mod rpc;
pub mod schedule;
pub mod stream;

use crate::codec::PayloadDecoder;
use crate::{FitzError, Result};

pub(crate) fn decode_domain_error(decoder: &mut PayloadDecoder<'_>) -> Result<FitzError> {
    let code = decoder.get_u16()?;
    let class = decoder.get_u8()?;
    let effect = decoder.get_u8()?;
    let message = decoder.get_string()?;
    if class > 10 || effect > 2 || !decoder.is_empty() {
        return Err(FitzError::Protocol(
            "invalid common domain error envelope".into(),
        ));
    }
    Ok(FitzError::DomainWire {
        code,
        class,
        effect,
        message,
    })
}

fn decode_ok(response: &[u8]) -> Result<()> {
    let mut decoder = PayloadDecoder::new(response);
    match decoder.get_u8()? {
        0 => Ok(()),
        1 => Err(decode_domain_error(&mut decoder)?),
        status => Err(FitzError::Protocol(format!(
            "unknown response status {status}"
        ))),
    }
}
