use crate::codec::{MAX_MESSAGE_SIZE, PayloadDecoder, PayloadEncoder};
use crate::{FitzError, Result};

pub(crate) const EXTENSIONS_PRESENT: u8 = 4;
pub(crate) const CLIENT_CAPABILITIES: [u8; 10] = [0, 2, 0, 0, 0, 1, 0, 0, 0, 2];

fn malformed() -> FitzError {
    FitzError::Protocol("malformed Wire 3 extension envelope".into())
}

pub(crate) fn encode(core: &[u8], fields: &[(u16, &[u8])]) -> Result<Vec<u8>> {
    let mut encoder = PayloadEncoder::new();
    encoder.put_bytes(core);
    let mut previous = 0;
    for &(id, value) in fields {
        if id <= previous {
            return Err(malformed());
        }
        encoder.put_u16(id).put_bytes(value);
        previous = id;
    }
    if fields.is_empty() {
        return Err(malformed());
    }
    let payload = encoder.finish();
    if payload.len().saturating_add(15) > MAX_MESSAGE_SIZE {
        return Err(FitzError::FrameTooLarge(payload.len() + 15));
    }
    Ok(payload)
}

// Unknown extension values stay borrowed and are never copied or retained.
pub(crate) fn decode(payload: &[u8], message_type: u16, flags: u8) -> Result<(&[u8], u16)> {
    let handshake = message_type == crate::protocol::message_type::HELLO;
    if flags & EXTENSIONS_PRESENT == 0 {
        return if handshake {
            Err(malformed())
        } else {
            Ok((payload, 0))
        };
    }
    let mut decoder = PayloadDecoder::new(payload);
    let core_length = usize::try_from(decoder.get_u32()?).map_err(|_| malformed())?;
    let core_end = 4_usize.checked_add(core_length).ok_or_else(malformed)?;
    if core_end >= payload.len() {
        return Err(malformed());
    }
    let mut cursor = core_end;
    let mut capabilities_seen = false;
    let mut mask = 0;
    while cursor < payload.len() {
        let mut field = PayloadDecoder::new(&payload[cursor..]);
        let id = field.get_u16()?;
        let length = usize::try_from(field.get_u32()?).map_err(|_| malformed())?;
        cursor += 6;
        if id == 0 || length > payload.len() - cursor {
            return Err(malformed());
        }
        if handshake && id == 1 {
            if capabilities_seen || length < 2 {
                return Err(malformed());
            }
            capabilities_seen = true;
            let mut list = PayloadDecoder::new(&payload[cursor..cursor + length]);
            let count = usize::from(list.get_u16()?);
            if length != 2 + 4 * count {
                return Err(malformed());
            }
            let mut previous = 0;
            for _ in 0..count {
                let value = list.get_u32()?;
                if value <= previous {
                    return Err(malformed());
                }
                previous = value;
                if value == 1 || value == 2 {
                    mask |= 1 << value;
                }
            }
        }
        cursor += length;
    }
    if handshake && !capabilities_seen {
        return Err(malformed());
    }
    Ok((&payload[4..core_end], mask))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_decode_shared_wire_three_vectors() {
        // Arrange
        let vectors = include_str!("../tests/fixtures/wire3_vectors.txt");
        // Act
        for line in vectors.lines() {
            let (name, hex) = line.split_once(' ').unwrap();
            let frame: Vec<u8> = hex
                .as_bytes()
                .as_chunks::<2>()
                .0
                .iter()
                .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                .collect();
            let header = crate::codec::decode_frame_header(&frame).unwrap();
            let (core, mask) = decode(&frame[15..], header.message_type, header.flags).unwrap();
            // Assert
            assert!(!core.is_empty(), "{name}");
            if name == "hello_wire3_queue_capabilities" {
                assert_eq!(mask, 6);
            }
        }
    }

    #[test]
    fn should_reject_malformed_envelopes() {
        // Arrange
        let inputs: &[&[u8]] = &[
            &[],
            &[0, 0, 0, 0],
            &[0, 0, 0, 9],
            &[0, 0, 0, 0, 1],
            &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            &[0, 0, 0, 0, 0, 9, 255, 255, 255, 255],
        ];
        // Act
        let rejected = inputs
            .iter()
            .all(|payload| decode(payload, 104, 4).is_err());
        // Assert
        assert!(rejected);
    }

    #[test]
    fn should_reject_duplicate_hello_capabilities() {
        // Arrange
        let payload = [0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0];
        // Act
        let result = decode(&payload, 2, 4);
        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_borrow_core_only_payload() {
        // Arrange
        let payload = b"hi";
        // Act
        let (core, _) = decode(payload, 104, 0).unwrap();
        // Assert
        assert_eq!(payload.as_ptr(), core.as_ptr());
    }
}
