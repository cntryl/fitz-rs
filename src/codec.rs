//! Payload codec: sequential typed fields for domain message bodies.
//!
//! Matches the server's `payload_codec` format (not TLV; fixed-order typed fields).
//! Frame-level encode/decode is in `encode_message_frame` / `decode_message_frame`.

use crate::error::{FitzError, Result};

/// Encoder for message payloads (sequential typed fields).
pub struct PayloadEncoder {
    buf: Vec<u8>,
}

impl PayloadEncoder {
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    pub fn put_u8(&mut self, value: u8) -> &mut Self {
        self.buf.push(value);
        self
    }

    pub fn put_u32(&mut self, value: u32) -> &mut Self {
        self.buf.extend_from_slice(&value.to_be_bytes());
        self
    }

    pub fn put_u64(&mut self, value: u64) -> &mut Self {
        self.buf.extend_from_slice(&value.to_be_bytes());
        self
    }

    pub fn put_bytes(&mut self, bytes: &[u8]) -> &mut Self {
        self.buf.extend_from_slice(
            &u32::try_from(bytes.len())
                .expect("byte field exceeds u32")
                .to_be_bytes(),
        );
        self.buf.extend_from_slice(bytes);
        self
    }

    pub fn put_raw(&mut self, bytes: &[u8]) -> &mut Self {
        self.buf.extend_from_slice(bytes);
        self
    }

    pub fn put_string(&mut self, s: &str) -> &mut Self {
        let bytes = s.as_bytes();
        self.buf.extend_from_slice(
            &u32::try_from(bytes.len())
                .expect("string field exceeds u32")
                .to_be_bytes(),
        );
        self.buf.extend_from_slice(bytes);
        self
    }

    pub fn finish(self) -> Vec<u8> {
        self.buf
    }
}

impl Default for PayloadEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Decoder for message payloads (sequential typed fields).
pub struct PayloadDecoder<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> PayloadDecoder<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn check_len(&self, n: usize) -> Result<()> {
        if self.pos + n > self.buf.len() {
            return Err(FitzError::Codec("Not enough bytes".to_string()));
        }
        Ok(())
    }

    pub fn get_u8(&mut self) -> Result<u8> {
        self.check_len(1)?;
        let value = self.buf[self.pos];
        self.pos += 1;
        Ok(value)
    }

    pub fn get_u32(&mut self) -> Result<u32> {
        self.check_len(4)?;
        let bytes = &self.buf[self.pos..self.pos + 4];
        self.pos += 4;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub fn get_u64(&mut self) -> Result<u64> {
        self.check_len(8)?;
        let bytes = &self.buf[self.pos..self.pos + 8];
        self.pos += 8;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    pub fn get_bytes(&mut self) -> Result<Vec<u8>> {
        let len = self.get_u32()? as usize;
        self.check_len(len)?;
        let bytes = self.buf[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(bytes)
    }

    pub fn get_fixed<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.check_len(N)?;
        let mut value = [0_u8; N];
        value.copy_from_slice(&self.buf[self.pos..self.pos + N]);
        self.pos += N;
        Ok(value)
    }

    pub fn get_string(&mut self) -> Result<String> {
        let bytes = self.get_bytes()?;
        String::from_utf8(bytes).map_err(|_| FitzError::Codec("Invalid UTF-8".to_string()))
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub fn is_empty(&self) -> bool {
        self.pos >= self.buf.len()
    }
}

/// Encode a message frame: [u16 `msg_type`][payload]
#[cfg(test)]
pub fn encode_message_frame(msg_type: u16, payload: &[u8]) -> Vec<u8> {
    assert!(
        u16::try_from(payload.len()).is_ok(),
        "payload exceeds u16 frame limit"
    );

    try_encode_message_frame(msg_type, payload).expect("payload length checked above")
}

/// Checked message frame encoding used by transport-facing code.
pub fn try_encode_message_frame(msg_type: u16, payload: &[u8]) -> Result<Vec<u8>> {
    if payload.len() > u16::MAX as usize {
        return Err(FitzError::FrameTooLarge(payload.len()));
    }

    let mut frame = Vec::new();

    // Encode message type (multi-byte for type ≥ 255)
    if msg_type < 255 {
        frame.push(u8::try_from(msg_type).expect("single-byte message type"));
    } else {
        frame.push(0xFF);
        frame.extend_from_slice(&msg_type.to_be_bytes());
    }

    // Encode length (u16 BE)
    frame.extend_from_slice(
        &u16::try_from(payload.len())
            .expect("payload size checked above")
            .to_be_bytes(),
    );

    // Append payload
    frame.extend_from_slice(payload);

    Ok(frame)
}

/// Decode a message frame: returns (`msg_type`, `payload_start`)
pub fn decode_message_frame(buf: &[u8]) -> Result<(u16, usize)> {
    if buf.is_empty() {
        return Err(FitzError::Codec("Empty frame".to_string()));
    }

    let (msg_type, header_len) = if buf[0] == 0xFF {
        if buf.len() < 3 {
            return Err(FitzError::Codec(
                "Incomplete multi-byte message type".to_string(),
            ));
        }
        let msg_type = u16::from_be_bytes([buf[1], buf[2]]);
        (msg_type, 3)
    } else {
        (u16::from(buf[0]), 1)
    };

    if buf.len() < header_len + 2 {
        return Err(FitzError::Codec("Incomplete length field".to_string()));
    }

    let len = u16::from_be_bytes([buf[header_len], buf[header_len + 1]]) as usize;
    let payload_start = header_len + 2;

    if buf.len() < payload_start + len {
        return Err(FitzError::Codec("Incomplete payload bytes".to_string()));
    }

    if buf.len() != payload_start + len {
        return Err(FitzError::Codec(
            "Frame length does not match payload size".to_string(),
        ));
    }

    Ok((msg_type, payload_start))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_round_trip_u64() {
        // Arrange
        let mut enc = PayloadEncoder::new();
        enc.put_u64(0x0123_4567_89AB_CDEF);
        let buf = enc.finish();

        // Act
        let mut dec = PayloadDecoder::new(&buf);
        let value = dec.get_u64().unwrap();

        // Assert
        assert_eq!(value, 0x0123_4567_89AB_CDEF);
    }

    #[test]
    fn should_round_trip_string() {
        // Arrange
        let mut enc = PayloadEncoder::new();
        enc.put_string("hello");
        let buf = enc.finish();

        // Act
        let mut dec = PayloadDecoder::new(&buf);
        let value = dec.get_string().unwrap();

        // Assert
        assert_eq!(value, "hello");
    }

    #[test]
    fn should_round_trip_bytes() {
        // Arrange
        let mut enc = PayloadEncoder::new();
        enc.put_bytes(b"data");
        let buf = enc.finish();

        // Act
        let mut dec = PayloadDecoder::new(&buf);
        let value = dec.get_bytes().unwrap();

        // Assert
        assert_eq!(value, b"data");
    }

    #[test]
    fn should_round_trip_message_type_boundary_given_single_byte_encoding_when_frame_decoded() {
        // Arrange
        let message_types = [0, 1, 254];

        // Act
        let decoded = message_types.map(|message_type| {
            let frame = encode_message_frame(message_type, b"hello");
            decode_message_frame(&frame).unwrap().0
        });

        // Assert
        assert_eq!(decoded, message_types);
    }

    #[test]
    fn should_decode_message_frame() {
        // Arrange
        let frame = encode_message_frame(100, b"hello");

        // Act
        let (msg_type, payload_start) = decode_message_frame(&frame).unwrap();

        // Assert
        assert_eq!(msg_type, 100);
        assert_eq!(&frame[payload_start..], b"hello");
    }

    #[test]
    fn should_encode_message_type_zero_as_single_byte() {
        let frame = encode_message_frame(0, b"ok");
        assert_eq!(frame[0], 0);
        assert_eq!(&frame[1..3], &[0, 2]);
    }

    #[test]
    fn should_round_trip_message_type_boundary_given_escaped_encoding_when_frame_decoded() {
        // Arrange
        let message_types = [255, 256, u16::MAX];

        // Act
        let decoded = message_types.map(|message_type| {
            let frame = encode_message_frame(message_type, b"ok");
            decode_message_frame(&frame).unwrap().0
        });

        // Assert
        assert_eq!(decoded, message_types);
    }

    #[test]
    fn should_reject_oversized_payload_given_value_above_frame_limit_when_encoding_attempted() {
        // Arrange
        let payload = vec![0u8; (u16::MAX as usize) + 1];

        // Act
        let err = try_encode_message_frame(100, &payload).unwrap_err();

        // Assert
        assert!(matches!(err, FitzError::FrameTooLarge(_)));
    }

    #[test]
    fn should_reject_truncated_headers_given_incomplete_frame_bytes_when_frame_decoded() {
        // Arrange
        let frames: [&[u8]; 4] = [&[], &[0xFF], &[0xFF, 0], &[100, 0]];

        // Act
        let results = frames.map(decode_message_frame);

        // Assert
        assert!(results.iter().all(Result::is_err));
    }

    #[test]
    fn should_reject_trailing_bytes_given_extra_frame_data_when_strict_decoding_used() {
        // Arrange
        let frame = vec![100, 0, 2, b'h', b'i', b'!'];

        // Act
        let err = decode_message_frame(&frame).unwrap_err();

        // Assert
        assert!(err.to_string().contains("Frame length does not match"));
    }

    #[test]
    fn should_reject_truncated_value_given_incomplete_length_prefixed_field_when_decoding_attempted()
     {
        // Arrange
        let bytes = [0, 0, 0, 4, b'a', b'b'];
        let mut decoder = PayloadDecoder::new(&bytes);

        // Act
        let error = decoder.get_bytes().unwrap_err();

        // Assert
        assert!(matches!(error, FitzError::Codec(_)));
    }
}
