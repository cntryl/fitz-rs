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

    pub fn put_u16(&mut self, value: u16) -> &mut Self {
        self.buf.extend_from_slice(&value.to_be_bytes());
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

    pub fn get_u16(&mut self) -> Result<u16> {
        self.check_len(2)?;
        let bytes = &self.buf[self.pos..self.pos + 2];
        self.pos += 2;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
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

pub const FRAME_HEADER_LEN: usize = 15;
pub const MAX_MESSAGE_SIZE: usize = 1_048_576;
pub const ACK_REQUESTED: u8 = 1;
pub const RESPONSE_FINAL: u8 = 2;
const VALID_FLAGS: u8 = ACK_REQUESTED | RESPONSE_FINAL;

#[cfg(test)]
pub(crate) fn test_hello_payload() -> Vec<u8> {
    let mut encoder = PayloadEncoder::new();
    encoder
        .put_u16(2)
        .put_u32(0)
        .put_u32(u32::try_from(MAX_MESSAGE_SIZE).expect("frame limit fits in u32"))
        .put_u32(256)
        .put_u64(1);
    encoder.finish()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub message_type: u16,
    pub flags: u8,
    pub correlation_id: u64,
    pub payload_start: usize,
}

/// Encode one Wire-2 message frame with an empty header correlation.
#[cfg(test)]
pub fn encode_message_frame(msg_type: u16, payload: &[u8]) -> Vec<u8> {
    try_encode_message_frame(msg_type, payload).expect("payload fits Wire-2 message limit")
}

/// Checked Wire-2 message frame encoding used by transport-facing code.
pub fn try_encode_message_frame(msg_type: u16, payload: &[u8]) -> Result<Vec<u8>> {
    try_encode_frame(msg_type, 0, 0, payload)
}

/// Encode one Wire-2 message with explicit flags and correlation.
pub fn try_encode_frame(
    msg_type: u16,
    flags: u8,
    correlation_id: u64,
    payload: &[u8],
) -> Result<Vec<u8>> {
    let message_size = FRAME_HEADER_LEN.saturating_add(payload.len());
    if message_size > MAX_MESSAGE_SIZE || u32::try_from(payload.len()).is_err() {
        return Err(FitzError::FrameTooLarge(message_size));
    }
    if flags & !VALID_FLAGS != 0 {
        return Err(FitzError::Protocol(format!(
            "unsupported frame flags {flags:#x}"
        )));
    }
    let mut frame = Vec::with_capacity(message_size);
    frame.extend_from_slice(&msg_type.to_be_bytes());
    frame.push(flags);
    frame.extend_from_slice(&correlation_id.to_be_bytes());
    frame.extend_from_slice(
        &u32::try_from(payload.len())
            .expect("payload length checked above")
            .to_be_bytes(),
    );
    frame.extend_from_slice(payload);
    Ok(frame)
}

/// Decode and validate one Wire-2 message header and exact payload length.
pub fn decode_frame_header(buf: &[u8]) -> Result<FrameHeader> {
    if buf.len() < FRAME_HEADER_LEN {
        return Err(FitzError::Codec("truncated Wire-2 frame header".into()));
    }
    let message_type = u16::from_be_bytes([buf[0], buf[1]]);
    let flags = buf[2];
    if flags & !VALID_FLAGS != 0 {
        return Err(FitzError::Protocol(format!(
            "unsupported frame flags {flags:#x}"
        )));
    }
    let correlation_id = u64::from_be_bytes(buf[3..11].try_into().expect("fixed header slice"));
    let payload_len =
        u32::from_be_bytes(buf[11..15].try_into().expect("fixed header slice")) as usize;
    let expected = FRAME_HEADER_LEN.saturating_add(payload_len);
    if expected > MAX_MESSAGE_SIZE {
        return Err(FitzError::FrameTooLarge(expected));
    }
    if buf.len() != expected {
        return Err(FitzError::Codec(
            "Wire-2 frame length does not match payload size".into(),
        ));
    }
    Ok(FrameHeader {
        message_type,
        flags,
        correlation_id,
        payload_start: FRAME_HEADER_LEN,
    })
}

/// Decode a message frame: returns (`msg_type`, `payload_start`).
#[cfg(test)]
pub fn decode_message_frame(buf: &[u8]) -> Result<(u16, usize)> {
    let header = decode_frame_header(buf)?;
    Ok((header.message_type, header.payload_start))
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
    fn should_decode_response_final_flag_given_correlated_terminal_response() {
        // Arrange
        let frame = try_encode_frame(104, RESPONSE_FINAL, 1, b"ok").unwrap();

        // Act
        let header = decode_frame_header(&frame).unwrap();

        // Assert
        assert_eq!(header.flags, RESPONSE_FINAL);
        assert_eq!(header.correlation_id, 1);
    }

    #[test]
    fn should_encode_message_type_zero_in_fixed_header() {
        // Arrange
        let payload = b"ok";
        // Act
        let frame = encode_message_frame(0, payload);
        let header = decode_frame_header(&frame).unwrap();
        // Assert
        assert_eq!(header.message_type, 0);
        assert_eq!(&frame[FRAME_HEADER_LEN..], payload);
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
        let payload = vec![0u8; MAX_MESSAGE_SIZE - FRAME_HEADER_LEN + 1];

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
        let mut frame = encode_message_frame(100, b"hi");
        frame.push(b'!');

        // Act
        let err = decode_message_frame(&frame).unwrap_err();

        // Assert
        assert_eq!(err.kind(), crate::error::FitzErrorKind::Protocol);
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
