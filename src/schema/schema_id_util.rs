/// Magic byte for single-schema framing (PIP-420).
pub const MAGIC_BYTE_VALUE: u8 = 0xFF;
/// Magic byte for key-value schema framing (PIP-420).
pub const MAGIC_BYTE_KEY_VALUE: u8 = 0xFE;

/// Parsed schema ID information from magic-byte framed data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaIdInfo {
    Single(Vec<u8>),
    KeyValue { key_id: Vec<u8>, value_id: Vec<u8> },
}

pub fn add_magic_header(schema_id: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(1 + schema_id.len());
    result.push(MAGIC_BYTE_VALUE);
    result.extend_from_slice(schema_id);
    result
}

/// Parse a magic-byte-framed schema ID from raw bytes.
///
/// Returns:
/// - `Ok(None)` — no magic byte present (data has no schema framing)
/// - `Ok(Some(info))` — valid framing parsed successfully
/// - `Err(...)` — magic byte present but data is truncated / corrupt
pub fn strip_magic_header(data: &[u8]) -> Result<Option<SchemaIdInfo>, crate::Error> {
    if data.is_empty() {
        return Ok(None);
    }
    match data[0] {
        MAGIC_BYTE_VALUE => Ok(Some(SchemaIdInfo::Single(data[1..].to_vec()))),
        MAGIC_BYTE_KEY_VALUE => {
            if data.len() < 5 {
                return Err(crate::Error::Custom(format!(
                    "corrupt KV schema_id: expected at least 5 bytes, got {}",
                    data.len()
                )));
            }
            let key_len = u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
            if data.len() < 5 + key_len {
                return Err(crate::Error::Custom(format!(
                    "corrupt KV schema_id: key_len={} but only {} bytes remain",
                    key_len,
                    data.len() - 5
                )));
            }
            let key_id = data[5..5 + key_len].to_vec();
            let value_id = data[5 + key_len..].to_vec();
            Ok(Some(SchemaIdInfo::KeyValue { key_id, value_id }))
        }
        _ => Ok(None),
    }
}

/// Strip the single-value magic prefix (`0xFF`) from an inner schema ID.
///
/// Inner `PulsarSchema::encode()` returns schema IDs framed with the `0xFF`
/// magic byte (via `add_magic_header`).  When composing a key-value schema ID,
/// we need the **raw** inner IDs — the KV frame (`0xFE`) provides its own
/// length-delimited envelope, so the inner `0xFF` prefix must be removed to
/// avoid double-framing.
///
/// Returns the raw bytes after stripping. If the prefix is absent the input is
/// returned unchanged (defensive — inner schemas *should* always frame).
pub fn strip_single_magic_prefix(data: &[u8]) -> &[u8] {
    if data.first() == Some(&MAGIC_BYTE_VALUE) {
        &data[1..]
    } else {
        data
    }
}

/// Build a KV-framed schema ID from **raw** (unframed) inner schema IDs.
///
/// Callers must strip the inner `0xFF` magic prefix before calling this
/// function — use [`strip_single_magic_prefix`] on values returned by inner
/// `PulsarSchema::encode()`.
///
/// Wire format: `[0xFE] [4-byte key_len BE] [raw_key_id] [raw_value_id]`
pub fn generate_kv_schema_id(
    key_schema_id: Option<&[u8]>,
    value_schema_id: Option<&[u8]>,
) -> Option<Vec<u8>> {
    if key_schema_id.is_none() && value_schema_id.is_none() {
        return None;
    }
    let key_bytes = key_schema_id.unwrap_or(&[]);
    let val_bytes = value_schema_id.unwrap_or(&[]);
    let key_len = key_bytes.len() as u32;

    let mut result = Vec::with_capacity(1 + 4 + key_bytes.len() + val_bytes.len());
    result.push(MAGIC_BYTE_KEY_VALUE);
    result.extend_from_slice(&key_len.to_be_bytes());
    result.extend_from_slice(key_bytes);
    result.extend_from_slice(val_bytes);
    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_magic_header() {
        let id = vec![0x00, 0x00, 0x00, 0x01]; // schema ID = 1
        let framed = add_magic_header(&id);
        assert_eq!(framed[0], MAGIC_BYTE_VALUE);
        assert_eq!(&framed[1..], &id);
    }

    #[test]
    fn test_add_magic_header_empty() {
        let framed = add_magic_header(&[]);
        assert_eq!(framed, vec![MAGIC_BYTE_VALUE]);
    }

    #[test]
    fn test_strip_magic_header_single() {
        let id = vec![0x00, 0x00, 0x00, 0x05];
        let framed = add_magic_header(&id);
        let info = strip_magic_header(&framed).unwrap().unwrap();
        assert_eq!(info, SchemaIdInfo::Single(id));
    }

    #[test]
    fn test_strip_magic_header_empty_input() {
        assert!(strip_magic_header(&[]).unwrap().is_none());
    }

    #[test]
    fn test_strip_magic_header_no_magic() {
        assert!(strip_magic_header(&[0x00, 0x01, 0x02]).unwrap().is_none());
    }

    #[test]
    fn test_strip_magic_header_corrupt_kv() {
        // KV magic byte but truncated data — should return Err, not None
        let data = vec![MAGIC_BYTE_KEY_VALUE, 0x00];
        assert!(strip_magic_header(&data).is_err());
    }

    #[test]
    fn test_strip_magic_header_corrupt_kv_key_len() {
        // KV magic byte, valid key_len=10 but only 2 bytes of data after header
        let data = vec![MAGIC_BYTE_KEY_VALUE, 0x00, 0x00, 0x00, 0x0A, 0x01, 0x02];
        assert!(strip_magic_header(&data).is_err());
    }

    #[test]
    fn test_generate_kv_schema_id_both() {
        let key_id = vec![0x00, 0x00, 0x00, 0x01];
        let val_id = vec![0x00, 0x00, 0x00, 0x02];
        let framed = generate_kv_schema_id(Some(&key_id), Some(&val_id)).unwrap();
        // [0xFE] [4-byte key_len BE] [key_id] [value_id]
        assert_eq!(framed[0], MAGIC_BYTE_KEY_VALUE);
        let key_len = u32::from_be_bytes([framed[1], framed[2], framed[3], framed[4]]);
        assert_eq!(key_len, 4);
        assert_eq!(&framed[5..9], &key_id);
        assert_eq!(&framed[9..], &val_id);
    }

    #[test]
    fn test_generate_kv_schema_id_key_only() {
        let key_id = vec![0x00, 0x00, 0x00, 0x03];
        let framed = generate_kv_schema_id(Some(&key_id), None).unwrap();
        assert_eq!(framed[0], MAGIC_BYTE_KEY_VALUE);
        let key_len = u32::from_be_bytes([framed[1], framed[2], framed[3], framed[4]]);
        assert_eq!(key_len, 4);
        assert_eq!(&framed[5..9], &key_id);
        assert!(framed[9..].is_empty());
    }

    #[test]
    fn test_generate_kv_schema_id_neither() {
        assert_eq!(generate_kv_schema_id(None, None), None);
    }

    #[test]
    fn test_roundtrip_kv() {
        let key_id = vec![0x01, 0x02];
        let val_id = vec![0x03, 0x04, 0x05];
        let framed = generate_kv_schema_id(Some(&key_id), Some(&val_id)).unwrap();
        let info = strip_magic_header(&framed).unwrap().unwrap();
        assert_eq!(
            info,
            SchemaIdInfo::KeyValue {
                key_id,
                value_id: val_id,
            }
        );
    }

    #[test]
    fn test_strip_single_magic_prefix_with_prefix() {
        let framed = add_magic_header(&[0x00, 0x01]);
        assert_eq!(strip_single_magic_prefix(&framed), &[0x00, 0x01]);
    }

    #[test]
    fn test_strip_single_magic_prefix_without_prefix() {
        let raw = vec![0x00, 0x01];
        assert_eq!(strip_single_magic_prefix(&raw), &[0x00, 0x01]);
    }

    #[test]
    fn test_strip_single_magic_prefix_empty() {
        assert_eq!(strip_single_magic_prefix(&[]), &[] as &[u8]);
    }

    #[test]
    fn test_strip_single_magic_prefix_only_magic_byte() {
        assert_eq!(strip_single_magic_prefix(&[MAGIC_BYTE_VALUE]), &[] as &[u8]);
    }

    #[test]
    fn test_strip_single_magic_prefix_kv_magic_untouched() {
        // KV magic byte (0xFE) should NOT be stripped
        let data = vec![MAGIC_BYTE_KEY_VALUE, 0x01, 0x02];
        assert_eq!(strip_single_magic_prefix(&data), data.as_slice());
    }
}
