//! Compression strategy configs

use lz4::block::CompressionMode;

#[derive(Clone, Debug)]
pub enum Compression {
    None,
    Lz4(CompressionLz4),
    Zlib(CompressionZlib),
    Zstd(CompressionZstd),
    Snappy,
}

#[derive(Debug)]
pub struct CompressionLz4 {
    pub mode: CompressionMode,
}

#[derive(Clone, Copy, Debug)]
pub struct CompressionZlib {
    pub level: u32,
}

#[derive(Clone, Copy, Debug)]
pub struct CompressionZstd {
    pub level: i32,
}

impl Default for Compression {
    fn default() -> Self {
        Compression::None
    }
}

impl Default for CompressionLz4 {
    fn default() -> Self {
        CompressionLz4 {
            mode: CompressionMode::DEFAULT,
        }
    }
}

impl Default for CompressionZlib {
    fn default() -> Self {
        CompressionZlib {
            level: flate2::Compression::default().level(),
        }
    }
}

impl Default for CompressionZstd {
    fn default() -> Self {
        CompressionZstd {
            level: zstd::DEFAULT_COMPRESSION_LEVEL,
        }
    }
}

impl Clone for CompressionLz4 {
    fn clone(&self) -> Self {
        CompressionLz4 {
            mode: match self.mode {
                CompressionMode::HIGHCOMPRESSION(i) => CompressionMode::HIGHCOMPRESSION(i),
                CompressionMode::FAST(i) => CompressionMode::FAST(i),
                CompressionMode::DEFAULT => CompressionMode::DEFAULT,
            }
        }
    }
}
