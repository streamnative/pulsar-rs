//! Compression strategy configs

use lz4::block::CompressionMode;

/// Wrapper of supported compression algorithms
#[derive(Clone, Debug)]
pub enum Compression {
    None,
    Lz4(CompressionLz4),
    Zlib(CompressionZlib),
    Zstd(CompressionZstd),
    Snappy,
}

/// Options of the [lz4](https://lz4.github.io/lz4/) algorithm
#[derive(Debug)]
pub struct CompressionLz4 {
    /// compression mode of lz4 to be used
    pub mode: CompressionMode,
}

/// Options of the [zlib](https://www.zlib.net/) algorithm
#[derive(Default, Clone, Copy, Debug)]
pub struct CompressionZlib {
    /// compression level of zlib to be used (0-9)
    pub level: flate2::Compression,
}

/// Options of the [zstd](http://facebook.github.io/zstd/zstd_manual.html) algorithm
#[derive(Clone, Copy, Debug)]
pub struct CompressionZstd {
    /// compression level of zstd to be used ([`zstd::compression_level_range()`])
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
