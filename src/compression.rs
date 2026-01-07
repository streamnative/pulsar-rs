//! Compression strategy configs

/// Wrapper of supported compression algorithms
#[derive(Default, Clone, Debug)]
pub enum Compression {
    #[default]
    None,
    #[cfg(feature = "lz4")]
    Lz4(CompressionLz4),
    #[cfg(feature = "flate2")]
    Zlib(CompressionZlib),
    #[cfg(feature = "zstd")]
    Zstd(CompressionZstd),
    #[cfg(feature = "snap")]
    Snappy(CompressionSnappy),
}

/// Options of the [lz4](https://lz4.github.io/lz4/) algorithm
#[cfg(feature = "lz4")]
#[derive(Debug, Default, Clone)]
pub struct CompressionLz4 {
    /// compression mode of lz4 to be used
    pub mode: lz4::block::CompressionMode,
}

/// Options of the [zlib](https://www.zlib.net/) algorithm
#[cfg(feature = "flate2")]
#[derive(Default, Clone, Copy, Debug)]
pub struct CompressionZlib {
    /// compression level of zlib to be used (0-9)
    pub level: flate2::Compression,
}

/// Options of the [zstd](http://facebook.github.io/zstd/zstd_manual.html) algorithm
#[cfg(feature = "zstd")]
#[derive(Clone, Copy, Debug)]
pub struct CompressionZstd {
    /// compression level of zstd to be used ([`zstd::compression_level_range()`])
    pub level: i32,
}

#[cfg(feature = "zstd")]
impl Default for CompressionZstd {
    fn default() -> Self {
        CompressionZstd {
            level: zstd::DEFAULT_COMPRESSION_LEVEL,
        }
    }
}

/// Options of the [snappy](http://google.github.io/snappy/) algorithm
#[cfg(feature = "snap")]
#[derive(Default, Clone, Copy, Debug)]
pub struct CompressionSnappy {
    // empty for extensions
}
