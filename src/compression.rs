//! Compression strategy configs

use lz4::block::CompressionMode;

/// Wrapper of supported compression algorithms
#[derive(Default, Debug)]
pub enum Compression {
    #[default]
    None,
    /// Options of the [lz4](https://lz4.github.io/lz4/) algorithm
    Lz4 {
        /// compression mode of lz4 to be used
        mode: CompressionMode
    },
    /// Options of the [zlib](https://www.zlib.net/) algorithm
    Zlib {
        /// compression level of zlib to be used (0-9)
        level: flate2::Compression,
    },
    /// Options of the [zstd](http://facebook.github.io/zstd/zstd_manual.html) algorithm
    Zstd {
        /// compression level of zstd to be used ([`zstd::compression_level_range()`])
        level: i32,
    },
    /// Options of the [snappy](http://google.github.io/snappy/) algorithm
    Snappy,
}

// TODO replace with `derive` once lz4 upstream releases https://github.com/10XGenomics/lz4-rs/pull/30.
impl Clone for Compression {
    fn clone(&self) -> Self {
        match self {
            Compression::None => Compression::None,
            Compression::Lz4 { mode } => Compression::Lz4 {
                mode: match mode {
                    CompressionMode::HIGHCOMPRESSION(i) => CompressionMode::HIGHCOMPRESSION(*i),
                    CompressionMode::FAST(i) => CompressionMode::FAST(*i),
                    CompressionMode::DEFAULT => CompressionMode::DEFAULT,
                }
            },
            Compression::Zlib { level } => Compression::Zlib { level: *level },
            Compression::Zstd { level } => Compression::Zstd { level: *level },
            Compression::Snappy => Compression::Snappy
        }
    }
}
