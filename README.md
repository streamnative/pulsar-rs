## pulsar-rs: Future-based Rust client for [Apache Pulsar](https://pulsar.apache.org/)

[![crates](https://img.shields.io/crates/v/pulsar.svg)](https://crates.io/crates/pulsar)
[![docs](https://img.shields.io/docsrs/pulsar)](https://docs.rs/pulsar)

[Documentation](https://docs.rs/pulsar)

This is a pure Rust client for Apache Pulsar that does not depend on the C++ Pulsar library. It provides an async/await based API, compatible with [Tokio](https://tokio.rs/) and [async-std](https://async.rs/).

Features:

- URL based (`pulsar://` and `pulsar+ssl://`) connections with DNS lookup
- multi topic consumers (based on a regex or list)
- TLS connection
- configurable executor (Tokio or async-std)
- automatic reconnection with exponential back off
- message batching
- compression with LZ4, zlib, zstd or Snappy (can be deactivated with Cargo features)
- telemetry using [tracing](https://github.com/tokio-rs/tracing) crate (can be activated with Cargo features)

### Getting Started

Add the following dependencies in your `Cargo.toml`:

```toml
futures = "0.3"
pulsar = "4.0"
tokio = "1.0"
```

Try out [examples](examples):

- [producer](examples/producer.rs)
- [consumer](examples/consumer.rs)
- [reader](examples/reader.rs)

### Project Maintainers

- [@CleverAkanoa](https://github.com/CleverAkanoa)
- [@DonghunLouisLee](https://github.com/DonghunLouisLee)
- [@FlorentinDUBOIS](https://github.com/FlorentinDUBOIS)
- [@Geal](https://github.com/Geal)
- [@fantapsody](https://github.com/fantapsody)
- [@freeznet](https://github.com/freeznet)
- [@stearnsc](https://github.com/stearnsc)
- [@tisonkun](https://github.com/tisonkun)

### Contribution

This project welcomes your PR and issues. For example, refactoring, adding features, correcting English, etc.

Thanks to all the people who already contributed!

<a href="https://github.com/streamnative/pulsar-rs/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=streamnative/pulsar-rs" />
</a>

### License

This library is licensed under the terms of both the MIT license and the Apache License (Version 2.0), and may include packages written by third parties which carry their own copyright notices and license terms.

See [LICENSE-APACHE](LICENSE-APACHE), [LICENSE-MIT](LICENSE-MIT), and [COPYRIGHT](COPYRIGHT) for details.
