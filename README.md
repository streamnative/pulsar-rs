# pulsar-rs: Future-based Rust client for [Apache Pulsar](https://pulsar.apache.org/)

[![crates](https://img.shields.io/crates/v/pulsar.svg)](https://crates.io/crates/pulsar)
[![docs](https://img.shields.io/docsrs/pulsar)](https://docs.rs/pulsar)

This is a pure Rust client for Apache Pulsar that does not depend on the C++ Pulsar library. It provides an async/await based API, compatible with [Tokio](https://tokio.rs/) and [async-std](https://async.rs/).

Features:

- URL based (`pulsar://` and `pulsar+ssl://`) connections with DNS lookup;
- Multi topic consumers (based on a regex or list);
- TLS connection;
- Configurable executor (Tokio or async-std);
- Automatic reconnection with exponential back off;
- Message batching;
- Compression with LZ4, zlib, zstd or Snappy (can be deactivated with Cargo features);
- Telemetry using [tracing](https://github.com/tokio-rs/tracing) crate (can be activated with Cargo features).

## Getting Started

Add the following dependencies in your `Cargo.toml`:

```toml
futures = "0.3"
pulsar = "5.1"
tokio = "1.0"
```

Try out [examples](examples):

- [producer](examples/producer.rs)
- [consumer](examples/consumer.rs)
- [reader](examples/reader.rs)

## Project Maintainers

- [@CleverAkanoa](https://github.com/CleverAkanoa)
- [@DonghunLouisLee](https://github.com/DonghunLouisLee)
- [@FlorentinDUBOIS](https://github.com/FlorentinDUBOIS)
- [@Geal](https://github.com/Geal)
- [@fantapsody](https://github.com/fantapsody)
- [@freeznet](https://github.com/freeznet)
- [@stearnsc](https://github.com/stearnsc)

## Contribution

This project welcomes your PR and issues. For example, refactoring, adding features, correcting English, etc.

Thanks to all the people who already contributed!

<a href="https://github.com/streamnative/pulsar-rs/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=streamnative/pulsar-rs" />
</a>

## License

This library is licensed under the terms of both the MIT license and the Apache License (Version 2.0), and may include packages written by third parties which carry their own copyright notices and license terms.

See [LICENSE-APACHE](LICENSE-APACHE), [LICENSE-MIT](LICENSE-MIT), and [COPYRIGHT](COPYRIGHT) for details.

## History

This project is originally created by [@stearnsc](https://github.com/stearnsc) and others at [Wyyerd](https://github.com/wyyerd) at 2018. Later at 2022, the orginal creators [decided to transfer the repository to StreamNative](https://github.com/streamnative-oss/sn-pulsar-rs/issues/20).

Currently, this project is actively maintained under the StreamNative organization with a diverse [maintainers group](#project-maintainers).

## About StreamNative

Founded in 2019 by the original creators of Apache Pulsar, [StreamNative](https://streamnative.io/) is one of the leading contributors to the open-source Apache Pulsar project. We have helped engineering teams worldwide make the move to Pulsar with [StreamNative Cloud](https://streamnative.io/product), a fully managed service to help teams accelerate time-to-production.
