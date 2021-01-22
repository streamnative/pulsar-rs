## How to compile

- `git clone https://github.com/apache/pulsar` into the repository `target/` folder.
- `cargo run` in the compiler folder.

Or, if you already have pulsar cloned, you can run `PULSAR_DIR={PULSAR_LOCATION} cargo run` instead.

The resultant structs will be created in the `proto/src/prost` folder. Build the `pulsar-proto` library.