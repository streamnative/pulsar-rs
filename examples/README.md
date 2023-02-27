# Examples

In order to run these examples, you will need a running Apache-Pulsar backend. You can start one with:

```bash
docker run -it \
  -p 6650:6650 \
  -p 8080:8080 \
  --mount source=pulsardata,target=/pulsar/data \
  --mount source=pulsarconf,target=/pulsar/conf \
  apachepulsar/pulsar:2.10.0 \
  bin/pulsar standalone
```

## Producer

```bash
RUST_LOG=info cargo run --release --example producer
```


## Consumer

```bash
$ RUST_LOG=info cargo run --release --example consumer
```
