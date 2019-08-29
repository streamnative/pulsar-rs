extern crate pulsar;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;

use futures::future::Future;
use pulsar::{Authentication, Pulsar, ProducerOptions};
use std::net::SocketAddr;
use tokio::runtime::Runtime;

#[derive(Debug, Serialize)]
pub struct Data {
    hello: bool,
    world: String,
}

fn main() {
    let mut args = std::env::args();
    let _ = args.next();

    let (topic, address) = match (args.next(), args.next()) {
        (Some(t), Some(a)) => {
            let address: SocketAddr = a.parse().expect("could not parse Pulsar broker address");
            (t, address)
        }
        (None, _) => panic!("missing topic"),
        (_, None) => panic!("missing pulsar address"),
    };

    let auth = args.next().map(|token| Authentication {
        name: String::from("token"),
        data: token.into(),
    });

    let runtime = Runtime::new().unwrap();

    Pulsar::new(address, auth, runtime.executor())
        .and_then(|client| {
            client.send_json(
                topic,
                &Data {
                    hello: true,
                    world: "!".to_string(),
                },
                None,
                ProducerOptions::default()
            )
        })
        .wait()
        .unwrap();
    runtime.shutdown_now().wait().unwrap();
}
