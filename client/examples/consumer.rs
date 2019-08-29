extern crate pulsar;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;

use futures::{
    future::{join_all, Future},
    Stream,
};
use std::net::SocketAddr;
use tokio::runtime::Runtime;

use pulsar::{message::Payload, Authentication, DeserializeMessage, Pulsar, SubType};

#[derive(Debug, Deserialize)]
pub struct Data {
    hello: bool,
    world: String,
}

impl DeserializeMessage for Data {
    type Output = Result<Self, serde_json::Error>;

    fn deserialize_message(payload: Payload) -> Self::Output
    where
        Self: std::marker::Sized,
    {
        serde_json::from_slice(&payload.data)
    }
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
            client.create_partitioned_consumers::<Data, _, _>(
                topic,
                "my_subscriber",
                SubType::Shared,
            )
        })
        .and_then(|mut v| {
            info!("created {} consumers", v.len());

            let consumers = v
                .drain(..)
                .enumerate()
                .map(|(i, c)| {
                    info!("will apply for_each on consumer {}", i);
                    c.for_each(
                        move |msg: pulsar::Message<Result<Data, serde_json::Error>>| {
                            info!("got message: {:?}", msg.payload);
                            msg.ack.ack();
                            Ok(())
                        },
                    )
                })
                .collect::<Vec<_>>();

            join_all(consumers).map_err(|e| e.into()).map(|val| {
                println!("consumers returned: {:?}", val);
            })
        })
        .map_err(|e| {
            println!("got error: {:?}", e);
            e
        })
        .wait()
        .unwrap();

    runtime.shutdown_on_idle().wait().unwrap();

    println!("finished");
}
