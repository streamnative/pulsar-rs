#[cfg(test)]
#[macro_use]
extern crate serde_derive;

use futures::Future;
use pulsar::{Connection, Producer};
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct ConnectionManager {
    addr: String,
    executor: tokio::runtime::TaskExecutor
}

impl r2d2::ManageConnection for ConnectionManager {
    type Connection = Connection;
    type Error = pulsar::ConnectionError;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        Future::wait(Connection::new(self.addr.clone(), None, None, self.executor.clone()))
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        Future::wait(conn.sender().lookup_topic(String::from("test"), false))
            .map(|_| ())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        !conn.is_valid()
    }
}

pub struct ProducerConnectionManager {
    addr: String,
    producer_name: String,
    executor: tokio::runtime::TaskExecutor,
    connection_index: AtomicUsize,
}

impl ProducerConnectionManager {
    pub fn new<S1: Into<String>, S2: Into<String>>(addr: S1, producer_name: S2, executor: tokio::runtime::TaskExecutor) -> ProducerConnectionManager {
        ProducerConnectionManager {
            addr: addr.into(),
            producer_name: producer_name.into(),
            executor,
            connection_index: AtomicUsize::new(0)
        }
    }
}

impl r2d2::ManageConnection for ProducerConnectionManager {
    type Connection = Producer;
    type Error = pulsar::ProducerError;

    fn connect(&self) -> Result<Producer, Self::Error> {
        let name = format!("{}_{}", &self.producer_name, self.connection_index.fetch_add(1, Ordering::Relaxed));
        Producer::new(self.addr.as_str(), name, None, None, self.executor.clone())
            .wait()
    }

    fn is_valid(&self, conn: &mut Producer) -> Result<(), Self::Error> {
        conn.check_connection().wait().map_err(|e| pulsar::ProducerError::Connection(e))
    }

    fn has_broken(&self, conn: &mut Producer) -> bool {
        !conn.is_valid()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use r2d2;
    use futures::Stream;
    use pulsar::{Consumer, ConsumerBuilder, SubType, ConsumerError, ConnectionError};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestData {
        pub data: String
    }

    #[test]
    #[ignore]
    fn it_works() {
        let addr = "127.0.0.1:6650";
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let pool = r2d2::Pool::new(ProducerConnectionManager::new(
            addr,
            "r2d2_test_producer",
            runtime.executor()
        )).unwrap();

        let mut a = pool.get().unwrap();
        let mut b = pool.get().unwrap();

        let dataz = TestData { data: String::from("dataz") };

        let send_1 = a.send_json("r2d2_test", &dataz.clone());
        let send_2 = b.send_json("r2d2_test", &dataz.clone());

        send_1.join(send_2).wait().unwrap();

        let consumer: Consumer<TestData> = ConsumerBuilder::new(addr, runtime.executor())
            .with_subscription("r2d2_test")
            .with_topic("r2d2_test")
            .with_subscription_type(SubType::Exclusive)
            .build()
            .wait()
            .unwrap();

        let mut consumed_messages = 0;
        let consumed = consumer.for_each(move |(msg, ack)| {
            consumed_messages += 1;
            ack.ack();
            if let Err(e) = msg {
                println!("Consumer error: {}", e);
            }
            if consumed_messages == 2 {
                //err here to shutdown
                Err(ConsumerError::Connection(ConnectionError::Unexpected("Done!".to_owned())))
            } else {
                Ok(())
            }
        }).wait();

        match &consumed {
            Err(ConsumerError::Connection(ConnectionError::Unexpected(msg))) if msg.as_str() == "Done!" => {},
            other => panic!("Unexpected consumer shutdown. Found: {:?}", other)
        }

        runtime.shutdown_now();
    }
}
