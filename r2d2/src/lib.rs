extern crate failure;
extern crate futures;
extern crate pulsar;
extern crate r2d2;
extern crate tokio;

#[cfg(test)]
extern crate serde;

#[cfg(test)]
#[macro_use]
extern crate serde_derive;

use failure::Fail;
use futures::Future;
use pulsar::{Connection, Producer, Error};
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct ConnectionManager {
    addr: String,
    executor: tokio::runtime::TaskExecutor
}

impl r2d2::ManageConnection for ConnectionManager {
    type Connection = Connection;
    type Error = failure::Compat<Error>;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let connection = Future::wait(Connection::new(self.addr.clone(), self.executor.clone()))
            .map_err(|e| e.compat())?;
        Ok(connection)
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        Future::wait(conn.sender().lookup_topic(String::from("test")))
            .map(|_| ())
            .map_err(|e| e.compat())
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
    type Error = failure::Compat<Error>;

    fn connect(&self) -> Result<Producer, Self::Error> {
        let name = format!("{}_{}", &self.producer_name, self.connection_index.fetch_add(1, Ordering::Relaxed));
        Producer::new(self.addr.as_str(), name, self.executor.clone())
            .wait()
            .map_err(|e| e.compat())
    }

    fn is_valid(&self, conn: &mut Producer) -> Result<(), Self::Error> {
        conn.check_connection().wait().map_err(|e| e.compat())
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
    use pulsar::{Consumer, ConsumerBuilder, SubType};

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
        let consumed = consumer.for_each(move |msg| {
            consumed_messages += 1;
            let (data, ack) = msg.unwrap();
            let _ = ack.ack();
            assert_eq!(data.data.as_str(), "dataz");
            if consumed_messages == 2 {
                //err here to shutdown
                Err(Error::Unexpected("Done!".to_string()))
            } else {
                Ok(())
            }
        }).wait();

        match &consumed {
            Err(Error::Unexpected(msg)) if msg.as_str() == "Done!" => {},
            other => panic!("Unexpected consumer shutdown. Found: {:?}", other)
        }

        runtime.shutdown_now();
    }
}
