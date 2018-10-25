extern crate failure;
extern crate futures;
extern crate pulsar_client;
extern crate r2d2;
extern crate tokio;

use failure::Fail;
use futures::Future;
use pulsar_client::{Connection, Error};
use std::thread;

pub struct ConnectionManager {
    addr: String,
}

impl r2d2::ManageConnection for ConnectionManager {
    type Connection = Connection;
    type Error = failure::Compat<Error>;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let (producer, handlers) = Future::wait(Connection::new(self.addr.clone()))
            .map_err(|e| e.compat())?;
        thread::spawn(|| tokio::run(handlers));
        Ok(producer)
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        Future::wait(conn.lookup_topic(String::from("test")))
            .map(|_| ())
            .map_err(|e| e.compat())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        !conn.is_valid()
    }
}


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
