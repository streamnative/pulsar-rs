use futures::channel::oneshot;
use crate::error::Error;
use futures::{Future, TryFutureExt, FutureExt};
use futures::task::{Context, Poll};
use futures::future::IntoFuture;
use crate::proto::Message;

//pub(crate) trait ResolverT<T> {
//    fn resolve(self, t: T);
//    fn map<F, A>(self, f: F) -> Map<T, A, Self, F>
//        where F: FnOnce(T) -> A + Send + 'static;
//}
//
//pub struct Map<A, B, R: ResolverT<A>, F> {
//    resolver: R,
//    f: F
//}
//
//impl ResolverT for Map {
//
//}

//enum ResolverFutureState {
//    A(oneshot::Receiver<Box<dyn Future<Output=Result<T, Error>>>>),
//    B(Box<dyn Future<Output=Result<T, Error>>>),
//}
//
//pub(crate) struct ResolverFuture<T> {
//    inner: ResolverFutureState
//}
//
//impl<T> ResolverFuture<T> {
//    fn new(rx: oneshot::Receiver<Box<dyn Future<Output=Result<T, E>>>>) -> Self {
//        ResolverFuture {
//            inner: ResolverFutureState::A(rx)
//        }
//    }
//}
//
//impl<T> Future for ResolverFuture<T> {
//    type Output = Result<T, Error>;
//
//    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//        match &mut self.inner {
//            ResolverFutureState::A(rx) => {
//                let ready = ready!(rx.poll(cx));
//                let ready = Box::new(ready
//                    .map_err(|_| Error::disconnected("sender unexpectedly dropped"))
//                    .and_then(|r| r));
//                *self.inner = ResolverFutureState::B(ready);
//                self.poll(cx)
//            },
//            ResolverFutureState::B(f) => {
//                f.poll(cx)
//            }
//        }
//    }
//}

pub(crate) struct Resolver<T>(oneshot::Sender<T>);

impl<T> Resolver<T> {
    pub fn new() -> (Resolver<T>, impl Future<Output = Result<T, Error>>) {
        let (tx, rx) = oneshot::channel();
        (
            Resolver(tx),
            rx.map_err(|_| Error::disconnected("sender unexpectedly dropped"))
                .and_then(|r| r)
        )
    }

    pub fn resolve(self, result: Result<T, Error>) {
        let _ = self.0.send(result);
    }
}

//pub(crate) struct Resolver<T>(Box<dyn FnOnce<Box<dyn Future<Output=Result<T, Error>>>>>);
//impl<T> Resolver<T> {
//    pub fn new() -> (Resolver<T>, impl Future<Output=Result<T, Error>>) {
//        let (tx, rx) = oneshot::channel();
//        let resolver = Resolver(Box::new(move |f| {
//            let _ = tx.send(f);
//        }));
//        let f = ResolverFuture::new(rx);
//        (resolver, f)
//    }
//
//    pub fn map<F: FnOnce(A) -> T, A>(self, f: F) -> Resolver<A> {
//        let resolve = self.0;
//        Resolver(Box::new(move |result: Result<A, Error>| {
//            resolve(Box::new(result.map(f)))
//        }))
//    }
//
//    pub fn and_then<F: FnOnce(A) -> Result<T, Error>, A>(self, f: F) -> Resolver<A> {
//        let resolve = self.0;
//        Resolver(Box::new(move |result: Result<A, Error>| {
//            resolve(Box::new(result.and_then(f)))
//        }))
//    }
//
//    pub fn then<F: FnOnce(Result<A, Error>) -> Result<T, Error>, A>(self, f: F) -> Resolver<A> {
//        let resolve = self.0;
//        Resolver(Box::new(move |result: Result<A, Error>| {
//            resolve(Box::new(f(result)))
//        }))
//    }
//
//    pub fn resolve(self, result: Result<Message, Error>) {
//        self.resolve_fut(result.into_future())
//    }
//
//    pub fn resolve_fut(self, f: impl Future<Output=Result<Message, Error>>) {
//        self.0.call_once(Box::new(f))
//    }
//}


