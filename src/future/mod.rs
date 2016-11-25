use spawn_async::spawn_async;
use futures::{Future, Poll, Async};
use futures::sync::oneshot::{channel, Sender, Receiver};
use futures::task::{self, Run, Executor};
use std::panic::{self, AssertUnwindSafe};
use std::thread;
use thread_pool::Registry;

pub struct RayonFuture<T, E> {
    inner: Receiver<thread::Result<Result<T, E>>>,
}

struct RayonSender<F, T> {
    future: F,
    tx: Option<Sender<T>>,
}

pub fn spawn_future_async<F>(f: F) -> RayonFuture<F::Item, F::Error>
    where F: Future + Send + 'static,
          F::Item: Send + 'static,
          F::Error: Send + 'static,
{
    let (tx, rx) = channel();
    // AssertUnwindSafe is fine here because we will propagate the
    // panic later.
    let sender = RayonSender {
        future: AssertUnwindSafe(f).catch_unwind(),
        tx: Some(tx),
    };
    let registry = Registry::current();
    task::spawn(sender).execute(registry);
    RayonFuture { inner: rx }
}

impl Executor for Registry {
    fn execute(&self, r: Run) {
        spawn_async(|| r.run());
    }
}

impl<T: Send, E: Send> Future for RayonFuture<T, E> {
    type Item = T;
    type Error = E;

    fn poll(&mut self) -> Poll<T, E> {
        match self.inner.poll().expect("shouldn't be canceled") {
            Async::Ready(Ok(Ok(e))) => Ok(e.into()),
            Async::Ready(Ok(Err(e))) => Err(e),
            Async::Ready(Err(e)) => panic::resume_unwind(e),
            Async::NotReady => Ok(Async::NotReady),
        }
    }
}

impl<F: Future> Future for RayonSender<F, Result<F::Item, F::Error>> {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        if let Ok(Async::Ready(_)) = self.tx.as_mut().unwrap().poll_cancel() {
            // Cancelled, bail out
            return Ok(().into())
        }

        let res = match self.future.poll() {
            Ok(Async::Ready(e)) => Ok(e),
            Ok(Async::NotReady) => return Ok(Async::NotReady),
            Err(e) => Err(e),
        };
        self.tx.take().unwrap().complete(res);
        Ok(Async::Ready(()))
    }
}

#[cfg(test)]
mod test;
