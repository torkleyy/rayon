use spawn_async::spawn_async;
use job::{HeapJob, StackJob, JobMode};
use futures::{IntoFuture, Future, Poll, Async};
use futures::future::lazy;
use futures::sync::oneshot::{channel, Sender, Receiver};
use futures::task::{self, Run, Executor};
use std::panic;
use std::thread;
use thread_pool::{self, Registry, WorkerThread};
use unwind;

pub struct RayonFuture<T, E> {
    inner: Receiver<thread::Result<Result<T, E>>>,
}

impl<T: Send + 'static, E: Send + 'static> Future for RayonFuture<T, E> {
    type Item = T;
    type Error = E;

    fn poll(&mut self) -> Poll<T, E> {
        match self.inner.poll().expect("shouldn't be canceled") {
            Async::Ready(Ok(Ok(e))) => Ok(e.into()),
            Async::Ready(Ok(Err(e))) => Err(e),
            Async::Ready(Err(e)) => unwind::resume_unwinding(e),
            Async::NotReady => Ok(Async::NotReady),
        }
    }
}

struct RayonSender<F, T> {
    future: F,
    tx: Option<Sender<T>>,
}

pub fn spawn_async_future<F>(f: F) -> RayonFuture<F::Item, F::Error>
    where F: Future + Send + 'static,
          F::Item: Send + 'static,
          F::Error: Send + 'static,
{
    let (tx, rx) = channel();
    // AssertUnwindSafe is used here becuase `Send + 'static` is basically
    // an alias for an implementation of the `UnwindSafe` trait but we can't
    // express that in the standard library right now.
    let sender = RayonSender {
        future: f,
        tx: Some(tx),
    };
    executor::spawn(sender).execute(self.inner.clone());
    CpuFuture { inner: rx }
}

impl Executor for Registry {
    fn execute(&self, r: Run) {
        spawn_async(|| r.run());
    }
}

/// Fires off a task into the Rayon threadpool that will run
/// asynchronously. This this task runs asynchronously, it cannot hold
/// any references to the enclosing stack frame. Like a regular Rust
/// thread, it should always be created with a `move` closure and
/// should not hold any references (except for `&'static` references).
///
/// NB: If this closure should panic, the resulting error is just
/// dropped onto the floor and is not propagated.
///
/// # Examples
///
/// This code creates a Rayon that task. The task fires off a message
/// on the channel (`22`) when it executes. The spawning task then
/// waits for the message. This is a handy pattern if you want to
/// delegate work into the Rayon threadpool.
///
/// **Warning: Do not write blocking code like this on the Rayon
/// threadpool!** This is only useful if you know that you are not
/// currently in the Rayon threadpool; otherwise, it will be
/// inefficient at best and could deadlock at worst.
///
/// ```rust
/// use std::sync::mpsc::channel;
///
/// // Create a channel
/// let (tx, rx) = channel();
///
/// // Start an async rayon thread, giving it the
/// // transmit endpoint.
/// rayon::spawn_async(move || {
///     tx.send(22).unwrap();
/// });
///
/// // Block until the Rayon thread sends us some data.
/// // Note that if the job should panic (or otherwise terminate)
/// // before sending, this `unwrap()` will fail.
/// let v = rx.recv().unwrap();
/// assert!(v == 22);
/// ```
#[cfg(feature = "unstable")]
pub fn spawn_async<A>(func: A)
    where A: FnOnce() + Send + 'static
{
    let job = Box::new(HeapJob::new(move |mode| {
        // FIXME: Feels like the unwinding code could be factored into
        // HeapJob somehow.
        match mode {
            JobMode::Execute => match unwind::halt_unwinding(|| func()) {
                Ok(()) => { }
                Err(_) => {
                    /* drop this panic on the floor; nobody cares about your problems */
                }
            },
            JobMode::Abort => { }
        }
    }));

    unsafe {
        // We assert that this does not hold any references (we know
        // this because of the `'static` bound in the inferface);
        // moreover, we assert that the code below is not supposed to
        // be able to panic, and hence the data won't leak but will be
        // enqueued into some deque for later execution.
        let job = job.as_job_ref();
        let worker_thread = WorkerThread::current();
        if worker_thread.is_null() {
            thread_pool::get_registry().inject(&[job]);
        } else {
            (*worker_thread).push(job);
        }
    }
}
