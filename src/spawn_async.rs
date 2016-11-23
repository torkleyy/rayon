#[allow(unused_imports)]
use log::Event::*;
use job::{HeapJob, JobMode};
use thread_pool::{self, WorkerThread};
use unwind;

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

