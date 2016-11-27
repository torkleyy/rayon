use latch::{Latch, LockLatch, SpinLatch};
#[allow(unused_imports)]
use log::Event::*;
use job::StackJob;
use thread_pool::{self, WorkerThread};
use std::mem;
use unwind;

pub fn join<A, B, RA, RB>(oper_a: A, oper_b: B) -> (RA, RB)
    where A: FnOnce() -> RA + Send,
          B: FnOnce() -> RB + Send,
          RA: Send,
          RB: Send
{
    unsafe {
        let worker_thread = WorkerThread::current();

        // slow path: not yet in the thread pool
        if worker_thread.is_null() {
            return join_inject(oper_a, oper_b);
        }

        let worker_thread = &*worker_thread;

        log!(Join { worker: worker_thread.index() });

        // create virtual wrapper for task b; this all has to be
        // done here so that the stack frame can keep it all live
        // long enough
        let job_b = StackJob::new(oper_b, SpinLatch::new());
        let job_b_ref = job_b.as_job_ref();
        worker_thread.push(job_b_ref);

        // execute task a; hopefully b gets stolen
        let result_a;
        {
            // If job A panics, we have to make sure that job B is
            // finished executing before we actually unwind, because
            // it may be using bits of data on the stack frame. The
            // best way to do this is to pop it before it gets
            // stolen. Failing that, we will just have to spin on the
            // latch. We can't really risk executing work, because if
            // that work should panic, that would be a double panic,
            // which would kill the process.
            let guard = unwind::finally(&job_b.latch, |job_b_latch| {
                // might have been popped by job A, so check that first
                if job_b_latch.probe() {
                    return;
                }

                // if not, should still be on the deque, unless it was
                // stolen; pop until we find it
                while let Some(job) = worker_thread.pop() {
                    if job == job_b_ref { // found it!
                        return;
                    }
                }

                // d'oh, must have been stolen
                job_b_latch.spin();
            });

            // Execute job a.
            result_a = oper_a();

            // If successful, no need for panic recovery.
            mem::forget(guard);
        }

        // Try to pop job B from the local stack. It may already have
        // been popped by job A; it may also have been stolen. There
        // may also be some tasks pushed on top of it in the stack,
        // and we will have to pop those off to get to it.
        while !job_b.latch.probe() {
            if let Some(job) = worker_thread.pop() {
                if job == job_b_ref {
                    // Found it! Let's run it.
                    //
                    // Note that this could panic, but it's ok if we unwind here.
                    log!(PoppedRhs { worker: worker_thread.index() });
                    let result_b = job_b.run_inline();
                    return (result_a, result_b);
                } else {
                    log!(PoppedJob { worker: worker_thread.index() });
                    worker_thread.execute(job);
                }
            } else {
                // Local deque is empty. Time to steal from other
                // threads.
                log!(LostJob { worker: worker_thread.index() });
                worker_thread.wait_until(&job_b.latch);
                debug_assert!(job_b.latch.probe());
                break;
            }
        }

        return (result_a, job_b.into_result());
    }
}

#[cold] // cold path
unsafe fn join_inject<A, B, RA, RB>(oper_a: A, oper_b: B) -> (RA, RB)
    where A: FnOnce() -> RA + Send,
          B: FnOnce() -> RB + Send,
          RA: Send,
          RB: Send
{
    let job_a = StackJob::new(oper_a, LockLatch::new());
    let job_b = StackJob::new(oper_b, LockLatch::new());

    thread_pool::global_registry().inject(&[job_a.as_job_ref(), job_b.as_job_ref()]);

    job_a.latch.wait();
    job_b.latch.wait();

    (job_a.into_result(), job_b.into_result())
}
