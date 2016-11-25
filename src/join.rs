use latch::{Latch, LockLatch, SpinLatch};
#[allow(unused_imports)]
use log::Event::*;
use job::{JobMode, JobRef, StackJob};
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

        // Execute job B, if it wasn't already completed.
        //
        // (Note that job B may have been popped by a blocked task in
        // job A; in that case, the latch will be set, so check that
        // first before we go off popping things.)
        if !job_b.latch.probe() {
            // try to pop `b`; there may have been other jobs pushed on our deque
            // by job a, so we have to get through those first.
            if let Some(job) = worker_thread.pop() {
                if job == job_b_ref {
                    log!(PoppedJob { worker: worker_thread.index() });
                    let result_b = job_b.run_inline(); // not stolen, let's do it!
                    return (result_a, result_b);
                }

                join_cold(&job_b.latch, Some(job));
            } else {
                join_cold(&job_b.latch, None);
            }
        }

        return (result_a, job_b.into_result());
    }
}

/// Recover from a failure to pop "job B" from a join.
/// This could be because it was stolen by another thread,
/// popped during execution of the LHS, or because the LHS
/// pushed other jobs.
///
/// Recovery works by popping jobs from the local deque and/or
/// stealing from other deques until the latch is set. NB: in the case
/// where there are still jobs on the local deque, it's not obvious
/// that executing those jobs is the best strategy. These jobs must
/// have been either `spawn_async()` calls or else spawned into an
/// enclosing scope, and one could imagine it's better to process them
/// then. But the deque doesn't easily permit that, and of course it
/// *may* be better to stick with the DFS strategy.
///
/// The argument `popping` is a job that was popped but not yet
/// executed. If `Some(_)`, it is always executed first.
///
/// Unsafe: must be called on a worker thread.
#[cold]
unsafe fn join_cold(job_b_latch: &SpinLatch,
                    popped: Option<JobRef>) {
    let worker_thread = WorkerThread::current();
    debug_assert!(!worker_thread.is_null());
    let worker_thread = &*worker_thread;

    if let Some(job) = popped {
        job.execute(JobMode::Execute);
    }

    log!(LostJob { worker: worker_thread.index() });
    worker_thread.wait_until(job_b_latch);
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
