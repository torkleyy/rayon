use latch::{Latch, LockLatch, SpinLatch};
#[allow(unused_imports)]
use log::Event::*;
use job::StackJob;
use thread_pool::{self, ShadowStackFrame, WorkerThread};
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

        let shadow_frame = ShadowStackFrame::new(worker_thread.shadow_stack_head());
        worker_thread.push_shadow_frame(&shadow_frame);

        // create virtual wrapper for task b; this all has to be
        // done here so that the stack frame can keep it all live
        // long enough
        let job_b = StackJob::new(oper_b, SpinLatch::new());
        worker_thread.push(job_b.as_job_ref());

        // execute task a; hopefully b gets stolen
        let result_a;
        {
            let guard = unwind::finally(&job_b.latch, |job_b_latch| {
                // If job A panics, we have to make sure that job B is
                // finished executing before we actually unwind,
                // because it may be using bits of data on the stack
                // frame. The best way to do this is to pop it before
                // it gets stolen. Failing that, we will just have to
                // spin on the latch. We can't really risk executing
                // work, because if that work should panic, that would
                // be a double panic, which would kill the process.
                if worker_thread.is_shadow_stack_head(&shadow_frame) {
                    let mut failed_to_pop = false;
                    while worker_thread.is_shadow_stack_head(&shadow_frame) {
                        if worker_thread.pop().is_none() {
                            failed_to_pop = true;
                            break;
                        }
                    }

                    // If we observed this chain of events:
                    //
                    // - our shadow stack was head
                    // - we had a successful pop
                    // - our stack stack is *not* head
                    //
                    // then we know that we have popped job b, so we
                    // can just return without spinning.
                    if !failed_to_pop {
                        // job b never executed, so its latch should not have been set
                        debug_assert!(!job_b_latch.probe());
                        return;
                    }
                }

                // otherwise, job b was stolen, we have to spin
                job_b_latch.spin();
            });

            // execute job a
            result_a = oper_a();

            // if successful, no need for panic recovery
            mem::forget(guard);
        }

        // before we can try to pop b, we have to first pop off any async spawns
        // that have occurred on this thread
        //
        // NB-- it's not obvious that this is the right strategy. This
        // corresponds to doing the LHS pushing some jobs with
        // `scope.spawn()` (for a scope that encloses the current call
        // to `join()`). This call here forces us to execute those
        // jobs before trying to pop the RHS. This is consistent with
        // the "depth-first" strategy, but it's not obvious where to
        // place spawned jobs in that tree -- in some sense they may
        // better belong with their scope, in which case it seems
        // better to try to execute RHS before those jobs. We could
        // implement this in various ways.
        if worker_thread.pop_and_execute_all_but_one(&shadow_frame) {
            // if `pop_all_but_one()` returns true, then the next
            // thing popped from our local deque will be `job_b`; if
            // it returns false, then `job_b` was stolen (possibly
            // even by this thread)
            debug_assert!(worker_thread.is_shadow_stack_head(&shadow_frame));
            if let Some(j) = worker_thread.pop() {
                debug_assert!(!worker_thread.is_shadow_stack_head(&shadow_frame));
                debug_assert_eq!(job_b.as_job_ref(), j);
                log!(PoppedJob { worker: worker_thread.index() });
                let result_b = job_b.run_inline(); // not stolen, let's do it!
                return (result_a, result_b);
            }
        }

        // b was stolen, wait for the thief to be finish (and be helpful in the meantime)
        log!(LostJob { worker: worker_thread.index() });
        worker_thread.wait_until(&job_b.latch);
        let result_b = job_b.into_result();
        (result_a, result_b)
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
