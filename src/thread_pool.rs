use Configuration;
use deque;
use deque::{Worker, Stealer, Stolen};
use job::{JobRef, JobMode, StackJob};
use latch::{Latch, LockLatch};
#[allow(unused_imports)]
use log::Event::*;
use rand::{self, Rng};
use std::cell::Cell;
use std::sync::{Arc, Condvar, Mutex, Once, ONCE_INIT};
use std::process;
use std::thread;
use std::collections::VecDeque;
use std::mem;
use unwind;
use util::leak;
use num_cpus;

/// ////////////////////////////////////////////////////////////////////////

pub struct Registry {
    thread_infos: Vec<ThreadInfo>,
    state: Mutex<RegistryState>,
    work_available: Condvar,
}

struct RegistryState {
    terminate: bool,
    threads_at_work: usize,
    injected_jobs: VecDeque<JobRef>,
}

/// ////////////////////////////////////////////////////////////////////////
/// Initialization

static mut THE_REGISTRY: Option<&'static Registry> = None;
static THE_REGISTRY_SET: Once = ONCE_INIT;

/// Starts the worker threads (if that has not already happened). If
/// initialization has not already occurred, use the default
/// configuration.
pub fn get_registry() -> &'static Registry {
    THE_REGISTRY_SET.call_once(|| unsafe { init_registry(Configuration::new()) });
    unsafe { THE_REGISTRY.unwrap() }
}

/// Starts the worker threads (if that has not already happened) with
/// the given configuration.
pub fn get_registry_with_config(config: Configuration) -> &'static Registry {
    THE_REGISTRY_SET.call_once(|| unsafe { init_registry(config) });
    unsafe { THE_REGISTRY.unwrap() }
}

/// Initializes the global registry with the given configuration.
/// Meant to be called from within the `THE_REGISTRY_SET` once
/// function. Declared `unsafe` because it writes to `THE_REGISTRY` in
/// an unsynchronized fashion.
unsafe fn init_registry(config: Configuration) {
    let registry = leak(Registry::new(config.num_threads()));
    THE_REGISTRY = Some(registry);
}

enum Work {
    None,
    Job(JobRef),
    Terminate,
}

impl Registry {
    pub fn new(num_threads: Option<usize>) -> Arc<Registry> {
        let limit_value = match num_threads {
            Some(value) => value,
            None => num_cpus::get(),
        };

        let (workers, stealers): (Vec<_>, Vec<_>) = (0..limit_value).map(|_| deque::new()).unzip();

        let registry = Arc::new(Registry {
            thread_infos: stealers.into_iter()
                .map(|s| ThreadInfo::new(s))
                .collect(),
            state: Mutex::new(RegistryState::new()),
            work_available: Condvar::new(),
        });

        for (index, worker) in workers.into_iter().enumerate() {
            let registry = registry.clone();
            thread::spawn(move || unsafe { main_loop(worker, registry, index) });
        }

        registry
    }

    pub fn num_threads(&self) -> usize {
        self.thread_infos.len()
    }

    /// Waits for the worker threads to get up and running.  This is
    /// meant to be used for benchmarking purposes, primarily, so that
    /// you can get more consistent numbers by having everything
    /// "ready to go".
    pub fn wait_until_primed(&self) {
        for info in &self.thread_infos {
            info.primed.wait();
        }
    }

    /// ////////////////////////////////////////////////////////////////////////
    /// MAIN LOOP
    ///
    /// So long as all of the worker threads are hanging out in their
    /// top-level loop, there is no work to be done.

    fn start_working(&self, index: usize) {
        log!(StartWorking { index: index });
        {
            let mut state = self.state.lock().unwrap();
            state.threads_at_work += 1;
        }
        self.work_available.notify_all();
    }

    pub unsafe fn inject(&self, injected_jobs: &[JobRef]) {
        log!(InjectJobs { count: injected_jobs.len() });
        {
            let mut state = self.state.lock().unwrap();

            // It should not be possible for `state.terminate` to be true
            // here. It is only set to true when the user creates (and
            // drops) a `ThreadPool`; and, in that case, they cannot be
            // calling `inject()` later, since they dropped their
            // `ThreadPool`.
            assert!(!state.terminate, "inject() sees state.terminate as true");

            state.injected_jobs.extend(injected_jobs);
        }
        self.work_available.notify_all();
    }

    fn wait_for_work(&self, _worker: usize, was_active: bool) -> Work {
        log!(WaitForWork {
            worker: _worker,
            was_active: was_active,
        });

        let mut state = self.state.lock().unwrap();

        if was_active {
            state.threads_at_work -= 1;
        }

        loop {
            // Check if we need to terminate.
            if state.terminate {
                return Work::Terminate;
            }

            // Otherwise, if anything was injected from outside,
            // return that.  Note that this gives preference to
            // injected items over stealing from others, which is a
            // bit dubious, but then so is the opposite.
            if let Some(job) = state.injected_jobs.pop_front() {
                state.threads_at_work += 1;
                self.work_available.notify_all();
                return Work::Job(job);
            }

            // If any of the threads are running a job, we should spin
            // up, since they may generate subworkitems.
            if state.threads_at_work > 0 {
                return Work::None;
            }

            state = self.work_available.wait(state).unwrap();
        }
    }

    pub fn terminate(&self) {
        {
            let mut state = self.state.lock().unwrap();
            state.terminate = true;
            for job in state.injected_jobs.drain(..) {
                unsafe {
                    job.execute(JobMode::Abort);
                }
            }
        }
        self.work_available.notify_all();
    }
}

impl RegistryState {
    pub fn new() -> RegistryState {
        RegistryState {
            threads_at_work: 0,
            injected_jobs: VecDeque::new(),
            terminate: false,
        }
    }
}

struct ThreadInfo {
    // latch is set once thread has started and we are entering into
    // the main loop
    primed: LockLatch,
    stealer: Stealer<JobRef>,
}

impl ThreadInfo {
    fn new(stealer: Stealer<JobRef>) -> ThreadInfo {
        ThreadInfo {
            primed: LockLatch::new(),
            stealer: stealer,
        }
    }
}

/// ////////////////////////////////////////////////////////////////////////
/// WorkerThread identifiers

pub struct WorkerThread {
    worker: Worker<JobRef>,
    stealers: Vec<Stealer<JobRef>>,
    index: usize,

    /// A counter tracking the "logical stack depth" for our local
    /// deque -- basically, what it *would* be, if other people
    /// weren't stealing from the top. Every time we push onto the
    /// deque, we increment this counter, and everytime we pop from
    /// the deque, we decrement it.
    ///
    /// Basically the whole purpose of this counter is so that
    /// `join()` can call `worker.pop()` for its RHS and know with
    /// confidence that the thing it gets back is either `None` (if
    /// RHS was stolen) or the RHS task. This lets join avoid using a
    /// latch/virtual-call in the case where `Some` is returned.
    ///
    /// The actual logic here is a bit subtle. It relies on the key
    /// assumption that nobody has a reason to block unless someone
    /// else has stolen a task from their local deque. Put another
    /// way, if I push N tasks, then I would never try to pop more
    /// than N tasks. This is because, if I pop all N of my tasks,
    /// then my work is done and I am ready to return. If fail to pop
    /// them all, then this is because another thread stole them, in
    /// which case I have no reason to pop, since the deque is empty.
    ///
    /// Therefore, we can keep a *logical stack counter* at all
    /// points, and we know that whenever we execute a job, it can
    /// only increase this counter or leave it the same. A job J
    /// should never pop things that J (or one of its callees) did not
    /// itself push.
    ///
    /// This allows `join()` to record the stack depth as D; push the
    /// RHS (increasing stack depth to D+1) and execute the LHS. After
    /// executing the LHS, the new logical stack is D' > D. So if we
    /// are able to pop D' - D things, the last thing popped must be
    /// the RHS (note, though, that it may have been stolen, so we may
    /// not able to pop all those things).
    ///
    /// One operation that this invariant disallows (or least
    /// makes...inconvenient) is a blocking join of random tasks in
    /// the Rayon queue (i.e., ones that your current task did not
    /// transitively spawn). If we permitted you to join some random
    /// task, it may not have been spawned by your local deque. As a
    /// result, while you are waiting, you would be inclined to pop
    /// items from your local deque, and maybe steal from others.  But
    /// in that case you might easily pop things that you did not
    /// push, violating the invariant. So we could permit this
    /// operation, but rather than *popping* from the deque you would
    /// steal from it.  The whole thing is just inefficient, though,
    /// so we don't offer that API, and instead encourage the use of
    /// futures.
    ///
    /// A concrete example of how things could go wrong:
    ///
    /// - `join` comes in with stack depth 0
    /// - `join` pushes RHS to stack depth 1
    /// - LHS calls `wait` on some random job; while waiting:
    ///     - we pop RHS, dropping stack depth to 0
    ///     - steal another task, execute it; this task pushes 1 task `X`
    ///     - up, signal is set, break
    /// - `join` reads stack depth as 1, pops task `X`, assumes it is equal to RHS
    spawn_count: Cell<usize>,

    /// A weak random number generator.
    rng: rand::XorShiftRng,
}

// This is a bit sketchy, but basically: the WorkerThread is
// allocated on the stack of the worker on entry and stored into this
// thread local variable. So it will remain valid at least until the
// worker is fully unwound. Using an unsafe pointer avoids the need
// for a RefCell<T> etc.
thread_local! {
    static WORKER_THREAD_STATE: Cell<*mut WorkerThread> =
        Cell::new(0 as *mut WorkerThread)
}

impl WorkerThread {
    /// Gets the `WorkerThread` index for the current thread; returns
    /// NULL if this is not a worker thread. This pointer is valid
    /// anywhere on the current thread.
    #[inline]
    pub unsafe fn current() -> *mut WorkerThread {
        WORKER_THREAD_STATE.with(|t| t.get())
    }

    /// Sets `self` as the worker thread index for the current thread.
    /// This is done during worker thread startup.
    unsafe fn set_current(&mut self) {
        WORKER_THREAD_STATE.with(|t| {
            assert!(t.get().is_null());
            t.set(self);
        });
    }

    #[inline]
    pub fn index(&self) -> usize {
        self.index
    }

    /// Read current value of the spawn counter.
    ///
    /// See the `spawn_count` field for an extensive comment on the
    /// meaning of the spawn counter.
    #[inline]
    pub fn current_spawn_count(&self) -> usize {
        self.spawn_count.get()
    }

    /// Pops spawned (async) jobs until our spawn count reaches
    /// `start_count` or the deque is empty. This routine is used to
    /// ensure that the local deque is "balanced".
    ///
    /// See the `spawn_count` field for an extensive comment on the
    /// meaning of the spawn counter and use of this function.
    #[inline]
    pub unsafe fn pop_spawned_jobs(&self, start_count: usize) {
        debug_assert!(self.spawn_count.get() >= start_count);
        while self.spawn_count.get() > start_count {
            if let Some(job_ref) = self.pop() {
                job_ref.execute(JobMode::Execute);
            } else {
                self.spawn_count.set(start_count);
                break;
            }
        }
        debug_assert!(self.spawn_count.get() == start_count);
    }

    #[inline]
    pub unsafe fn push(&self, job: JobRef) {
        self.spawn_count.set(self.spawn_count.get() + 1);
        self.worker.push(job);
    }

    /// Pop `job` from top of stack, returning `false` if it has been
    /// stolen.
    #[inline]
    pub unsafe fn pop(&self) -> Option<JobRef> {
        let spawn_count = self.spawn_count.get();
        if spawn_count > 0 {
            self.spawn_count.set(spawn_count - 1);
            if let Some(result) = self.worker.pop() {
                Some(result)
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Keep stealing jobs until the latch is set.
    #[cold]
    pub unsafe fn steal_until<L: Latch>(&mut self, latch: &L) {
        // we only ever try to steal if we've exhausted our local work
        debug_assert!(self.worker.pop().is_none());

        // load initial logical depth of our local deque; this will be
        // used later to check if stolen jobs pushed work we might
        // want to do
        let spawn_count = self.spawn_count.get();

        // the code below should swallow all panics and hence never
        // unwind; but if something does wrong, we want to abort,
        // because otherwise other code in rayon may assume that the
        // latch has been signaled, and hence that permit random
        // memory accesses, which would be *very bad*
        let guard = unwind::finally((), |_| process::exit(2222));

        while !latch.probe() {
            // check if something we stole pushed new local jobs
            if self.spawn_count.get() > spawn_count {
                if let Some(job) = self.pop() {
                    job.execute(JobMode::Execute);
                    continue;
                }
            }

            // if not, try to steal some more
            if !self.steal_and_execute() {
                thread::yield_now();
            }
        }

        mem::forget(guard); // successful execution, do not abort
    }

    /// Try to steal a single job. If successful, execute it and
    /// return true. Else return false.
    unsafe fn steal_and_execute(&mut self) -> bool {
        if let Some(job) = self.steal() {
            job.execute(JobMode::Execute);
            true
        } else {
            false
        }
    }

    /// Try to pop a job locally; if none is found, try to steal a job.
    ///
    /// This is only used in the main worker loop: code elsewhere
    /// never pops indiscriminantly, but always with some notion of
    /// the current stack depth.
    unsafe fn pop_or_steal(&mut self) -> Option<JobRef> {
        self.pop().or_else(|| self.steal())
    }

    /// Try to steal a single job and return it.
    ///
    /// This should only be done as a last resort, when there is no
    /// local work to do.
    unsafe fn steal(&mut self) -> Option<JobRef> {
        // we only steal when we don't have any work to do locally
        debug_assert!(self.worker.pop().is_none());

        // otherwise, try to steal
        if self.stealers.is_empty() {
            return None;
        }
        let start = self.rng.next_u32() % self.stealers.len() as u32;
        let (lo, hi) = self.stealers.split_at(start as usize);
        hi.iter()
            .chain(lo)
            .filter_map(|stealer| {
                match stealer.steal() {
                    Stolen::Empty => None,
                    Stolen::Abort => None, // loop?
                    Stolen::Data(v) => Some(v),
                }
            })
            .next()
    }
}

/// ////////////////////////////////////////////////////////////////////////

unsafe fn main_loop(worker: Worker<JobRef>, registry: Arc<Registry>, index: usize) {
    let stealers = registry.thread_infos
        .iter()
        .enumerate()
        .filter(|&(i, _)| i != index)
        .map(|(_, ti)| ti.stealer.clone())
        .collect::<Vec<_>>();

    assert!(stealers.len() < ::std::u32::MAX as usize,
            "We assume this is not going to happen!");

    let mut worker_thread = WorkerThread {
        worker: worker,
        stealers: stealers,
        index: index,
        spawn_count: Cell::new(0),
        rng: rand::weak_rng(),
    };
    worker_thread.set_current();

    // let registry know we are ready to do work
    registry.thread_infos[index].primed.set();

    // Worker threads should not panic. If they do, just abort, as the
    // internal state of the threadpool is corrupted. Note that if
    // **user code** panics, we should catch that and redirect.
    let abort_guard = unwind::AbortIfPanic;

    let mut was_active = false;
    loop {
        match registry.wait_for_work(index, was_active) {
            Work::Job(injected_job) => {
                injected_job.execute(JobMode::Execute);
                was_active = true;
                continue;
            }
            Work::Terminate => break,
            Work::None => {}
        }

        was_active = false;
        while let Some(job) = worker_thread.pop_or_steal() {
            // How do we want to prioritize injected jobs? this gives
            // them very low priority, which seems good. Finish what
            // we are doing before taking on new things.
            log!(StoleWork { worker: index });
            registry.start_working(index);
            job.execute(JobMode::Execute);
            was_active = true;
        }
    }

    // Normal termination, do not abort.
    mem::forget(abort_guard);
}

pub fn in_worker<OP>(op: OP)
    where OP: FnOnce(&WorkerThread) + Send
{
    unsafe {
        let owner_thread = WorkerThread::current();
        if !owner_thread.is_null() {
            // Perfectly valid to give them a `&T`: this is the
            // current thread, so we know the data structure won't be
            // invalidated until we return.
            op(&*owner_thread);
        } else {
            in_worker_cold(op);
        }
    }
}

#[cold]
unsafe fn in_worker_cold<OP>(op: OP)
    where OP: FnOnce(&WorkerThread) + Send
{
    // never run from a worker thread; just shifts over into worker threads
    debug_assert!(WorkerThread::current().is_null());
    let job = StackJob::new(|| in_worker(op), LockLatch::new());
    get_registry().inject(&[job.as_job_ref()]);
    job.latch.wait();
}
