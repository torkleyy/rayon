use Configuration;
use deque;
use deque::{Worker, Stealer, Stolen};
use job::{JobRef, JobMode, StackJob};
use latch::{Latch, LockLatch};
#[allow(unused_imports)]
use log::Event::*;
use rand::{self, Rng};
use std::cell::{Cell, UnsafeCell};
use std::sync::{Arc, Condvar, Mutex, Once, ONCE_INIT};
use std::process;
use std::thread;
use std::collections::VecDeque;
use std::mem;
use std::ptr;
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

static mut THE_REGISTRY: Option<&'static Arc<Registry>> = None;
static THE_REGISTRY_SET: Once = ONCE_INIT;

/// Starts the worker threads (if that has not already happened). If
/// initialization has not already occurred, use the default
/// configuration.
pub fn global_registry() -> &'static Arc<Registry> {
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
    let registry = leak(Arc::new(Registry::new(config.num_threads())));
    THE_REGISTRY = Some(registry);
}

enum Work {
    None,
    Job(JobRef),
    Terminate,
}

impl Registry {
    /// Gets a handle to the current registry. If we are in a worker, this
    /// is the worker's registry, otherwise its the global registry.
    pub fn current() -> Arc<Registry> {
        unsafe {
            let worker = WorkerThread::current();
            if worker.is_null() {
                global_registry().clone()
            } else {
                (*worker).registry().clone()
            }
        }
    }

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

    /// A weak random number generator.
    rng: UnsafeCell<rand::XorShiftRng>,

    /// The head of a linked list called the "shadow stack" which
    /// tracks how many tasks have been pushed/popped to our local
    /// deque. The purpose of this counter is so that `join()` can
    /// call `worker.pop()` for its RHS and know with confidence that
    /// the thing it gets back is either `None` (if RHS was stolen) or
    /// the RHS task. This lets join avoid using a latch/virtual-call
    /// in the case where `Some` is returned.
    ///
    /// The idea here is roughly like this. For every call to `join`,
    /// we push a new "frame" onto the shadow stack by allocating a
    /// struct on the stack and setting `shadow_stack_head` to point
    /// at it. These frames form a linked list through the `parent`
    /// pointer. This list winds back up the stack and terminates in a
    /// sentinel frame that is pushed when the worker starts. This
    /// sentinel frame will never be popped. (More about it later.)
    ///
    /// Each frame in this stack has a counter (`pushed`) for how many
    /// `push()` calls have occurred while this frame was at the head.
    /// When you call `push()`, we will increment the counter for the
    /// head frame. (Note that all of this data is thread-local, so
    /// for the counter a simple `Cell` suffices.)
    ///
    /// We (generally) maintain the invariant that (a) the head frame
    /// on the stack is never null and (b) that its `pushed` counter
    /// is always non-zero. So when we pop, we decrement the `pushed`
    /// counter and, if it reaches zero, we pop the head
    /// frame. Because the `pushed` counter of the head frame is
    /// always non-zero, we don't need to worry about overflow.
    ///
    /// As we said earlier, to ensure that the head frame is never
    /// null, there is a sentinel frame that is pushed when the worker
    /// starts. It has an initial pushed counter of 1. So this means
    /// that, so long as we don't call `pop()` when the shadow stack
    /// is empty, the shadow stack frame is never going to be popped.
    ///
    /// To ensure that invariant, we are careful to call `pop()` only
    /// when we know shadow stack frame is not empty. This is not
    /// actually hard, since we don't call `pop()` in many places, and
    /// most of them can just test before hand if shadow stack is
    /// empty (it's a cheap operation to check). Moreover, if any of
    /// them call `pop()` and observe a `None` response, they can also
    /// eagerly clear the shadow stack by popping all its frames (see
    /// the section on "popping frames" below).
    ///
    /// # How join works
    ///
    /// So let's walk through how join works. If begins by pushing a
    /// frame onto the shadow stack, let's call that frame F.  F
    /// initially has a pushed count of 0. Then it immediately pushes
    /// the RHS task, bumping that count to 1.
    ///
    /// Now join can execute the LHS. If there are only join
    /// operations in the LHS, then pushes/pops will always be
    /// balanced, and so when the LHS returns, we know that the frame
    /// F will still have a `pushed` count of 1.
    ///
    /// But sometimes there might be unbalanced pushes. This can
    /// happen with calls to `Scope::spawn()` or `spawn_async()`.  In
    /// either of these cases, `join()` will first call a routine
    /// called `pop_all_but_one()`. This basically pops frames until
    /// the `pushed` count reaches 1. At that point, we can call
    /// `pop()` to pop off the RHS. Of course, at any point here we
    /// might find that things have been stolen, in which case we
    /// branch off to the theft logic.
    ///
    /// There is one other subtlety though -- there might also have
    /// been **unbalanced pops**. In other words, the LHS task -- or
    /// something started by the LHS -- might itself have popped the
    /// RHS. This can happen thanks to the `wait()` method on a
    /// `spawn_async()` result or with futures integration. Either of
    /// these will basically pop-and-steal until something is ready.
    /// (Note that both `join` and `scope` never pop things that they
    /// did not push, since they are only blocked until things they
    /// pushed are finished; so, if they block at all, it's because
    /// the local deque is empty thanks to thieves.)
    ///
    /// If there are unbalanced pops, then what will happen is that
    /// the shadow stack frame F will have been popped. This can be
    /// seen simply by comparing the head pointer to see if it is
    /// still F. Naturally, the `pop_all_but_one()` routine does this,
    /// and hence it *also* aborts if the frame F is popped (in which
    /// case the RHS has been stolen, but by the current thread).
    shadow_stack_head: Cell<*const ShadowStackFrame>,

    /// a pointer to the root shadow stack head
    sentinel_frame: *const ShadowStackFrame,

    registry: Arc<Registry>,
}

pub struct ShadowStackFrame {
    pushed: Cell<usize>,
    parent: *const ShadowStackFrame,
}

impl ShadowStackFrame {
    pub fn new(parent: *const ShadowStackFrame) -> Self {
        ShadowStackFrame {
            pushed: Cell::new(0),
            parent: parent
        }
    }
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
    unsafe fn set_current(thread: *mut WorkerThread) {
        WORKER_THREAD_STATE.with(|t| {
            assert!(t.get().is_null());
            t.set(thread);
        });
    }

    /// Returns the registry that owns this worker thread.
    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    /// Our index amongst the worker threads (ranges from `0..self.num_threads()`).
    #[inline]
    pub fn index(&self) -> usize {
        self.index
    }

    #[inline]
    pub unsafe fn push(&self, job: JobRef) {
        debug_assert!(!self.shadow_stack_head.get().is_null());
        let shadow_stack_head = &*self.shadow_stack_head.get();
        shadow_stack_head.pushed.set(shadow_stack_head.pushed.get() + 1);
        self.worker.push(job);
    }

    /// Pop `job` from top of stack, returning `false` if it has been
    /// stolen.
    #[inline]
    pub unsafe fn pop(&self) -> Option<JobRef> {
        debug_assert!(!self.is_shadow_stack_empty());
        let shadow_stack_head = &*self.shadow_stack_head.get();
        let pushed = shadow_stack_head.pushed.get();
        debug_assert!(pushed > 0);
        if pushed > 1 {
            shadow_stack_head.pushed.set(pushed - 1);
        } else {
            shadow_stack_head.pushed.set(0);
            let parent = shadow_stack_head.parent;
            debug_assert!(!parent.is_null()); // we never pop the sentinel
            self.shadow_stack_head.set(parent);
        }

        self.worker.pop()
    }

    pub unsafe fn shadow_stack_head(&self) -> *const ShadowStackFrame {
        debug_assert!(!self.shadow_stack_head.get().is_null());
        self.shadow_stack_head.get()
    }

    pub unsafe fn is_shadow_stack_head(&self, shadow_frame: *const ShadowStackFrame) -> bool {
        self.shadow_stack_head.get() == shadow_frame
    }

    pub unsafe fn is_shadow_stack_empty(&self) -> bool {
        debug_assert!(!self.shadow_stack_head.get().is_null());

        let head = &*self.shadow_stack_head.get();
        debug_assert!(head.pushed.get() >= 1);

        // the shadow stack is considered empty if the only thing on
        // it is the sentinel, and its `pushed` count is 1
        if head.pushed.get() > 1 {
            return false; // pushed count too high
        }
        if !head.parent.is_null() {
            return false; // not the sentinel
        }

        // if the shadow stack is empty, should not be able to pop
        debug_assert!(self.worker.pop().is_none());

        true
    }

    unsafe fn clear_shadow_stack(&self) {
        self.shadow_stack_head.set(self.sentinel_frame);
        (*self.sentinel_frame).pushed.set(1);
    }

    pub unsafe fn push_shadow_frame(&self, shadow_frame: *const ShadowStackFrame) {
        debug_assert!(!shadow_frame.is_null());
        debug_assert!((*shadow_frame).parent == self.shadow_stack_head.get());
        self.shadow_stack_head.set(shadow_frame);
    }

    #[inline]
    pub unsafe fn pop_and_execute_all_but_one(&self, head: &ShadowStackFrame) -> bool {
        loop {
            if !self.is_shadow_stack_head(head) {
                // this shadow stack frame got popped somewhere along the way, abort
                return false;
            }
            if head.pushed.get() == 1 {
                // shadow stack is down to its last job, all set!
                return true;
            }

            if let Some(job) = self.pop() {
                // execute next job, then repeat
                job.execute(JobMode::Execute);
            } else {
                // job was stolen, abort; moreover, pop all shadow stacks
                self.clear_shadow_stack();
                return false;
            }
        }
    }

    /// Wait until the latch is set. Try to keep busy by popping and
    /// stealing tasks as necessary.
    #[inline]
    pub unsafe fn wait_until<L: Latch>(&self, latch: &L) {
        if !latch.probe() {
            self.wait_until_cold(latch);
        }
    }

    #[cold]
    unsafe fn wait_until_cold<L: Latch>(&self, latch: &L) {
        // the code below should swallow all panics and hence never
        // unwind; but if something does wrong, we want to abort,
        // because otherwise other code in rayon may assume that the
        // latch has been signaled, and hence that permit random
        // memory accesses, which would be *very bad*
        let guard = unwind::finally((), |_| process::exit(2222));

        while !latch.probe() {
            // if not, try to steal some more
            if !self.pop_or_steal_and_execute() {
                thread::yield_now();
            }
        }

        mem::forget(guard); // successful execution, do not abort
    }

    /// Try to steal a single job. If successful, execute it and
    /// return true. Else return false.
    unsafe fn pop_or_steal_and_execute(&self) -> bool {
        if let Some(job) = self.pop_or_steal() {
            job.execute(JobMode::Execute);
            true
        } else {
            false
        }
    }

    /// Try to pop a job locally; if none is found, try to steal a job.
    ///
    /// This is only used in the main worker loop or when stealing:
    /// code elsewhere never pops indiscriminantly, but always with
    /// some notion of the current stack depth.
    unsafe fn pop_or_steal(&self) -> Option<JobRef> {
        if !self.is_shadow_stack_empty() {
            if let Some(job) = self.pop() {
                return Some(job);
            }
            self.clear_shadow_stack();
        }
        self.steal()
    }

    /// Try to steal a single job and return it.
    ///
    /// This should only be done as a last resort, when there is no
    /// local work to do.
    unsafe fn steal(&self) -> Option<JobRef> {
        // we only steal when we don't have any work to do locally
        debug_assert!(self.worker.pop().is_none());

        // otherwise, try to steal
        if self.stealers.is_empty() {
            return None;
        }

        let start = {
            // OK to use this UnsafeCell because (a) this data is
            // confined to current thread, as WorkerThread is not Send
            // nor Sync and (b) rand crate will not call back into
            // this method.
            let rng = &mut *self.rng.get();
            rng.next_u32() % self.stealers.len() as u32
        };
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

    let sentinel_frame = ShadowStackFrame::new(ptr::null());
    sentinel_frame.pushed.set(1);

    let mut worker_thread = WorkerThread {
        worker: worker,
        stealers: stealers,
        index: index,
        rng: UnsafeCell::new(rand::weak_rng()),
        registry: registry.clone(),
        shadow_stack_head: Cell::new(&sentinel_frame),
        sentinel_frame: &sentinel_frame,
    };
    WorkerThread::set_current(&mut worker_thread);

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
    global_registry().inject(&[job.as_job_ref()]);
    job.latch.wait();
}
