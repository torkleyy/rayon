use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::{Mutex, Condvar};
use std::ptr;
use std::thread;

/// We define various kinds of latches, which are all a primitive signaling
/// mechanism. A latch starts as false. Eventually someone calls `set()` and
/// it becomes true. You can test if it has been set by calling `probe()`.
///
/// Some kinds of latches, but not all, support a `wait()` operation
/// that will wait until the latch is set, blocking efficiently. That
/// is not part of the trait since it is not possibly to do with all
/// latches.
///
/// The intention is that `set()` is called once, but `probe()` may be
/// called any number of times. Once `probe()` returns true, the memory
/// effects that occurred before `set()` become visible.
///
/// It'd probably be better to refactor the API into two paired types,
/// but that's a bit of work, and this is not a public API.
pub trait Latch {
    /// Test if the latch is set.
    fn probe(&self) -> bool;

    /// Set the latch, signalling others.
    fn set(&self);
}

/// Spin latches are the simplest, most efficient kind, but they do
/// not support a `wait()` operation. They just have a boolean flag
/// that becomes true when `set()` is called.
pub struct SpinLatch {
    b: AtomicBool,
}

impl SpinLatch {
    #[inline]
    pub fn new() -> SpinLatch {
        SpinLatch { b: AtomicBool::new(false) }
    }

    /// Block until latch is set. Use with caution, this burns CPU.
    #[inline]
    pub fn spin(&self) {
        while !self.probe() {
            thread::yield_now();
        }
    }
}

impl Latch for SpinLatch {
    #[inline]
    fn probe(&self) -> bool {
        self.b.load(Ordering::Acquire)
    }

    #[inline]
    fn set(&self) {
        self.b.store(true, Ordering::Release);
    }
}

/// A Latch starts as false and eventually becomes true. You can block
/// until it becomes true.
pub struct LockLatch {
    m: Mutex<bool>,
    v: Condvar,
}

impl LockLatch {
    #[inline]
    pub fn new() -> LockLatch {
        LockLatch {
            m: Mutex::new(false),
            v: Condvar::new(),
        }
    }

    /// Block until latch is set.
    pub fn wait(&self) {
        let mut guard = self.m.lock().unwrap();
        while !*guard {
            guard = self.v.wait(guard).unwrap();
        }
    }
}

impl Latch for LockLatch {
    #[inline]
    fn probe(&self) -> bool {
        // Not particularly efficient, but we don't really use this operation
        let guard = self.m.lock().unwrap();
        *guard
    }

    #[inline]
    fn set(&self) {
        let mut guard = self.m.lock().unwrap();
        *guard = true;
        self.v.notify_all();
    }
}

/// A latch that starts as a spin-latch, but where a client can come
/// later and invoke `wait()`. This client will then install a mutex
/// so that they can block without spinning.
///
/// One special condition for an upgrade latch is that there must be
/// **exactly one** client that calls `wait()`. If multiple clients
/// attempt to call `wait()`, then panics may occur.
///
/// This is useful if you don't know if you will need to call `wait()`
/// or not. It is ever-so-slightly less efficient than the others for
/// the not knowing, since it requires an extra `if` in the `set()`
/// code.
pub struct UpgradeLatch {
    // The upgrade latch can be in one of three states:
    //
    // - initial: the data ptr is NULL, meaning that neither `set()` nor `wait()` has been called
    // - wait: when `wait()` is called, caller sets `data` a pointer `ll` to a `LockLatch`
    //   and block on `ll`
    // - set: when `set()` is called, we set data ptr to the sentinel `SET` value; if we were
    //   in wait state, then we `set` the lock-latch
    data: AtomicPtr<LockLatch>
}

const SET: *mut LockLatch = 0x1 as *mut LockLatch;

impl UpgradeLatch {
    #[inline]
    pub fn new() -> UpgradeLatch {
        UpgradeLatch {
            data: AtomicPtr::new(ptr::null_mut())
        }
    }

    /// Block until upgrade latch is set.
    ///
    /// # Panic warning
    ///
    /// This method is intended to be invoked only once. If this
    /// method is invoked more than once, and in particular more than
    /// once in parallel, then it may panic.
    pub fn wait(&self) {
        let mut latch = LockLatch::new();
        let latch_ptr: *mut LockLatch = &mut latch;
        loop {
            let data = self.data.load(Ordering::Acquire); // (*1)
            if data == SET {
                return; // all done
            } else if !data.is_null() {
                panic!("more than one actor is waiting on upgrade latch");
            }

            if let Ok(_) = self.data.compare_exchange_weak(ptr::null_mut(),
                                                           latch_ptr,
                                                           Ordering::Release, // (*2)
                                                           Ordering::Relaxed) { // (*3)
                // Success: installed our latch.
                latch.wait();
                return;
            }
        }

        // (*1) As in probe, we need to *acquire* any memory reads
        // before latch was set.
        //
        // (*2) On success: we need to *release* the memory
        // for our `LockLatch`, since the one who sets the
        // latch will be using it.
        //
        // (*3) On failure: no ordering requirements, we will just
        // loop again.
    }
}

impl Latch for UpgradeLatch {
    #[inline]
    fn probe(&self) -> bool {
        // Need to acquire any memory reads before latch was set:
        self.data.load(Ordering::Acquire) == SET
    }

    /// Set the latch to true, releasing all threads who are waiting.
    #[inline]
    fn set(&self) {
        let data: *mut LockLatch = self.data.swap(SET, Ordering::SeqCst); // (*1)

        // if someone called `wait()` and successfully installed a
        // `LockLatch`, then they are going to be blocking on it, so
        // we know the pointer must still be valid, and we should call
        // `set()`:
        if !data.is_null() {
            assert!(data != SET, "`set()` called more than once"); // (*2)
            unsafe {
                (*data).set();
            }
        }

        // (*1) We need to both *acquire* and *release*. We
        // are *acquiring* (potentially) the `LockLatch` installed by
        // caller of `wait`, and we are *releasing* whatever occurred
        // before this latch was `set`.
        //
        // (*2) Users are not supposed to invoke `set()` more than
        // once. But since this is a safe API, we can't assume that.
    }
}
