use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};

/// The "epoch" is used to handle thread activity. The idea is that we
/// want worker threads to start to spin down when there is nothing to
/// do, but to spin up quickly.
pub struct Epoch {
    state: AtomicUsize,
    data: Mutex<()>,
    work_available: Condvar,
}

const AWAKE: usize = 0;
const SLEEPY: usize = 1;
const ASLEEP: usize = 1;

impl Epoch {
    pub fn new() -> Epoch {
        Epoch {
            state: AtomicUsize::new(AWAKE),
            data: Mutex::new(()),
            work_available: Condvar::new(),
        }
    }

    #[inline]
    pub fn tickle(&self) {
        let old_state = self.state.swap(AWAKE, Ordering::Relaxed);
        if old_state == ASLEEP {
            self.awaken();
        }
    }

    #[cold]
    fn awaken(&self) {
        let data = self.data.lock().unwrap();
        self.work_available.notify_all();
    }

    pub fn get_sleepy(&self) {
        // If the registry is AWAKE, make it SLEEPY. Otherwise, if
        // must be SLEEPY or ASLEEP already, so we can leave the state
        // the same.
        self.state.compare_exchange(AWAKE, SLEEPY, Ordering::Relaxed, Ordering::Relaxed);
    }

    pub fn sleep(&self) {
        if self.state.load(Ordering::Relaxed) == AWAKE {
            // since we got sleepy, somebody woke us up
            return;
        } else {
            let mut data = self.data.lock().unwrap();
            loop {
                match self.state.compare_exchange_weak(SLEEPY, ASLEEP, Ordering::Relaxed, Ordering::Relaxed) {
                    Err(AWAKE) => {
                        // since we got sleepy, somebody woke us up
                        return;
                    }
                    Ok(_) | Err(ASLEEP) => {
                        // registry is now asleep; wait to be awoken.
                        // Note that if there is a spurious wake-up,
                        // that's actually ok.
                        data = self.work_available.wait(data).unwrap();
                        return;
                    }
                    Err(_) => {
                        // spurious failure
                    }
                }
            }
        }
    }
}
