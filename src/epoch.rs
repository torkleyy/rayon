use log::Event::*;
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
const ASLEEP: usize = 2;

impl Epoch {
    pub fn new() -> Epoch {
        Epoch {
            state: AtomicUsize::new(AWAKE),
            data: Mutex::new(()),
            work_available: Condvar::new(),
        }
    }

    #[inline]
    pub fn tickle(&self, worker_index: usize) {
        let old_state = self.state.swap(AWAKE, Ordering::Relaxed);
        log!(Tickle { worker: worker_index, old_state: old_state });
        if old_state == ASLEEP {
            self.awaken();
        }
    }

    #[cold]
    fn awaken(&self) {
        let _data = self.data.lock().unwrap();
        self.work_available.notify_all();
    }

    pub fn get_sleepy(&self, worker_index: usize) {
        // If the registry is AWAKE, make it SLEEPY. Otherwise, if
        // must be SLEEPY or ASLEEP already, so we can leave the state
        // the same.
        let r = self.state.compare_exchange(AWAKE, SLEEPY, Ordering::Relaxed, Ordering::Relaxed);
        log!(GetSleepy { worker: worker_index, result: r });
    }

    pub fn sleep(&self, worker_index: usize) {
        if self.state.load(Ordering::Relaxed) == AWAKE {
            // since we got sleepy, somebody woke us up
            log!(GotAwoken { worker: worker_index });
            return;
        } else {
            let data = self.data.lock().unwrap();
            loop {
                match self.state.compare_exchange_weak(SLEEPY, ASLEEP, Ordering::Relaxed, Ordering::Relaxed) {
                    Err(AWAKE) => {
                        // since we got sleepy, somebody woke us up
                        log!(GotAwoken { worker: worker_index });
                        return;
                    }
                    Ok(_) | Err(ASLEEP) => {
                        // registry is now asleep; wait to be awoken.
                        // Note that if there is a spurious wake-up,
                        // that's actually ok.
                        log!(FellAsleep { worker: worker_index });
                        let _ = self.work_available.wait(data).unwrap();
                        log!(GotAwoken { worker: worker_index });
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
