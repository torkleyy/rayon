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
        if old_state != AWAKE {
            self.awaken();
        }
    }

    #[cold]
    fn awaken(&self) {
        let data = self.data.lock().unwrap();
        self.state.store(AWAKE, Ordering::Relaxed);
        self.work_available.notify_all();
    }

    pub fn sleep(&self) {
        let old_state = self.state.swap(SLEEPY, Ordering::Relaxed);
        let mut data = self.data.lock().unwrap();
        loop {
            let state = self.state.load(Ordering::Relaxed);
            if state == AWAKE {
                return;
            }
            match self.state.compare_exchange_weak(state,
                                                   ASLEEP,
                                                   Ordering::Relaxed,
                                                   Ordering::Relaxed) {
                Ok(_) => {
                    data = self.work_available.wait(data).unwrap();
                }
                Err(_) => { }
            }
        }
    }
}
