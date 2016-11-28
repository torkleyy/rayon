use log::Event::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};
use std::thread;
use std::usize;
use std::sync::atomic::Ordering::SeqCst;

// SUBTLE CORRECTNESS POINTS
//
// - Can't afford a "lost" tickle, because if thread X gets sleepy
//   and then misses a tickle, that might be the tickle to indicate that
//   its latch is set.
// - Sleeping while a latch is held: bad

/// The "epoch" is used to handle thread activity. The idea is that we
/// want worker threads to start to spin down when there is nothing to
/// do, but to spin up quickly.
pub struct Epoch {
    state: AtomicUsize,
    data: Mutex<()>,
    tickle: Condvar,
}

#[derive(Copy, Clone)]
struct State {
    value: usize
}

const AWAKE: usize = 0;
const SLEEPING: usize = 1;

impl State {
    fn new(f: usize) -> Self {
        State { value: f }
    }

    fn anyone_sleeping(self) -> bool {
        (self.value & SLEEPING) != 0
    }

    fn anyone_sleepy(self) -> bool {
        (self.value >> 1) != 0
    }

    fn worker_is_sleepy(self, worker_id: usize) -> bool {
        (self.value >> 1) == (worker_id + 1)
    }

    fn with_worker(self, worker_id: usize) -> State {
        let value = (worker_id + 1) << 1;
        let value = value + (self.value & 0x1);
        State { value: value }
    }
}

const N: usize = 0;

impl Epoch {
    pub fn new() -> Epoch {
        Epoch {
            state: AtomicUsize::new(AWAKE),
            data: Mutex::new(()),
            tickle: Condvar::new(),
        }
    }

    #[inline]
    fn load_state(&self, o: Ordering) -> State {
        State::new(self.state.load(o))
    }

    pub fn work_found(&self, worker_index: usize, yields: usize) -> usize {
        if yields >= N {
            self.tickle(worker_index);
        }
        0
    }

    pub fn no_work_found(&self, worker_index: usize, yields: usize) -> usize {
        if yields < N {
            thread::yield_now();
            yields + 1
        } else if yields == N {
            if self.get_sleepy(worker_index) {
                yields + 1
            } else {
                yields
            }
        } else {
            debug_assert_eq!(yields, N + 1);
            self.sleep(worker_index);
            0
        }
    }

    pub fn tickle(&self, worker_index: usize) {
        let old_state = self.load_state(SeqCst);
        if old_state.value != AWAKE {
            self.tickle_cold(worker_index);
        }
    }

    #[cold]
    fn tickle_cold(&self, worker_index: usize) {
        let _data = self.data.lock().unwrap();
        let old_state = State::new(self.state.swap(AWAKE, SeqCst));
        log!(Tickle { worker: worker_index, old_state: old_state.value });
        if old_state.anyone_sleeping() {
            self.tickle.notify_all();
        }
    }

    fn get_sleepy(&self, worker_index: usize) -> bool {
        let _data = self.data.lock().unwrap();
        let state = self.load_state(SeqCst);
        log!(GetSleepy { worker: worker_index, state: state.value });
        if state.anyone_sleepy() {
            // somebody else is already sleepy, so we'll just wait our turn
            false
        } else {
            // make ourselves the sleepy one
            let new_state = state.with_worker(worker_index);
            self.state.store(new_state.value, SeqCst);
            true
        }
    }

    fn sleep(&self, worker_index: usize) {
        let data = self.data.lock().unwrap();
        let state = self.load_state(SeqCst);
        if state.worker_is_sleepy(worker_index) {
            self.state.store(SLEEPING, SeqCst);

            // Don't do this in a loop. If we do it in a loop, we need
            // some way to distinguish the ABA scenario where the pool
            // was awoken but before we could process it somebody went
            // to sleep. Note that if we get a false wakeup it's not a
            // problem for us, we'll just loop around and maybe get
            // sleepy again.
            log!(FellAsleep { worker: worker_index });
            let _ = self.tickle.wait(data).unwrap();
            log!(GotAwoken { worker: worker_index });
        } else {
            log!(GotInterrupted { worker: worker_index });
        }
    }
}
