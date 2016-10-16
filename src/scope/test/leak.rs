use std::sync::{Condvar, Mutex};

pub struct Pool {
    counter: Mutex<usize>,
    cvar: Condvar,
}

impl Pool {
    pub fn new() -> Pool {
        Pool {
            counter: Mutex::new(0),
            cvar: Condvar::new(),
        }
    }

    pub fn alloc(&self) -> Object {
        println!("alloc");
        *self.counter.lock().unwrap() += 1;
        Object { pool: self }
    }

    fn free(&self) {
        println!("free");
        let mut counter = self.counter.lock().unwrap();
        *counter -= 1;
        self.cvar.notify_all();
    }

    fn block_until_all_freed(&self) {
        let mut counter = self.counter.lock().unwrap();
        while *counter > 0 {
            counter = self.cvar.wait(counter).unwrap();
        }
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        self.block_until_all_freed();
    }
}

pub struct Object<'a> {
    pool: &'a Pool
}

impl<'a> Object<'a> {
    pub fn touch(&self) { }
}

impl<'a> Clone for Object<'a> {
    fn clone(&self) -> Self {
        self.pool.alloc()
    }
}

impl<'a> Drop for Object<'a> {
    fn drop(&mut self) {
        self.pool.free();
    }
}
