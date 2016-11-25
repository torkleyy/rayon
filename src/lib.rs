#![allow(non_camel_case_types)] // I prefer to use ALL_CAPS for type parameters
#![cfg_attr(test, feature(conservative_impl_trait))]

extern crate deque;
#[cfg(feature = "unstable")]
extern crate futures;
extern crate libc;
extern crate num_cpus;
extern crate rand;

#[macro_use]
mod log;

mod api;
mod future;
mod latch;
mod join;
mod job;
pub mod par_iter;
pub mod prelude;
#[cfg(test)]
mod test;
#[cfg(feature = "unstable")]
mod scope;
#[cfg(feature = "unstable")]
mod spawn_async;
mod thread_pool;
mod unwind;
mod util;

pub use api::Configuration;
pub use api::InitError;
pub use api::dump_stats;
pub use api::initialize;
pub use api::ThreadPool;
#[cfg(feature = "unstable")]
pub use future::{RayonFuture, spawn_future_async};
pub use join::join;
#[cfg(feature = "unstable")]
pub use spawn_async::spawn_async;
#[cfg(feature = "unstable")]
pub use scope::{scope, Scope};
