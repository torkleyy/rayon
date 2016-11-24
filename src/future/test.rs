use futures::future;

use futures::future::{Future, BoxFuture};
use super::spawn_future;

fn done<T: Send + 'static>(t: T) -> BoxFuture<T, ()> {
    future::ok(t).boxed()
}

#[allow(unreachable_code)]
fn argh<T: Send + 'static>() -> BoxFuture<T, ()> {
    future::lazy(|| future::ok(panic!("hello, world!"))).boxed()
}

#[test]
fn join() {
    let a = spawn_future(done(1));
    let b = spawn_future(done(2));
    let res = a.join(b).map(|(a, b)| a + b).wait();

    assert_eq!(res.unwrap(), 3);
}

#[test]
fn select() {
    let a = spawn_future(done(1));
    let b = spawn_future(done(2));
    let (item1, next) = a.select(b).wait().ok().unwrap();
    let item2 = next.wait().unwrap();

    assert!(item1 != item2);
    assert!((item1 == 1 && item2 == 2) || (item1 == 2 && item2 == 1));
}

#[test]
#[should_panic]
fn panic_prop() {
    let a = spawn_future(argh::<()>());
    let _ = a.wait(); // should panic, not return error
}

