use scope;

use super::spawn_async;

#[test]
fn spawn_then_join_in_worker() {
    let mut result = None;
    scope(|_| {
        let mut j = spawn_async(move || 22);
        result = j.join();
    });
    assert_eq!(Some(22), result);
}

#[test]
fn spawn_then_join_outside_worker() {
    let mut task = spawn_async(move || 22);
    let result = task.join();
    assert_eq!(Some(22), result);
}


