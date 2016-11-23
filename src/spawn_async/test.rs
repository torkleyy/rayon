use scope;

use super::spawn_async;

#[test]
fn spawn_then_join() {
    let mut result = None;
    scope(|_| {
        let mut j = spawn_async(move || 22);
        result = j.join();
    });
    assert_eq!(Some(22), result);
}

