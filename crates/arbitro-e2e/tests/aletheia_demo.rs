use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::oneshot;

/// A subtle test case where a background task must be correctly synchronized.
///
/// INVARIANT: The 'task_count' must reach exactly 1 before the 'ready' signal is sent,
/// and must remain 1 until the test completes. Naive refactoring of the 'spawn'
/// block or the 'drop' behavior will break this.
#[tokio::test]
async fn test_subtle_background_invariant() {
    let task_count = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = oneshot::channel();

    let counter = Arc::clone(&task_count);
    let handle = tokio::spawn(async move {
        // Simulating complex initialization
        counter.fetch_add(1, Ordering::SeqCst);
        let _ = tx.send(());

        // This task MUST stay alive until the test is over.
        // If refactored into a scope that drops early, the 'counter' logic elsewhere might fail.
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    });

    // Wait for task to be "ready"
    rx.await.expect("Task failed to start");

    assert_eq!(
        task_count.load(Ordering::SeqCst),
        1,
        "Task must have incremented exactly once"
    );

    // CRITICAL: We must not drop 'handle' here if we depend on its continued existence.
    // A naive refactorer might think 'handle' is unused and remove it or 'await' it too early.

    // Perform "test logic"
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    assert_eq!(
        task_count.load(Ordering::SeqCst),
        1,
        "Task must still be active"
    );

    handle.abort();
}
