use crate::event_loop::EventLoopHandle;
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

pub fn spawn<F>(fut: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send,
{
    let result = Arc::new(Mutex::new(ThreadResult::<F::Output>::default()));

    EventLoopHandle::current()
        .expect("spawn failed: no active runtime")
        .spawn(fut, result.clone());

    JoinHandle {
        result,
        has_registered: false,
    }
}

pub(crate) struct ThreadResult<T> {
    pub inner: Option<T>,
    pub waker: Option<Waker>,
}

impl<T> Default for ThreadResult<T> {
    fn default() -> Self {
        Self {
            inner: None,
            waker: None,
        }
    }
}

pub struct JoinHandle<T> {
    pub(crate) result: Arc<Mutex<ThreadResult<T>>>,
    has_registered: bool,
}

impl<T> JoinHandle<T> {
    pub fn is_ready(&self) -> bool {
        self.result.lock().unwrap().inner.is_some()
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.has_registered {
            self.result.lock().unwrap().waker = Some(cx.waker().clone());
            self.has_registered = true;
        }

        let result = self.result.lock().unwrap().inner.take();

        if let Some(result) = result {
            Poll::Ready(result)
        } else {
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use crate as async_runtime;

    use super::*;
    use async_runtime::time::sleep;
    use std::time::Duration;

    #[async_runtime::test]
    async fn thread_return_type_not_unit() {
        let handle = spawn(async { 42 });
        assert_eq!(handle.await, 42);
    }

    #[async_runtime::test]
    async fn thread_return_type_unit() {
        let handle = spawn(async {
            sleep(Duration::from_millis(1)).await;
            ()
        });
        assert_eq!(handle.await, ());
    }

    #[async_runtime::test(thread_count = 2)]
    async fn thread_return_type_unit_multi_thread() {
        let handle = spawn(async {
            sleep(Duration::from_millis(1)).await;
            ()
        });
        assert_eq!(handle.await, ());
    }
}
