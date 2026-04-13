use crate::event_loop::EventLoopHandle;
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

pub fn spawn<F>(fut: F) -> JoinHandle<F::Output>
where
    F: Future<Output = ()> + 'static,
{
    let result = Arc::new(Mutex::new(ThreadResult::default()));

    EventLoopHandle::current()
        .expect("no active runtime")
        .spawn(fut, result.clone());

    JoinHandle {
        result,
        has_registered: false,
    }
}

#[derive(Default)]
pub(crate) struct ThreadResult<T> {
    pub inner: Option<T>,
    pub waker: Option<Waker>,
}

pub struct JoinHandle<T> {
    result: Arc<Mutex<ThreadResult<T>>>,
    has_registered: bool,
}

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.has_registered {
            self.result.lock().unwrap().waker = Some(cx.waker().clone());
            self.has_registered = true;
        }

        if let Some(result) = self.result.lock().unwrap().inner.take() {
            Poll::Ready(result)
        } else {
            Poll::Pending
        }
    }
}
