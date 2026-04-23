use super::{signal::Signal, thread::ThreadResult};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

pub(crate) struct Task<T> {
    pub fut: Pin<Box<dyn Future<Output = T> + Send + 'static>>,
    pub signal: Arc<Signal>,
    pub result: Arc<Mutex<ThreadResult<T>>>,
}

pub(crate) trait TaskHeader: Send {
    fn poll(&mut self);
    fn signal(&self) -> Arc<Signal>;
}

impl<T> TaskHeader for Task<T>
where
    T: Send,
{
    fn signal(&self) -> Arc<Signal> {
        Arc::clone(&self.signal)
    }

    fn poll(&mut self) {
        match self
            .fut
            .as_mut()
            .poll(&mut Context::from_waker(&Waker::from(Arc::clone(
                &self.signal,
            )))) {
            Poll::Pending => {
                self.signal.pause();
            }
            Poll::Ready(result) => {
                let mut this_result = self.result.lock().unwrap();
                this_result.inner = Some(result);
                if let Some(waker) = this_result.waker.take() {
                    waker.wake();
                }
                self.signal.ready();
            }
        }
    }
}
