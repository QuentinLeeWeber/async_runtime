use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

pub struct Mutex<T> {
    inner: Arc<std::sync::Mutex<UnsafeCell<T>>>,
    queue: Arc<std::sync::Mutex<Vec<Waker>>>,
}

impl<T> Mutex<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner: Arc::new(std::sync::Mutex::new(UnsafeCell::new(inner))),
            queue: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    pub async fn lock(&self) -> MutexGuard<T> {
        LockFuture {
            queue: self.queue.clone(),
            has_registered: false,
        }
        .await;

        let inner = Arc::clone(&self.inner);

        MutexGuard {
            queue: self.queue.clone(),
            inner,
        }
    }
}

struct LockFuture {
    queue: Arc<std::sync::Mutex<Vec<Waker>>>,
    has_registered: bool,
}

impl Future for LockFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let waker = cx.waker();

        if !self.has_registered {
            self.queue.lock().unwrap().push(waker.clone());
            self.has_registered = true;
        }

        let queue = self.queue.lock().unwrap();

        if let Some(waker) = queue.iter().peekable().peek() {
            if waker.will_wake(waker) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        } else {
            unreachable!("mutex queue is empty, while trying to acquire lock")
        }
    }
}

pub struct MutexGuard<T> {
    inner: Arc<std::sync::Mutex<UnsafeCell<T>>>,
    queue: Arc<std::sync::Mutex<Vec<Waker>>>,
}

impl<T> DerefMut for MutexGuard<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.inner.lock().unwrap().get() }
    }
}

impl<T> Deref for MutexGuard<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.inner.lock().unwrap().get() }
    }
}

impl<T> Drop for MutexGuard<T> {
    fn drop(&mut self) {
        let mut queue = self.queue.lock().unwrap();
        queue.remove(0);

        if let Some(next_waker) = queue.iter().peekable().peek_mut() {
            next_waker.wake_by_ref();
        }
    }
}
