use std::{
    cell::UnsafeCell,
    collections::VecDeque,
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

unsafe impl<T> Send for Mutex<T> {}
unsafe impl<T> Sync for Mutex<T> {}

pub struct Mutex<T: ?Sized> {
    inner: Arc<UnsafeCell<T>>,
    queue: Arc<std::sync::Mutex<WakerQueue>>,
}

impl<T> Mutex<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner: Arc::new(UnsafeCell::new(inner)),
            queue: Arc::new(std::sync::Mutex::new(WakerQueue::default())),
        }
    }

    pub async fn lock(&self) -> MutexGuard<T> {
        LockFuture {
            queue: self.queue.clone(),
            id: None,
        }
        .await;

        let inner = Arc::clone(&self.inner);

        MutexGuard {
            queue: self.queue.clone(),
            inner,
        }
    }
}

#[derive(Debug, Default)]
struct WakerQueue {
    buf: VecDeque<(Waker, u64)>,
    next_id: u64,
}

struct LockFuture {
    queue: Arc<std::sync::Mutex<WakerQueue>>,
    id: Option<u64>,
}

impl Future for LockFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.id.is_none() {
            let next_id = {
                let mut queue = self.queue.lock().unwrap();
                let next_id = queue.next_id;
                queue.buf.push_back((cx.waker().clone(), next_id));
                queue.next_id += 1;
                next_id
            };
            self.id = Some(next_id);
        }

        let id = match self.id {
            Some(id) => id,
            None => return Poll::Pending,
        };

        if let Some(waker) = self.queue.lock().unwrap().buf.get(0) {
            if id == waker.1 {
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
    inner: Arc<UnsafeCell<T>>,
    queue: Arc<std::sync::Mutex<WakerQueue>>,
}

impl<T> DerefMut for MutexGuard<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.inner.get() }
    }
}

impl<T> Deref for MutexGuard<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.inner.get() }
    }
}

impl<T> Drop for MutexGuard<T> {
    fn drop(&mut self) {
        let mut queue = self.queue.lock().unwrap();
        queue.buf.remove(0);
        if let Some(next_waker) = queue.buf.get(0).cloned() {
            queue.buf.pop_front();
            next_waker.0.wake_by_ref();
        }
    }
}
