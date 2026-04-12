use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, Instant},
};

use crate::event_loop::EventLoopHandle;

pub struct SleepFuture {
    completion_time: Instant,
    event_loop: Arc<Mutex<EventLoopHandle>>,
    is_spawned: bool,
}

impl SleepFuture {
    pub fn new(duration: Duration, event_loop: Arc<Mutex<EventLoopHandle>>) -> Self {
        Self {
            completion_time: Instant::now() + duration,
            event_loop,
            is_spawned: false,
        }
    }
}

impl Future for SleepFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let completion_time = self.completion_time.clone();

        if !self.is_spawned {
            self.event_loop
                .lock()
                .unwrap()
                .add_timer(completion_time, cx.waker().clone());
            self.is_spawned = true;
        }

        if completion_time <= Instant::now() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}
