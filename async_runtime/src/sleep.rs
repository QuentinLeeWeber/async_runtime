use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use crate::event_loop::EventLoopHandle;

pub struct SleepFuture {
    completion_time: Instant,
    is_spawned: bool,
}

impl SleepFuture {
    pub fn new(duration: Duration) -> Self {
        Self {
            completion_time: Instant::now() + duration,
            is_spawned: false,
        }
    }
}

impl Future for SleepFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let completion_time = self.completion_time;

        if !self.is_spawned {
            let mut event_loop =
                EventLoopHandle::current().expect("sleep failed: no active runtime");
            event_loop.add_timer(completion_time, cx.waker().clone());

            self.is_spawned = true;
        }

        if completion_time <= Instant::now() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}
