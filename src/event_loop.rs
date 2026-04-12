use std::{
    future::{Future, IntoFuture},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    time::Instant,
};

use crate::signal::{Signal, SignalState};

pub struct EventLoopHandle {
    tasks: Vec<(Pin<Box<dyn Future<Output = ()>>>, Arc<Signal>)>,
    timers: Vec<(Instant, Waker)>,
}

impl EventLoopHandle {
    pub fn new() -> Self {
        Self {
            tasks: Vec::new(),
            timers: Vec::new(),
        }
    }

    pub fn add_timer(&mut self, time: Instant, waker: Waker) {
        self.timers.push((time, waker));
    }

    pub fn spawn<F>(&mut self, fut: F)
    where
        F: Future<Output = ()> + 'static,
    {
        let waker = Arc::new(Signal::new());
        self.tasks.push((Box::pin(fut.into_future()), waker));
    }
}

pub struct EventLoop {
    handle: Arc<Mutex<EventLoopHandle>>,
    timers: Vec<(Instant, Waker)>,
    tasks: Vec<(Pin<Box<dyn Future<Output = ()>>>, Arc<Signal>)>,
}

impl EventLoop {
    pub fn new(handle: Arc<Mutex<EventLoopHandle>>) -> Self {
        Self {
            handle,
            timers: Vec::new(),
            tasks: Vec::new(),
        }
    }

    pub fn update(&mut self) {
        self.tasks.append(&mut self.handle.lock().unwrap().tasks);
        self.timers.append(&mut self.handle.lock().unwrap().timers);

        self.timers.retain(|(time, waker)| {
            if *time <= Instant::now() {
                waker.wake_by_ref();
                false
            } else {
                true
            }
        });

        self.tasks.retain_mut(|(task, signal)| {
            if let SignalState::Waiting = *signal.state.lock().unwrap() {
                return true;
            }

            match task
                .as_mut()
                .poll(&mut Context::from_waker(&Waker::from(Arc::clone(&signal))))
            {
                Poll::Pending => {
                    signal.pause();
                    true
                }
                Poll::Ready(_) => false,
            }
        });
    }
}
