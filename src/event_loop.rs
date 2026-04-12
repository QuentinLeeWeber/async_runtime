use std::{
    future::{Future, IntoFuture},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

use crate::signal::{Signal, SignalState};

pub struct EventLoopHandle {
    tasks: Vec<(Pin<Box<dyn Future<Output = ()>>>, Arc<Signal>)>,
    timers: Vec<(Instant, Waker)>,
}

impl EventLoopHandle {
    fn new() -> Self {
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
    pub fn new() -> (Self, Arc<Mutex<EventLoopHandle>>) {
        let handle = Arc::new(Mutex::new(EventLoopHandle::new()));
        (
            Self {
                handle: Arc::clone(&handle),
                timers: Vec::new(),
                tasks: Vec::new(),
            },
            handle,
        )
    }

    fn update(&mut self) -> bool {
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

        self.tasks.iter_mut().for_each(|(task, signal)| {
            if let SignalState::Waiting = *signal.state.lock().unwrap() {
                return;
            }

            match task
                .as_mut()
                .poll(&mut Context::from_waker(&Waker::from(Arc::clone(&signal))))
            {
                Poll::Pending => {
                    signal.pause();
                }
                Poll::Ready(_) => {
                    signal.ready();
                }
            }
        });

        if self.tasks.is_empty() {
            return true;
        }

        if let SignalState::Ready = *self.tasks.get(0).unwrap().1.state.lock().unwrap() {
            return true;
        }

        self.tasks.retain(|(_task, signal)| {
            if let SignalState::Ready = *signal.state.lock().unwrap() {
                return false;
            }
            true
        });

        false
    }

    pub fn block_on<F: Future<Output = ()> + 'static>(&mut self, future: F) {
        let signal = Arc::new(Signal::new());
        let task = Box::pin(future);
        self.tasks.push((task, Arc::clone(&signal)));

        loop {
            let block_on_finished = self.update();
            if block_on_finished {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}
