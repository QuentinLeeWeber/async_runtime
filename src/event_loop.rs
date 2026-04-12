use std::{
    cell::RefCell,
    future::{Future, IntoFuture},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

use crate::signal::{Signal, SignalState};

thread_local! {
    static CURRENT_HANDLE: RefCell<Option<EventLoopHandle>> = const { RefCell::new(None) };
}

#[derive(Clone)]
pub struct EventLoopHandle {
    queues: Arc<Mutex<EventLoopQueue>>,
}

struct EventLoopQueue {
    tasks: Vec<(Pin<Box<dyn Future<Output = ()>>>, Arc<Signal>)>,
    timers: Vec<(Instant, Waker)>,
}

impl EventLoopHandle {
    fn new() -> Self {
        Self {
            queues: Arc::new(Mutex::new(EventLoopQueue {
                tasks: Vec::new(),
                timers: Vec::new(),
            })),
        }
    }

    pub fn add_timer(&mut self, time: Instant, waker: Waker) {
        self.queues.lock().unwrap().timers.push((time, waker));
    }

    pub fn spawn<F>(&mut self, fut: F)
    where
        F: Future<Output = ()> + 'static,
    {
        let waker = Arc::new(Signal::new());
        self.queues
            .lock()
            .unwrap()
            .tasks
            .push((Box::pin(fut.into_future()), waker));
    }

    pub fn current<'a>() -> Option<Self> {
        CURRENT_HANDLE.with(|cell| cell.borrow_mut().clone())
    }
}

pub struct EventLoop {
    timers: Vec<(Instant, Waker)>,
    tasks: Vec<(Pin<Box<dyn Future<Output = ()>>>, Arc<Signal>)>,
}

impl EventLoop {
    pub fn new() -> Self {
        let handle = EventLoopHandle::new();
        CURRENT_HANDLE.with(|cell| cell.replace(Some(handle)));

        Self {
            timers: Vec::new(),
            tasks: Vec::new(),
        }
    }

    fn update(&mut self) -> bool {
        self.tasks.append(
            &mut EventLoopHandle::current()
                .unwrap()
                .queues
                .lock()
                .unwrap()
                .tasks,
        );

        self.timers.append(
            &mut EventLoopHandle::current()
                .unwrap()
                .queues
                .lock()
                .unwrap()
                .timers,
        );

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
