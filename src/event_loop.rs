use std::{
    cell::RefCell,
    future::{Future, IntoFuture},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

use crate::{
    signal::{Signal, SignalState},
    thread::ThreadResult,
};

thread_local! {
    static CURRENT_HANDLE: RefCell<Option<EventLoopHandle>> = const { RefCell::new(None) };
}

struct Task<T> {
    fut: Pin<Box<dyn Future<Output = T> + 'static>>,
    signal: Arc<Signal>,
    result: Arc<Mutex<ThreadResult<T>>>,
}

#[derive(Clone)]
pub struct EventLoopHandle {
    queues: Arc<Mutex<EventLoopQueue>>,
}

struct EventLoopQueue {
    tasks: Vec<Task<()>>,
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

    pub fn spawn<F>(&mut self, fut: F, result: Arc<Mutex<ThreadResult<F::Output>>>)
    where
        F: Future<Output = ()> + 'static,
    {
        let waker = Arc::new(Signal::new());

        let task = Task {
            fut: Box::pin(fut.into_future()),
            signal: waker,
            result,
        };

        self.queues.lock().unwrap().tasks.push(task);
    }

    pub fn current<'a>() -> Option<Self> {
        CURRENT_HANDLE.with(|cell| cell.borrow_mut().clone())
    }
}

pub struct EventLoop {
    timers: Vec<(Instant, Waker)>,
    tasks: Vec<Task<()>>,
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
        {
            let event_loop = EventLoopHandle::current().unwrap();
            let mut queues = event_loop.queues.lock().unwrap();

            self.tasks.append(&mut queues.tasks);
            self.timers.append(&mut queues.timers);
        }

        self.timers.retain(|(time, waker)| {
            if *time <= Instant::now() {
                waker.wake_by_ref();
                false
            } else {
                true
            }
        });

        self.tasks.iter_mut().for_each(|task| {
            if let SignalState::Waiting = *task.signal.state.lock().unwrap() {
                return;
            }

            match task
                .fut
                .as_mut()
                .poll(&mut Context::from_waker(&Waker::from(Arc::clone(
                    &task.signal,
                )))) {
                Poll::Pending => {
                    task.signal.pause();
                }
                Poll::Ready(result) => {
                    task.result.lock().unwrap().inner = Some(result);
                    if let Some(waker) = task.result.lock().unwrap().waker.take() {
                        waker.wake();
                    }
                    task.signal.ready();
                }
            }
        });

        if self.tasks.is_empty() {
            return true;
        }

        if let SignalState::Ready = *self.tasks.get(0).unwrap().signal.state.lock().unwrap() {
            return true;
        }

        self.tasks.retain(|task| {
            if let SignalState::Ready = *task.signal.state.lock().unwrap() {
                return false;
            }
            true
        });

        false
    }

    pub fn block_on<F: Future<Output = ()> + 'static>(&mut self, future: F) {
        let signal = Arc::new(Signal::new());
        let task = Task {
            fut: Box::pin(future),
            signal: Arc::clone(&signal),
            result: Arc::new(Mutex::new(ThreadResult {
                inner: None,
                waker: None,
            })),
        };
        self.tasks.push(task);

        loop {
            let block_on_finished = self.update();
            if block_on_finished {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

impl Drop for EventLoop {
    fn drop(&mut self) {
        CURRENT_HANDLE.set(None);
    }
}
