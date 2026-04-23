use crate::{
    event_loop::task::TaskHeader,
    thread::{self, ThreadResult},
};
use std::{
    cell::RefCell,
    future::{Future, IntoFuture},
    sync::{Arc, Mutex},
    task::Waker,
    time::{Duration, Instant},
};

mod prelude;
mod signal;
mod task;
mod thread_pool;

use prelude::*;
use signal::SignalState;
use task::Task;
use thread_pool::{MultiThreadedPool, SingleThreadedPool};

thread_local! {
    pub(crate) static CURRENT_HANDLE: RefCell<Option<EventLoopHandle>> = const { RefCell::new(None) };
}

#[derive(Clone)]
pub(crate) struct EventLoopHandle {
    queues: Arc<Mutex<EventLoopQueue>>,
}

struct EventLoopQueue {
    tasks: Vec<Box<dyn TaskHeader>>,
    timers: Vec<(Instant, Waker)>,
}

impl EventLoopHandle {
    pub(crate) fn new() -> Self {
        Self {
            queues: Arc::new(Mutex::new(EventLoopQueue {
                tasks: Vec::new(),
                timers: Vec::new(),
            })),
        }
    }

    pub(crate) fn add_timer(&mut self, time: Instant, waker: Waker) {
        self.queues.lock().unwrap().timers.push((time, waker));
    }

    pub fn spawn<F>(&mut self, fut: F, result: Arc<Mutex<ThreadResult<F::Output>>>)
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        let task = Box::new(Task::new(fut.into_future(), result));

        self.queues.lock().unwrap().tasks.push(task);
    }

    pub(crate) fn current() -> Option<Self> {
        CURRENT_HANDLE.with(|cell| cell.borrow_mut().clone())
    }
}

enum EventLoopMode {
    SingleThreaded {
        event_loop_handle: EventLoopHandle,
        worker: SingleThreadedPool,
    },
    MultiThreaded {
        event_loop_handles: Vec<EventLoopHandle>,
        worker_pool: MultiThreadedPool,
    },
}

pub struct EventLoop {
    timers: Vec<(Instant, Waker)>,
    tasks: Vec<Box<dyn TaskHeader>>,
    mode: EventLoopMode,
}

impl EventLoop {
    pub fn new(thread_count: usize) -> Self {
        let handle = EventLoopHandle::new();
        CURRENT_HANDLE.with(|cell| cell.replace(Some(handle.clone())));

        let mode = if thread_count == 1 {
            println!("Event Loop Mode: single threaded");
            EventLoopMode::SingleThreaded {
                event_loop_handle: handle,
                worker: SingleThreadedPool::new(),
            }
        } else {
            println!("Event Loop Mode: multi threaded ({} threads)", thread_count);
            let (worker_pool, mut handles) = MultiThreadedPool::new(thread_count);
            handles.push(handle);
            EventLoopMode::MultiThreaded {
                event_loop_handles: handles,
                worker_pool,
            }
        };

        Self {
            timers: Vec::new(),
            tasks: Vec::new(),
            mode,
        }
    }

    fn update(&mut self) {
        self.timers.retain(|(time, waker)| {
            if *time <= Instant::now() {
                waker.wake_by_ref();
                false
            } else {
                true
            }
        });

        match self.mode {
            EventLoopMode::SingleThreaded {
                ref event_loop_handle,
                ref mut worker,
            } => {
                {
                    let mut queues = event_loop_handle.queues.lock().unwrap();
                    self.tasks.append(&mut queues.tasks);
                    self.timers.append(&mut queues.timers);
                }
                let awaked_tasks = self.tasks.retain_filter(|task| {
                    matches!(*task.signal().state.lock().unwrap(), SignalState::Awaked)
                });

                worker.add_tasks(awaked_tasks);
                worker.update();
            }
            EventLoopMode::MultiThreaded {
                ref event_loop_handles,
                ref mut worker_pool,
            } => {
                for handle in event_loop_handles {
                    let mut queues = handle.queues.lock().unwrap();
                    self.tasks.append(&mut queues.tasks);
                    self.timers.append(&mut queues.timers);
                }

                let awaked_tasks = self.tasks.retain_filter(|task| {
                    matches!(*task.signal().state.lock().unwrap(), SignalState::Awaked)
                });

                worker_pool.add_tasks(awaked_tasks);
                worker_pool.update();
            }
        }

        self.tasks.retain(|task| {
            if let SignalState::Ready = *task.signal().state.lock().unwrap() {
                return false;
            }
            true
        });
    }

    pub fn block_on<F>(&mut self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let main = thread::spawn(future);

        loop {
            self.update();
            if main.is_ready() {
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
