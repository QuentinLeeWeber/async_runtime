use super::{CURRENT_HANDLE, EventLoopHandle, prelude::*, signal::SignalState, task::Task};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread::{self, JoinHandle},
};

pub struct SingleThreadedPool {
    tasks: Vec<Task<()>>,
}

pub struct MultiThreadedPool {
    worker: Vec<Worker>,
    tasks: Vec<Task<()>>,
    task_return: mpsc::Receiver<Task<()>>,
}

impl SingleThreadedPool {
    pub fn new() -> Self {
        Self { tasks: Vec::new() }
    }

    pub fn add_tasks(&mut self, tasks: Vec<Task<()>>) {
        self.tasks.extend(tasks);
    }

    pub fn update(&mut self) {
        self.tasks
            .iter_mut()
            .filter(|t| *t.signal.state.lock().unwrap() == SignalState::Awaked)
            .for_each(|task| {
                task.poll();
            });

        self.tasks.retain(|task| {
            if let SignalState::Ready = *task.signal.state.lock().unwrap() {
                return false;
            }
            true
        });
    }
}

impl MultiThreadedPool {
    pub fn new(num_worker: usize) -> (Self, Vec<EventLoopHandle>) {
        let (return_tx, return_rx) = mpsc::channel();
        let (worker, handles) = (0..num_worker)
            .into_iter()
            .map(|_| Worker::new(return_tx.clone()))
            .collect();

        let this = Self {
            worker,
            tasks: Vec::new(),
            task_return: return_rx,
        };

        (this, handles)
    }

    pub fn add_tasks(&mut self, tasks: Vec<Task<()>>) {
        self.tasks.extend(tasks);
    }

    pub fn update(&mut self) {
        while let Ok(task) = self.task_return.try_recv() {
            self.tasks.push(task);
        }

        let mut awaked_tasks = self
            .tasks
            .retain_filter(|task| *task.signal.state.lock().unwrap() == SignalState::Awaked);

        for worker in self.worker.iter_mut() {
            if worker.is_available.load(Ordering::SeqCst) {
                match awaked_tasks.pop() {
                    Some(task) => {
                        worker.is_available.store(false, Ordering::Release);
                        worker.tx.send(task).expect("could not send task to worker")
                    }
                    None => return,
                }
            }
        }
    }
}

struct Worker {
    _handle: JoinHandle<()>,
    tx: mpsc::Sender<Task<()>>,
    is_available: Arc<AtomicBool>,
}

impl Worker {
    fn new(task_return: mpsc::Sender<Task<()>>) -> (Self, EventLoopHandle) {
        let (tx, rx) = mpsc::channel();
        let is_available = Arc::new(AtomicBool::new(true));
        let handle = EventLoopHandle::new();

        let this = Self {
            _handle: thread::spawn({
                let is_available = Arc::clone(&is_available);
                let handle = handle.clone();
                move || {
                    CURRENT_HANDLE.with(|cell| cell.replace(Some(handle)));
                    Self::routine(rx, is_available, task_return)
                }
            }),
            tx,
            is_available,
        };

        (this, handle)
    }

    #[inline]
    fn routine(
        rx: mpsc::Receiver<Task<()>>,
        is_available: Arc<AtomicBool>,
        task_return: mpsc::Sender<Task<()>>,
    ) {
        loop {
            let mut task = rx.recv().unwrap();
            task.poll();
            if *task.signal.state.lock().unwrap() != SignalState::Ready {
                task_return.send(task).unwrap();
            }
            is_available.store(true, Ordering::SeqCst);
        }
    }
}
