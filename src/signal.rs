use std::{
    sync::{Arc, Mutex},
    task::Wake,
};

#[derive(Debug, Eq, PartialEq)]
pub enum SignalState {
    // Task is running / assigned to the worker pool
    Running,
    // Task waiting to be assigned to a worker
    Awaked,
    // Task is waiting to be awaked
    Waiting,
    // Task is done, and can be removed
    Ready,
}

#[derive(Debug)]
pub struct Signal {
    pub state: Mutex<SignalState>,
}

impl Signal {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(SignalState::Awaked),
        }
    }

    pub fn pause(&self) {
        let mut state = self.state.lock().unwrap();
        *state = SignalState::Waiting;
    }

    pub fn notify(&self) {
        let mut state = self.state.lock().unwrap();
        *state = SignalState::Awaked;
    }

    pub fn ready(&self) {
        let mut state = self.state.lock().unwrap();
        *state = SignalState::Ready;
    }
}

impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        self.notify();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.notify();
    }
}
