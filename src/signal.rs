use std::{
    sync::{Arc, Mutex},
    task::Wake,
};

#[derive(Debug)]
pub enum SignalState {
    Running,
    Waiting,
}

#[derive(Debug)]
pub struct Signal {
    pub state: Mutex<SignalState>,
}

impl Signal {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(SignalState::Running),
        }
    }

    pub fn pause(&self) {
        let mut state = self.state.lock().unwrap();
        *state = SignalState::Waiting;
    }

    pub fn notify(&self) {
        let mut state = self.state.lock().unwrap();
        *state = SignalState::Running;
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
