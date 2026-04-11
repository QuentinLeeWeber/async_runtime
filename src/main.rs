use std::{
    future::{Future, IntoFuture},
    sync::{Arc, Condvar, Mutex},
    task::{Context, Poll, Wake, Waker},
};

fn main() {
    let result = test().block_on();
    println!("{}", result);
}

async fn test() -> &'static str {
    "yeet"
}

impl<F: Future> FutureExt for F {}
pub trait FutureExt: Future {
    fn block_on(self) -> Self::Output
    where
        Self: Sized,
    {
        let mut fut = core::pin::pin!(self.into_future());

        let signal = Arc::new(Signal::new());

        let waker = Waker::from(Arc::clone(&signal));
        let mut context = Context::from_waker(&waker);

        loop {
            match fut.as_mut().poll(&mut context) {
                Poll::Pending => signal.wait(),
                Poll::Ready(item) => break item,
            }
        }
    }
}

enum SignalState {
    Empty,
    Waiting,
    Notified,
}

struct Signal {
    state: Mutex<SignalState>,
    cond: Condvar,
}

impl Signal {
    fn new() -> Self {
        Self {
            state: Mutex::new(SignalState::Empty),
            cond: Condvar::new(),
        }
    }

    fn wait(&self) {
        let mut state = self.state.lock().unwrap();
        match *state {
            SignalState::Notified => *state = SignalState::Empty,
            SignalState::Waiting => {
                unreachable!("Multiple threads waiting on the same signal: Open a bug report!");
            }
            SignalState::Empty => {
                *state = SignalState::Waiting;
                while let SignalState::Waiting = *state {
                    state = self.cond.wait(state).unwrap();
                }
            }
        }
    }

    fn notify(&self) {
        let mut state = self.state.lock().unwrap();
        match *state {
            SignalState::Notified => {}
            SignalState::Empty => *state = SignalState::Notified,
            SignalState::Waiting => {
                *state = SignalState::Empty;
                self.cond.notify_one();
            }
        }
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
