use std::{
    future::{Future, IntoFuture},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};

const EXTRA_DEBUG: bool = false;

fn main() {
    let event_loop_handle = Arc::new(Mutex::new(EventLoopHandle::new()));
    let mut event_loop = EventLoop::new(event_loop_handle.clone());

    event_loop_handle.lock().unwrap().spawn(test_loop(
        event_loop_handle.clone(),
        String::from("test_loop 1"),
        Duration::from_millis(0),
    ));

    event_loop_handle.lock().unwrap().spawn(test_loop(
        event_loop_handle.clone(),
        String::from("test_loop 2"),
        Duration::from_millis(1500),
    ));

    println!("end");
    loop {
        event_loop.update();
        if EXTRA_DEBUG {
            println!();
        }
        std::thread::sleep(Duration::from_millis(1));
        if EXTRA_DEBUG {
            println!("timers: {:?}", event_loop.timers);
        }
    }
}

async fn test_loop(
    event_loop: Arc<Mutex<EventLoopHandle>>,
    text: String,
    start_duration: Duration,
) {
    println!("test_loop");
    SleepFuture::new(start_duration, event_loop.clone()).await;
    loop {
        if EXTRA_DEBUG {
            println!();
        }
        println!("start loop");
        let sleep = SleepFuture::new(Duration::from_secs(3), event_loop.clone());
        println!("created sleep future");
        sleep.await;
        println!("3 sec {text}");
    }
}

struct SleepFuture {
    completion_time: Instant,
    event_loop: Arc<Mutex<EventLoopHandle>>,
    is_spawned: bool,
}

impl SleepFuture {
    fn new(duration: Duration, event_loop: Arc<Mutex<EventLoopHandle>>) -> Self {
        Self {
            completion_time: Instant::now() + duration,
            event_loop,
            is_spawned: false,
        }
    }
}

impl Future for SleepFuture {
    type Output = ();

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if EXTRA_DEBUG {
            println!("poll sleep future");
        }
        let completion_time = self.completion_time.clone();

        if !self.is_spawned {
            self.event_loop
                .lock()
                .unwrap()
                .add_timer(completion_time, cx.waker().clone());
            self.is_spawned = true;
        }

        if completion_time <= Instant::now() {
            if EXTRA_DEBUG {
                println!("ready");
            }
            Poll::Ready(())
        } else {
            if EXTRA_DEBUG {
                println!("not ready");
            }
            Poll::Pending
        }
    }
}

struct EventLoopHandle {
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

    fn add_timer(&mut self, time: Instant, waker: Waker) {
        if EXTRA_DEBUG {
            println!("add_timer")
        }
        self.timers.push((time, waker));
    }

    fn spawn<F>(&mut self, fut: F)
    where
        F: Future<Output = ()> + 'static,
    {
        let waker = Arc::new(Signal::new());
        self.tasks.push((Box::pin(fut.into_future()), waker));
    }
}

struct EventLoop {
    handle: Arc<Mutex<EventLoopHandle>>,
    timers: Vec<(Instant, Waker)>,
    tasks: Vec<(Pin<Box<dyn Future<Output = ()>>>, Arc<Signal>)>,
}

impl EventLoop {
    fn new(handle: Arc<Mutex<EventLoopHandle>>) -> Self {
        Self {
            handle,
            timers: Vec::new(),
            tasks: Vec::new(),
        }
    }

    fn update(&mut self) {
        self.tasks.append(&mut self.handle.lock().unwrap().tasks);
        self.timers.append(&mut self.handle.lock().unwrap().timers);

        if EXTRA_DEBUG {
            println!("update event loop");
        }

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

#[derive(Debug)]
enum SignalState {
    Running,
    Waiting,
}

#[derive(Debug)]
struct Signal {
    state: Mutex<SignalState>,
}

impl Signal {
    fn new() -> Self {
        Self {
            state: Mutex::new(SignalState::Running),
        }
    }

    fn pause(&self) {
        let mut state = self.state.lock().unwrap();
        *state = SignalState::Waiting;
    }

    fn notify(&self) {
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
