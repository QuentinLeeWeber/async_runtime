use std::{
    future::{Future, IntoFuture},
    pin::Pin,
    sync::{Arc, Condvar, Mutex},
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};

const EXTRA_DEBUG: bool = false;

fn main() {
    let event_loop_handle = Arc::new(Mutex::new(EventLoopHandle::new()));
    let mut event_loop = EventLoop::new(event_loop_handle.clone());

    let result = test().block_on();
    if EXTRA_DEBUG {
        println!("{}", result);
    }

    event_loop_handle
        .lock()
        .unwrap()
        .spawn(test_loop(event_loop_handle.clone()));

    event_loop_handle
        .lock()
        .unwrap()
        .spawn(test_loop2(event_loop_handle.clone()));

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

async fn test() -> &'static str {
    "yeet"
}

async fn test_loop(event_loop: Arc<Mutex<EventLoopHandle>>) {
    println!("test_loop");
    loop {
        if EXTRA_DEBUG {
            println!();
        }
        println!("start loop");
        let sleep = SleepFuture::new(Duration::from_secs(3), event_loop.clone());
        println!("created sleep future");
        sleep.await;
        println!("3 sec");
    }
}

async fn test_loop2(event_loop: Arc<Mutex<EventLoopHandle>>) {
    SleepFuture::new(Duration::from_millis(1500), event_loop.clone()).await;

    println!("test_loop");
    loop {
        if EXTRA_DEBUG {
            println!();
        }
        println!("start loop");
        let sleep = SleepFuture::new(Duration::from_secs(3), event_loop.clone());
        println!("created sleep future");
        sleep.await;
        println!("3 sec NUMMER 2");
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
                .add_timer(completion_time - Instant::now(), cx.waker().clone());
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
    tasks: Vec<Pin<Box<dyn Future<Output = ()>>>>,
    timers: Vec<(Duration, Waker)>,
}

impl EventLoopHandle {
    fn new() -> Self {
        Self {
            tasks: Vec::new(),
            timers: Vec::new(),
        }
    }

    fn add_timer(&mut self, duration: Duration, waker: Waker) {
        self.timers.push((duration, waker));
    }

    fn spawn<F>(&mut self, fut: F)
    where
        F: Future<Output = ()> + 'static,
    {
        self.tasks.push(Box::pin(fut.into_future()));
    }
}

struct EventLoop {
    handle: Arc<Mutex<EventLoopHandle>>,
    timers: Vec<(Duration, Waker)>,
    tasks: Vec<Pin<Box<dyn Future<Output = ()>>>>,
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
        for (duration, waker) in self.timers.iter() {
            if *duration <= Duration::from_secs(0) {
                waker.wake_by_ref();
            }
        }

        self.timers
            .retain(|(duration, _)| *duration > Duration::from_secs(0));

        /*for task in self.tasks.iter_mut() {
            let waker = Waker::from(Arc::new(Signal::new()));
            let mut context = Context::from_waker(&waker);
            match task.as_mut().poll(&mut context) {
                Poll::Pending => {}
                Poll::Ready(_) => {
                    //self.tasks.retain(|t| t != task);
                }
            }
        }*/
        self.tasks.retain_mut(|task| {
            task.as_mut()
                .poll(&mut Context::from_waker(&Waker::from(Arc::new(
                    Signal::new(),
                ))))
                .is_pending()
        });
    }
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
