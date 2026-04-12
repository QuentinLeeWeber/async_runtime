use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

mod event_loop;
mod signal;
mod sleep;

use event_loop::{EventLoop, EventLoopHandle};
use sleep::SleepFuture;

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

    loop {
        event_loop.update();
        std::thread::sleep(Duration::from_millis(1));
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
