use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

mod event_loop;
mod signal;
mod sleep;

use event_loop::{EventLoop, EventLoopHandle};
use sleep::SleepFuture;

fn main() {
    let (mut event_loop, event_loop_handle) = EventLoop::new();

    event_loop.block_on(async move {
        event_loop_handle.lock().unwrap().spawn(test_loop(
            event_loop_handle.clone(),
            String::from("test_loop 1"),
            Duration::from_millis(0),
        ));

        test_loop(
            event_loop_handle.clone(),
            String::from("test_loop 3"),
            Duration::from_millis(1500),
        )
        .await
    });
}

async fn test_loop(
    event_loop: Arc<Mutex<EventLoopHandle>>,
    text: String,
    start_duration: Duration,
) {
    SleepFuture::new(start_duration, event_loop.clone()).await;
    loop {
        println!("3 sec {text}");
        SleepFuture::new(Duration::from_secs(3), event_loop.clone()).await;
    }
}
