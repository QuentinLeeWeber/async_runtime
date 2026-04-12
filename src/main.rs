use std::time::Duration;

mod event_loop;
mod signal;
mod sleep;
mod thread;

use event_loop::EventLoop;
use sleep::SleepFuture;

fn main() {
    EventLoop::new().block_on(async move {
        thread::spawn(test_loop(
            String::from("test_loop 1"),
            Duration::from_millis(0),
        ));

        test_loop(String::from("test_loop 3"), Duration::from_millis(1500)).await
    });
}

async fn test_loop(text: String, start_duration: Duration) {
    SleepFuture::new(start_duration).await;
    loop {
        println!("3 sec {text}");
        SleepFuture::new(Duration::from_secs(3)).await;
    }
}
