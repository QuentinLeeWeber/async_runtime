use art::{mutex::Mutex, thread, time::sleep};
use std::{sync::Arc, time::Duration};

#[art::main(thread_count = 2)]
async fn main() {
    let data: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));

    thread::spawn(thread_a()).await;

    thread::spawn({
        let data = Arc::clone(&data);
        async move {
            sleep(Duration::from_secs(1)).await;
            loop {
                {
                    let mut data = data.lock().await;
                    println!("test loop 2 | data: {}", *data);
                    *data += 1;
                }
                sleep(Duration::from_millis(2000)).await;
            }
        }
    });

    loop {
        {
            let mut data = data.lock().await;
            println!("test loop 1 | data: {}", *data);
            *data += 1;
        }
        sleep(Duration::from_millis(2000)).await;
    }
}

async fn thread_a() {
    println!("A");
    let c = thread::spawn(thread_c());
    let b = thread::spawn(thread_b());
    c.await;
    b.await;
    thread::spawn(thread_d()).await;
}

async fn thread_b() {
    println!("B");
}

async fn thread_c() {
    sleep(Duration::from_millis(500)).await;
    println!("C");
}

async fn thread_d() {
    println!("D");
}
