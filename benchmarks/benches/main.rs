use art::{mutex::Mutex, thread};
use criterion::{Criterion, criterion_group, criterion_main};
use std::{hint::black_box, sync::Arc};

fn fibonacci_benchmark(c: &mut Criterion) {
    fn fibonacci(n: u64) -> u64 {
        match n {
            0 => 1,
            1 => 1,
            n => fibonacci(n - 1) + fibonacci(n - 2),
        }
    }

    #[art::block_on]
    async fn fibonacci_with_runtime(n: u64) -> u64 {
        fibonacci(n)
    }

    c.bench_function("fib 5 without runtime", |b| {
        b.iter(|| fibonacci(black_box(5)))
    });

    c.bench_function("fib 5 with runtime", |b| {
        b.iter(|| fibonacci_with_runtime(black_box(5)))
    });

    c.bench_function("fib 25 without runtime", |b| {
        b.iter(|| fibonacci(black_box(25)))
    });

    c.bench_function("fib 25 with runtime", |b| {
        b.iter(|| fibonacci_with_runtime(black_box(25)))
    });
}

fn mutex_benchmark(c: &mut Criterion) {
    async fn mutex() {
        let data = Arc::new(Mutex::new(0));
        let handle = thread::spawn({
            let data = Arc::clone(&data);
            async move {
                for _ in 0..1000 {
                    let mut guard = data.lock().await;
                    *guard += 1;
                }
            }
        });

        for _ in 0..1000 {
            let mut guard = data.lock().await;
            *guard += 1;
        }

        handle.await;
    }

    #[art::block_on]
    async fn mutex_single_thread() {
        mutex().await
    }

    #[art::block_on(thread_count = 2)]
    async fn mutex_multi_thread() {
        mutex().await
    }

    c.bench_function("mutex single thread", |b| b.iter(|| mutex_single_thread()));
    c.bench_function("mutex multi thread", |b| b.iter(|| mutex_multi_thread()));
}

fn heavy_task_benchmark(c: &mut Criterion) {
    fn tribonacci(n: u32) -> u64 {
        match n {
            0 => 0,
            1 | 2 => 1,
            n => tribonacci(n - 1) + tribonacci(n - 2) + tribonacci(n - 3),
        }
    }

    fn heavy_task() {
        tribonacci(black_box(25));
    }

    #[art::block_on]
    async fn heavy_task_single_threaded() {
        for _ in 0..8 {
            heavy_task();
        }
    }

    #[art::block_on]
    async fn heavy_task_single_threaded_with_spawn() {
        let handles = (0..8).map(|_| {
            thread::spawn(async {
                heavy_task();
            })
        });

        for handle in handles {
            handle.await;
        }
    }

    #[art::block_on(thread_count = 8)]
    async fn heavy_task_8_multi_threaded() {
        let handles = (0..8).map(|_| {
            thread::spawn(async {
                heavy_task();
            })
        });

        for handle in handles {
            handle.await;
        }
    }

    c.bench_function("heavy task single threaded", |b| {
        b.iter(|| heavy_task_single_threaded())
    });

    c.bench_function("heavy task single threaded with spawn", |b| {
        b.iter(|| heavy_task_single_threaded_with_spawn())
    });

    c.bench_function("heavy task 8 multi threaded", |b| {
        b.iter(|| heavy_task_8_multi_threaded())
    });
}

criterion_group!(
    benches,
    fibonacci_benchmark,
    mutex_benchmark,
    heavy_task_benchmark
);

criterion_main!(benches);
