use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

fn criterion_benchmark(c: &mut Criterion) {
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

fn fibonacci(n: u64) -> u64 {
    match n {
        0 => 1,
        1 => 1,
        n => fibonacci(n - 1) + fibonacci(n - 2),
    }
}

#[async_runtime::block_on]
async fn fibonacci_with_runtime(n: u64) -> u64 {
    fibonacci(n)
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
