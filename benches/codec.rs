use criterion::{Criterion, criterion_group, criterion_main};

fn frame_encoding(c: &mut Criterion) {
    c.bench_function("frame encoding 1KiB", |b| {
        let payload = vec![0_u8; 1024];
        b.iter(|| std::hint::black_box(&payload).len().to_be_bytes());
    });
}

criterion_group!(benches, frame_encoding);
criterion_main!(benches);
