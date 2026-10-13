/*
 *
 * Copyright 2026 gRPC authors.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *
 */

#![allow(missing_docs)]

use bencher::{benchmark_group, benchmark_main, Bencher};
use bytes::{BufMut, Bytes};
use http_body::Body;
use std::{
    hint::black_box,
    pin::{pin, Pin},
    task::{Context, Poll, Waker},
};
use tokio_stream::Stream;
use tonic::{
    codec::{BufferSettings, EncodeBody, EncodeBuf, Encoder},
    Status,
};

struct BytesEncoder(BufferSettings);

impl Encoder for BytesEncoder {
    type Item = Bytes;
    type Error = Status;

    fn encode(&mut self, item: Bytes, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        dst.put_slice(&item);
        Ok(())
    }

    fn buffer_settings(&self) -> BufferSettings {
        self.0
    }
}

// Always-ready input isolates encoding from executor scheduling and cooperative yields.
struct ReadyMessages<I>(I);

impl<I: Iterator<Item = Bytes> + Unpin> Stream for ReadyMessages<I> {
    type Item = Result<Bytes, Status>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.0.next().map(Ok))
    }
}

fn encode_messages(b: &mut Bencher, sizes: &[usize], retained_capacity: usize) {
    let settings =
        BufferSettings::new(8 * 1024, 1).max_encoding_buffer_retained_capacity(retained_capacity);
    let payloads: Vec<_> = sizes
        .iter()
        .map(|&size| Bytes::from(vec![42; size]))
        .collect();
    let source = ReadyMessages(payloads.iter().cloned().cycle());
    let mut body = pin!(EncodeBody::new_client(
        BytesEncoder(settings),
        source,
        None,
        None
    ));
    let mut cx = Context::from_waker(Waker::noop());
    b.bytes = sizes.iter().sum::<usize>() as u64;

    // Keep one RPC alive across iterations to measure allocation reuse. Drop each
    // frame before polling again, as a consumer that has finished sending it would.
    b.iter(|| {
        for _ in sizes {
            let Poll::Ready(Some(Ok(frame))) = body.as_mut().poll_frame(&mut cx) else {
                panic!("ready source should yield an encoded frame");
            };
            black_box(frame.into_data().unwrap());
        }
    });
}

fn small_unbounded(b: &mut Bencher) {
    encode_messages(b, &[256], usize::MAX);
}

fn small_bounded(b: &mut Bencher) {
    encode_messages(b, &[256], 64 * 1024);
}

fn large_unbounded(b: &mut Bencher) {
    encode_messages(b, &[1024 * 1024], usize::MAX);
}

fn large_bounded(b: &mut Bencher) {
    encode_messages(b, &[1024 * 1024], 64 * 1024);
}

fn mixed_unbounded(b: &mut Bencher) {
    let mut sizes = [256; 64];
    sizes[0] = 1024 * 1024;
    encode_messages(b, &sizes, usize::MAX);
}

fn mixed_bounded(b: &mut Bencher) {
    let mut sizes = [256; 64];
    sizes[0] = 1024 * 1024;
    encode_messages(b, &sizes, 64 * 1024);
}

benchmark_group!(
    retention,
    small_unbounded,
    small_bounded,
    large_unbounded,
    large_bounded,
    mixed_unbounded,
    mixed_bounded
);
benchmark_main!(retention);
