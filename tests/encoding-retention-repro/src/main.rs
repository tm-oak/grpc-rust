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

use bytes::{BufMut, Bytes};
use http_body::Body;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    pin::pin,
    sync::atomic::{AtomicUsize, Ordering::Relaxed},
    task::{Context, Poll, Waker},
};
use tokio_stream::StreamExt;
use tonic::{
    codec::{BufferSettings, CompressionEncoding, EncodeBody, EncodeBuf, Encoder},
    Status,
};

struct CountingAllocator;
static LIVE: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            LIVE.fetch_add(layout.size(), Relaxed);
        }
        ptr
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            LIVE.fetch_add(layout.size(), Relaxed);
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe {
            System.dealloc(ptr, layout);
        }
        LIVE.fetch_sub(layout.size(), Relaxed);
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let next = unsafe { System.realloc(ptr, layout, size) };
        if !next.is_null() {
            LIVE.fetch_add(size, Relaxed);
            LIVE.fetch_sub(layout.size(), Relaxed);
        }
        next
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

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

fn retained(cap: usize, compression: Option<CompressionEncoding>) -> usize {
    let payloads = [
        Bytes::from(vec![42; 1024 * 1024]),
        Bytes::from_static(b"hello"),
    ];
    // Exclude input Bytes sharing metadata from the encoder allocation count.
    drop(payloads[0].clone());
    let source =
        tokio_stream::iter(payloads.iter().cloned().map(Ok)).chain(tokio_stream::pending());
    let before = LIVE.load(Relaxed);
    let result;
    {
        let settings = BufferSettings::new(8192, 1).max_encoding_buffer_retained_capacity(cap);
        let mut body = pin!(EncodeBody::new_client(
            BytesEncoder(settings),
            source,
            compression,
            None
        ));
        let mut cx = Context::from_waker(Waker::noop());
        for _ in &payloads {
            let Poll::Ready(Some(Ok(frame))) = body.as_mut().poll_frame(&mut cx) else {
                panic!("expected frame");
            };
            drop(frame);
        }
        assert!(body.as_mut().poll_frame(&mut cx).is_pending());
        result = LIVE.load(Relaxed) - before;
    }
    assert_eq!(
        LIVE.load(Relaxed),
        before,
        "dropping the RPC must release its allocations"
    );
    result
}

fn main() {
    println!("retained requested allocation bytes after 1 MiB, 5 bytes, then idle; input excluded");
    for encoding in [None, Some(CompressionEncoding::Gzip)] {
        for cap in [usize::MAX, 65536, 0] {
            let bytes = retained(cap, encoding);
            println!("compression={encoding:?}, limit={cap}, retained_bytes={bytes}");
        }
    }
}
