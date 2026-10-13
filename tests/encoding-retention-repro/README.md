# Encoding buffer retention reproducer

This branch adds standalone validation tooling on top of the buffer-retention
change. It is intended as a PR reference, separately from the upstream patch.

## Retained memory

From the repository root, run (Rust 1.85 or newer):

```sh
cargo run --release --manifest-path tests/encoding-retention-repro/Cargo.toml
```

The probe keeps one outgoing RPC open, sends a 1 MiB message followed by a
5-byte message, drops both outgoing frames, and leaves the stream idle. It
compares unbounded retention (the unchanged default) with a 64 KiB limit and
zero retention, with and without gzip compression.

A counting allocator measures live requested allocation bytes. Input payloads
and their sharing metadata are excluded. The probe also asserts that dropping
the RPC releases the measured allocations. This measures live heap allocations,
not RSS or allocator size-class overhead; it does not use a network transport.
Both configurations run the patched code.

One local run on macOS (arm64) produced the following retained byte counts;
exact counts depend on dependency versions and platform:

| Compression | Unbounded | 64 KiB limit | Zero retention |
| --- | ---: | ---: | ---: |
| None | 1,048,621 | 8,232 | 0 |
| Gzip | 2,105,389 | 24,616 | 0 |

## Throughput

```sh
cargo bench -p tonic --bench encode
```

The benchmark compares unbounded retention with a 64 KiB limit for 256-byte
messages, 1 MiB messages, and a repeating sequence of one 1 MiB message followed
by 63 256-byte messages. Each case keeps one RPC alive across iterations and
drops each frame before polling again. Initial buffers are 8 KiB and the yield
threshold is 1 byte. Compression, transport, and executor scheduling are excluded.

Benchmark timings depend on the platform and allocator. The memory probe and
throughput benchmark answer separate questions: how much storage remains owned
by an idle stream, and what releasing oversized buffers costs during encoding.
