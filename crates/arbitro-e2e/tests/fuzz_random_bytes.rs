//! T20 — random bytes after HELLO must never abort the broker.
//!
//! A live socket sends a valid v2 magic + HELLO, then 1000 frames of
//! pure random bytes. The broker must close the connection at most
//! (per the v2 contract: "malformed frame → drop connection"), and
//! the SERVER process must stay alive serving other clients.
//!
//! Without B2/B3/B4 the broker panicked on the first crafted frame
//! and tore down every other connected client. With them, the
//! adversarial socket is dropped silently and a sibling client keeps
//! publishing.

mod test_helper;

use std::time::Duration;

use arbitro_proto::v2::magic::ARBITRO_MAGIC_V2;
use bytes::Bytes;
use test_helper::TestServerBuilder;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

#[tokio::test(flavor = "multi_thread")]
async fn t20_random_bytes_after_hello_never_abort_broker() {
    let server = TestServerBuilder::new().spawn().await;

    // Sibling client establishes a clean connection BEFORE the fuzzer
    // hits the server. We use it to confirm the broker is still alive
    // after the fuzzer is done.
    let sibling = server.connect().await;
    let resp = sibling
        .create_stream(b"alive_check", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .expect("sibling create_stream pre-fuzz");
    let alive_stream_id = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;

    // Fuzz socket — raw TCP, no client API.
    let mut sock = TcpStream::connect(&server.addr).await.expect("fuzz connect");

    // Send the v2 HELLO frame (8 B = magic + 4 trailing zeros).
    let mut hello = Vec::with_capacity(8);
    hello.extend_from_slice(&ARBITRO_MAGIC_V2.to_le_bytes());
    hello.extend_from_slice(&[0u8; 4]);
    sock.write_all(&hello).await.expect("write HELLO");

    // Linear-congruential RNG seeded deterministically so a failure
    // is reproducible without bringing in a real RNG dep.
    let mut state: u64 = 0xC011_1510_DEAD_BEEFu64;
    let mut rand_byte = || -> u8 {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (state >> 56) as u8
    };

    // Pour 1000 frames of pure noise. Each frame: 16-byte header bytes
    // + random msg_len-derived body. We deliberately let msg_len go up
    // to ~4 KB so the dispatcher walks malicious lengths into its
    // bounds checks (B2/B3/B4).
    for _ in 0..1000 {
        let frame_size: usize = 16 + (rand_byte() as usize * 16);
        let mut buf = vec![0u8; frame_size];
        for byte in buf.iter_mut() {
            *byte = rand_byte();
        }
        // Stamp a valid msg_len so the broker tries to parse the body.
        let body_len = (frame_size - 16) as u32;
        buf[4..8].copy_from_slice(&body_len.to_le_bytes());
        // The write may fail mid-burst because the broker dropped us.
        // That is the expected outcome; just bail out of the loop —
        // we only care that the BROKER stayed up.
        if sock.write_all(&buf).await.is_err() {
            break;
        }
    }
    // Best-effort flush + shutdown — broker likely already closed.
    let _ = sock.shutdown().await;
    drop(sock);

    // Give the broker a beat to finish processing whatever survived
    // the rude socket closure.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // The proof of survival: sibling can still publish + the broker
    // can still serve management calls. If the broker had panicked,
    // this would hang or error.
    sibling
        .publish_sync(alive_stream_id, b"alive_check.ping", Bytes::from_static(b"OK"))
        .await
        .expect("broker must still accept publishes from sibling after fuzz");

    let streams = sibling
        .list_streams(0, 100)
        .await
        .expect("broker must still answer list_streams after fuzz");
    assert!(
        !streams.is_empty(),
        "list_streams reply must not be empty — broker is alive and answering",
    );
}
