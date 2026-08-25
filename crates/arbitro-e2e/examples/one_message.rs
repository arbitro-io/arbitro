//! How long does ONE message take to come out?
//!
//! Not throughput. One message published, one message received, timed end
//! to end, over and over — so the number is the path a single message
//! walks, with nothing batched, pipelined or amortised behind it.
//!
//! Measured from just before `publish` to the moment `recv` returns, so it
//! includes the client's own write, the socket both ways, the store append,
//! the match and the delivery write. A throughput bench hides all of that
//! behind a batch; this cannot.

use std::time::Instant;

use arbitro_client_tokio::{
    AckPolicy, Client, ClientConfig, ConsumerBuilder, DeliverMode, DeliverPolicy, StreamBuilder,
};
use arbitro_server::{ArbitroServer, Config};
use bytes::Bytes;

const WARMUP: usize = 200;
const SAMPLES: usize = 2000;

#[tokio::main]
async fn main() {
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let addr = format!("127.0.0.1:{port}");

    let server = ArbitroServer::new(Config::default().listen_addr(addr.clone()));
    tokio::spawn(async move {
        let _ = server.run().await;
    });

    let client = loop {
        if let Ok(c) = Client::connect(ClientConfig {
            addr: addr.clone(),
            ..ClientConfig::default()
        })
        .await
        {
            break c;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    };

    let name = b"latency".as_slice();
    StreamBuilder::new(name)
        .filter(b"latency.>")
        .upsert(&client)
        .await
        .expect("upsert");
    let info = client.get_stream(name).await.expect("get_stream");
    let stream_id = u64::from_le_bytes(info[..8].try_into().unwrap()) as u32;

    let consumer_id = ConsumerBuilder::new(b"lat")
        .group(b"lat")
        .deliver_policy(DeliverPolicy::All)
        .deliver_mode(DeliverMode::Fanout)
        .ack_policy(AckPolicy::None)
        .create(&client, stream_id)
        .await
        .expect("create consumer");

    let mut handle = client
        .subscribe(stream_id, consumer_id, b"")
        .await
        .expect("subscribe");

    let payload = Bytes::from_static(&[0u8; 64]);
    let subject = b"latency.msg".as_slice();

    let mut us: Vec<f64> = Vec::with_capacity(SAMPLES);
    for i in 0..(WARMUP + SAMPLES) {
        let t0 = Instant::now();
        client
            .publish(stream_id, subject, payload.clone())
            .expect("publish");
        if handle.recv().await.is_none() {
            eprintln!("stream closed at {i}");
            break;
        }
        if i >= WARMUP {
            us.push(t0.elapsed().as_nanos() as f64 / 1000.0);
        }
    }

    us.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let at = |p: f64| us[((us.len() - 1) as f64 * p) as usize];
    let mean = us.iter().sum::<f64>() / us.len() as f64;

    println!("\npublish → receive, one message at a time ({} samples)\n", us.len());
    println!("  min   {:>8.1} µs", us[0]);
    println!("  p50   {:>8.1} µs", at(0.50));
    println!("  p90   {:>8.1} µs", at(0.90));
    println!("  p99   {:>8.1} µs", at(0.99));
    println!("  max   {:>8.1} µs", us[us.len() - 1]);
    println!("  mean  {:>8.1} µs", mean);
}
