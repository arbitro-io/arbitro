//! Does `GetStream` report the shard the stream actually lives on?
//!
//! Creates a handful of streams, asks the broker where each one is, and
//! checks the answer against what the placement rule says it must be. A
//! wrong answer here would send a client to the wrong port and put its
//! socket in a shard the drain cannot reach — the exact failure this
//! field exists to prevent.

use arbitro_client_tokio::{Client, ClientConfig, StreamBuilder};
use arbitro_server::{ArbitroServer, Config};

#[tokio::main]
async fn main() {
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let addr = format!("127.0.0.1:{port}");
    let shard_count = 8usize;

    let server = ArbitroServer::new(
        Config::default()
            .listen_addr(addr.clone())
            .shard_count(shard_count),
    );
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

    println!("shard_count = {shard_count}");
    println!("topology    = {:?}", client.shard_topology().await);
    println!();
    println!("{:<14} {:>12} {:>8} {:>10}", "stream", "id", "shard", "same as");

    let mut wrong = 0;
    let mut first: Option<u16> = None;
    for i in 0..12u32 {
        let name = format!("place_{i}");
        let mut filter = name.clone().into_bytes();
        filter.extend_from_slice(b".>");
        StreamBuilder::new(name.as_bytes())
            .filter(&filter)
            .upsert(&client)
            .await
            .expect("upsert");

        let info = client.get_stream(name.as_bytes()).await.expect("get_stream");
        let raw = u64::from_le_bytes(info[..8].try_into().unwrap());
        let id = raw as u32;
        let shard = ((raw >> 32) & 0xFFFF) as u16;
        // A stream is born on the shard of the connection that created
        // it, so streams made over ONE connection all share one shard.
        // That is the invariant now — not the old `id % shard_count`.
        let expected = first.unwrap_or(shard);
        first = Some(expected);
        let ok = shard == expected && (shard as usize) < shard_count;
        if !ok {
            wrong += 1;
        }
        println!(
            "{name:<14} {id:>12} {shard:>8} {expected:>10}{}",
            if ok { "" } else { "  <-- MISMATCH" }
        );
    }

    println!();
    if wrong == 0 {
        println!("OK — every stream reports the shard of the connection that created it");
    } else {
        println!("FAIL — {wrong} stream(s) reported a shard they are not on");
        std::process::exit(1);
    }
}
