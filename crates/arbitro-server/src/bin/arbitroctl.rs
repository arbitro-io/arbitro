//! §7.2 — arbitroctl: minimal admin CLI for the Arbitro broker.
//!
//! Hand-rolled argument parsing (no `clap`). Each subcommand maps to a
//! single client call and prints a one-line summary on stdout. Exits 0
//! on success, 1 on error with a clear message on stderr.
//!
//! Usage:
//!   arbitroctl list-streams
//!   arbitroctl list-consumers [--stream NAME]
//!   arbitroctl create-stream NAME [--max-msgs N] [--max-bytes B] [--max-age-secs S]
//!   arbitroctl delete-stream NAME
//!   arbitroctl purge-stream NAME
//!   arbitroctl drain-subject STREAM SUBJECT
//!   arbitroctl consumer-pending STREAM CONSUMER_NAME
//!
//! `ARBITRO_ADDR` controls broker target (default `127.0.0.1:9898`).

use std::process::ExitCode;

use arbitro_client_tokio::{Client, ClientConfig};
use bytes::Bytes;

fn usage() -> &'static str {
    "usage: arbitroctl <command> [args]\n\
     commands:\n\
       list-streams\n\
       list-consumers [--stream NAME]\n\
       create-stream NAME [--max-msgs N] [--max-bytes B] [--max-age-secs S]\n\
       delete-stream NAME\n\
       purge-stream NAME\n\
       drain-subject STREAM SUBJECT\n\
       consumer-pending STREAM CONSUMER_NAME\n\
     env:\n\
       ARBITRO_ADDR (default 127.0.0.1:9898)"
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!("{}", usage());
        return ExitCode::from(1);
    }

    let rt = match tokio::runtime::Runtime::new() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: failed to build runtime: {e}");
            return ExitCode::from(1);
        }
    };

    rt.block_on(async move {
        match run(args).await {
            Ok(()) => ExitCode::from(0),
            Err(e) => {
                eprintln!("error: {e}");
                ExitCode::from(1)
            }
        }
    })
}

async fn run(args: Vec<String>) -> Result<(), String> {
    let addr = std::env::var("ARBITRO_ADDR").unwrap_or_else(|_| "127.0.0.1:9898".to_string());
    let cmd = args[0].as_str();
    let rest = &args[1..];

    match cmd {
        "list-streams" => {
            let client = connect(&addr).await?;
            let resp = client
                .list_streams(0, 1000)
                .await
                .map_err(|e| format!("list-streams failed: {e:?}"))?;
            print_stream_list(&resp);
        }
        "list-consumers" => {
            let stream_name = parse_named_opt(rest, "--stream");
            let client = connect(&addr).await?;
            if let Some(name) = stream_name {
                let stream_id = resolve_stream_id(&client, name.as_bytes()).await?;
                let resp = client
                    .list_consumers(stream_id, 0, 1000)
                    .await
                    .map_err(|e| format!("list-consumers failed: {e:?}"))?;
                print_consumer_list(&resp);
            } else {
                // Walk every stream and print one block per stream.
                let resp = client
                    .list_streams(0, 1000)
                    .await
                    .map_err(|e| format!("list-streams failed: {e:?}"))?;
                let names = stream_names(&resp);
                for (sid, name) in names {
                    println!("stream={} ({}):", String::from_utf8_lossy(&name), sid);
                    let r = client
                        .list_consumers(sid, 0, 1000)
                        .await
                        .map_err(|e| format!("list-consumers failed: {e:?}"))?;
                    print_consumer_list(&r);
                }
            }
        }
        "create-stream" => {
            let name = rest.first().ok_or_else(|| "create-stream requires NAME".to_string())?;
            let max_msgs = parse_named_u64(rest, "--max-msgs").unwrap_or(0);
            let max_bytes = parse_named_u64(rest, "--max-bytes").unwrap_or(0);
            let max_age_secs = parse_named_u64(rest, "--max-age-secs").unwrap_or(0);
            let client = connect(&addr).await?;
            let resp = client
                .create_stream(
                    name.as_bytes(),
                    b">",
                    max_msgs, max_bytes, max_age_secs,
                    1, 0, 0, 0, 0,
                )
                .await
                .map_err(|e| format!("create-stream failed: {e:?}"))?;
            let sid = u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32;
            println!("created stream={name} id={sid}");
        }
        "delete-stream" => {
            let name = rest.first().ok_or_else(|| "delete-stream requires NAME".to_string())?;
            let client = connect(&addr).await?;
            client
                .delete_stream(name.as_bytes())
                .await
                .map_err(|e| format!("delete-stream failed: {e:?}"))?;
            println!("deleted stream={name}");
        }
        "purge-stream" => {
            let name = rest.first().ok_or_else(|| "purge-stream requires NAME".to_string())?;
            let client = connect(&addr).await?;
            client
                .purge_stream(name.as_bytes())
                .await
                .map_err(|e| format!("purge-stream failed: {e:?}"))?;
            println!("purged stream={name}");
        }
        "drain-subject" => {
            if rest.len() < 2 {
                return Err("drain-subject requires STREAM SUBJECT".to_string());
            }
            let client = connect(&addr).await?;
            client
                .drain_subject(rest[0].as_bytes(), rest[1].as_bytes())
                .await
                .map_err(|e| format!("drain-subject failed: {e:?}"))?;
            println!("drained stream={} subject={}", rest[0], rest[1]);
        }
        "consumer-pending" => {
            if rest.len() < 2 {
                return Err("consumer-pending requires STREAM CONSUMER_NAME".to_string());
            }
            let client = connect(&addr).await?;
            let stream_id = resolve_stream_id(&client, rest[0].as_bytes()).await?;
            let resp = client
                .get_consumer(stream_id, rest[1].as_bytes())
                .await
                .map_err(|e| format!("get-consumer failed: {e:?}"))?;
            // Body layout: u32 wire_id + … . We only need the consumer id.
            if resp.len() < 4 {
                return Err("malformed get-consumer reply".to_string());
            }
            let consumer_id = u32::from_le_bytes(resp[..4].try_into().unwrap());
            let pending = client
                .get_pending(consumer_id)
                .await
                .map_err(|e| format!("consumer-pending failed: {e:?}"))?;
            println!("consumer={} id={} ack_pending={}", rest[1], consumer_id, pending);
        }
        _ => {
            eprintln!("{}", usage());
            return Err(format!("unknown command: {cmd}"));
        }
    }
    Ok(())
}

async fn connect(addr: &str) -> Result<Client, String> {
    Client::connect(ClientConfig {
        addr: addr.to_string(),
        ..ClientConfig::default()
    })
    .await
    .map_err(|e| format!("connect to {addr} failed: {e:?}"))
}

fn parse_named_opt(args: &[String], flag: &str) -> Option<String> {
    let mut iter = args.iter();
    while let Some(a) = iter.next() {
        if a == flag {
            return iter.next().cloned();
        }
    }
    None
}

fn parse_named_u64(args: &[String], flag: &str) -> Option<u64> {
    parse_named_opt(args, flag).and_then(|v| v.parse().ok())
}

fn print_stream_list(resp: &Bytes) {
    let count = u32::from_le_bytes(resp[..4].try_into().unwrap()) as usize;
    let mut pos = 4usize;
    println!("streams ({count}):");
    for _ in 0..count {
        let wire_id = u32::from_le_bytes(resp[pos..pos + 4].try_into().unwrap());
        pos += 4;
        let name_len = u16::from_le_bytes(resp[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        let name = &resp[pos..pos + name_len];
        pos += name_len;
        println!("  {wire_id:>6}  {}", String::from_utf8_lossy(name));
    }
}

fn stream_names(resp: &Bytes) -> Vec<(u32, Vec<u8>)> {
    let count = u32::from_le_bytes(resp[..4].try_into().unwrap()) as usize;
    let mut pos = 4usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let wire_id = u32::from_le_bytes(resp[pos..pos + 4].try_into().unwrap());
        pos += 4;
        let name_len = u16::from_le_bytes(resp[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        let name = resp[pos..pos + name_len].to_vec();
        pos += name_len;
        out.push((wire_id, name));
    }
    out
}

fn print_consumer_list(resp: &Bytes) {
    let count = u32::from_le_bytes(resp[..4].try_into().unwrap()) as usize;
    println!("  consumers ({count})");
    // Body layout follows the same wire pattern (id + name); we don't
    // attempt to decode every field here — keeps arbitroctl simple.
}

async fn resolve_stream_id(client: &Client, name: &[u8]) -> Result<u32, String> {
    let resp = client
        .list_streams(0, 1000)
        .await
        .map_err(|e| format!("list-streams failed: {e:?}"))?;
    let names = stream_names(&resp);
    for (sid, n) in names {
        if n == name {
            return Ok(sid);
        }
    }
    Err(format!("stream not found: {}", String::from_utf8_lossy(name)))
}
