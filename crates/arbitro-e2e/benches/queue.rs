use arbitro_client::Client;
use arbitro_proto::config::StreamConfig;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server_addr = "127.0.0.1:9933";
    let stream_name = "queue_test";
    let group_name = "queue_group";

    println!("--- QUEUE BALANCING TEST (Orchestrated) ---");

    // 1. Start Server
    println!("  [Manager] Starting server on {}...", server_addr);
    let mut server_proc = Command::new("./arbitro-server")
        .env("ARBITRO_LISTEN", server_addr)
        .env("ARBITRO_LOG", "info")
        .spawn()
        .expect("Failed to spawn server");

    // Wait for server to be ready
    thread::sleep(Duration::from_secs(2));

    // 2. Setup Stream (Manager only initializes infrastructure)
    println!("  [Manager] Connecting to setup stream...");
    let client = Client::connect(server_addr).await?;
    let scfg = StreamConfig::new(stream_name.as_bytes(), b">").build();
    client.create_stream(&scfg).await?;
    println!("  [Manager] Stream '{}' created.", stream_name);

    // 3. Spawn 2 Workers (Both using the same group name)
    println!(
        "  [Manager] Spawning 2 workers joining group '{}'...",
        group_name
    );
    let mut w1 = Command::new("./endurance_client")
        .env("ARBITRO_ADDR", server_addr)
        .env("ARBITRO_ROLE", "consumer")
        .env("ARBITRO_STREAM", stream_name)
        .env("ARBITRO_GROUP", group_name)
        .env("ARBITRO_MODE", "queue")
        .env("ARBITRO_DURATION", "10")
        .stdout(Stdio::piped())
        .spawn()?;

    let mut w2 = Command::new("./endurance_client")
        .env("ARBITRO_ADDR", server_addr)
        .env("ARBITRO_ROLE", "consumer")
        .env("ARBITRO_STREAM", stream_name)
        .env("ARBITRO_GROUP", group_name)
        .env("ARBITRO_MODE", "queue")
        .env("ARBITRO_DURATION", "10")
        .stdout(Stdio::piped())
        .spawn()?;

    // Wait for workers to connect and join the same consumer group
    thread::sleep(Duration::from_secs(2));

    // 4. Produce 10,000 messages
    println!("  [Manager] Producing 10,000 messages...");
    let subject = b"queue.work";
    for i in 0..10000 {
        let payload = format!("msg-{}", i);
        client
            .publish(stream_name.as_bytes(), subject, payload.as_bytes())
            .await?;
    }
    println!("  [Manager] Production finished.");

    // 5. Wait and Collect
    println!("  [Manager] Waiting for workers to finish (12s)...");
    thread::sleep(Duration::from_secs(12));

    let _ = w1.kill();
    let _ = w2.kill();
    let _ = server_proc.kill();

    let out1 = w1.wait_with_output()?;
    let out2 = w2.wait_with_output()?;

    let s1 = String::from_utf8_lossy(&out1.stdout);
    let s2 = String::from_utf8_lossy(&out2.stdout);

    let count1 = parse_count(&s1);
    let count2 = parse_count(&s2);

    println!("\n--- FINAL RESULTS ---");
    println!("  Worker 1: {} msgs", count1);
    println!("  Worker 2: {} msgs", count2);
    println!("  Total:    {}", count1 + count2);

    if count1 > 0 && count2 > 0 && (count1 + count2) == 10000 {
        println!("SUCCESS: Load balancing verified!");
    } else {
        println!("FAILURE: Distribution unbalanced or incomplete.");
        println!(
            "\nDEBUG: Worker 1 Output (last 5 lines):\n{}",
            last_lines(&s1, 5)
        );
        println!(
            "\nDEBUG: Worker 2 Output (last 5 lines):\n{}",
            last_lines(&s2, 5)
        );
    }

    Ok(())
}

fn last_lines(s: &str, n: usize) -> String {
    s.lines()
        .rev()
        .take(n)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
        .join("\n")
}

fn parse_count(output: &str) -> u64 {
    for line in output.lines().rev() {
        if line.contains("Total Acked:") {
            if let Some(pos) = line.find("Total Acked:") {
                let part = &line[pos + "Total Acked:".len()..].trim();
                if let Ok(val) = part.parse::<u64>() {
                    return val;
                }
            }
        }
    }
    0
}
