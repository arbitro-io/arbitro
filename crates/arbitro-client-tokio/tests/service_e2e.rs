//! Service RPC E2E tests.
//!
//! Tests the full request/reply lifecycle through a real ArbitroServer:
//! - basic round-trip
//! - timeout
//! - multiple handlers + concurrent requests
//! - cross-connection (two clients)
//! - requester disconnect
//! - service crash → requester timeout

use std::time::Duration;

use arbitro_client_tokio::{Client, ClientConfig};
use arbitro_server::{ArbitroServer, Config};
use bytes::Bytes;

// ── helpers ─────────────────────────────────────────────────────────────────

fn portpicker() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

async fn start_server() -> String {
    let port = portpicker();
    let addr = format!("127.0.0.1:{port}");
    let cfg = Config::default()
        .listen_addr(addr.clone())
        .max_connections(64);
    tokio::spawn(async move {
        let _ = ArbitroServer::new(cfg).run().await;
    });
    tokio::time::sleep(Duration::from_millis(80)).await;
    addr
}

async fn connect(addr: &str) -> Client {
    let cfg = ClientConfig {
        addr: addr.to_string(),
        ..ClientConfig::default()
    };
    Client::connect(cfg).await.expect("connect")
}

// ── tests ───────────────────────────────────────────────────────────────────

/// #172: Basic request/reply round-trip.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn service_basic_request_reply() {
    let addr = start_server().await;
    let client = connect(&addr).await;

    // Build a service that echoes back the payload prefixed with "re:"
    let svc = client.service("echo").build().await.expect("service build");
    svc.handle(b"ping", |msg| async move {
        let mut reply = b"re:".to_vec();
        reply.extend_from_slice(msg.data());
        let _ = msg.reply(&reply);
        msg.ack();
    });

    // Give dispatch loop time to register
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Make a request from the same service (it has its own inbox)
    let resp = svc
        .request("echo", b"ping", Bytes::from_static(b"hello"), 5000)
        .await
        .expect("request");

    assert_eq!(&resp[..], b"re:hello");
    svc.close();
}

/// #173: Request timeout returns error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn service_request_timeout() {
    let addr = start_server().await;
    let client = connect(&addr).await;

    // Service "slow" with a handler that never replies
    let svc = client.service("slow").build().await.expect("service build");
    svc.handle(b"noreply", |msg| async move {
        // Deliberately do NOT reply — just ack to clear the message
        msg.ack();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Requester service
    let requester = client
        .service("slow-requester")
        .build()
        .await
        .expect("requester build");

    let result = requester
        .request("slow", b"noreply", Bytes::from_static(b"data"), 200)
        .await;

    assert!(result.is_err());
    match result.unwrap_err() {
        arbitro_client_tokio::ClientError::Timeout => {}
        other => panic!("expected Timeout, got: {:?}", other),
    }

    svc.close();
    requester.close();
}

/// #174: Service with multiple handlers, concurrent requests.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn service_multiple_handlers_concurrent() {
    let addr = start_server().await;
    let client = connect(&addr).await;

    let svc = client.service("multi").build().await.expect("service build");

    // Handler: "add" — parse two u32s from payload, return sum
    svc.handle(b"add", |msg| async move {
        let data = msg.data();
        if data.len() == 8 {
            let a = u32::from_le_bytes(data[..4].try_into().unwrap());
            let b = u32::from_le_bytes(data[4..8].try_into().unwrap());
            let sum = (a + b).to_le_bytes();
            let _ = msg.reply(&sum);
        }
        msg.ack();
    });

    // Handler: "upper" — uppercase ASCII payload
    svc.handle(b"upper", |msg| async move {
        let upper: Vec<u8> = msg.data().iter().map(|b| b.to_ascii_uppercase()).collect();
        let _ = msg.reply(&upper);
        msg.ack();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send concurrent requests
    let (r1, r2, r3) = tokio::join!(
        svc.request(
            "multi",
            b"add",
            Bytes::from([3u32.to_le_bytes(), 7u32.to_le_bytes()].concat()),
            5000
        ),
        svc.request(
            "multi",
            b"upper",
            Bytes::from_static(b"hello"),
            5000
        ),
        svc.request(
            "multi",
            b"add",
            Bytes::from([100u32.to_le_bytes(), 200u32.to_le_bytes()].concat()),
            5000
        ),
    );

    let sum1 = u32::from_le_bytes(r1.unwrap()[..4].try_into().unwrap());
    assert_eq!(sum1, 10);

    assert_eq!(&r2.unwrap()[..], b"HELLO");

    let sum2 = u32::from_le_bytes(r3.unwrap()[..4].try_into().unwrap());
    assert_eq!(sum2, 300);

    svc.close();
}

/// #175: Service across different connections (two clients).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn service_cross_connection() {
    let addr = start_server().await;

    // Client A hosts the "processor" service
    let client_a = connect(&addr).await;
    let processor = client_a
        .service("processor")
        .build()
        .await
        .expect("processor build");
    processor.handle(b"double", |msg| async move {
        let val = u32::from_le_bytes(msg.data()[..4].try_into().unwrap_or([0; 4]));
        let result = (val * 2).to_le_bytes();
        let _ = msg.reply(&result);
        msg.ack();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Client B makes a request to "processor" from its own service
    let client_b = connect(&addr).await;
    let requester = client_b
        .service("caller")
        .build()
        .await
        .expect("caller build");

    let resp = requester
        .request(
            "processor",
            b"double",
            Bytes::from(42u32.to_le_bytes().to_vec()),
            5000,
        )
        .await
        .expect("cross-connection request");

    let result = u32::from_le_bytes(resp[..4].try_into().unwrap());
    assert_eq!(result, 84);

    processor.close();
    requester.close();
}

/// #176: Requester disconnects mid-request, service handles gracefully.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn service_requester_disconnect_graceful() {
    let addr = start_server().await;
    let client = connect(&addr).await;

    // Service that takes 500ms to respond
    let svc = client.service("delayed").build().await.expect("service build");
    svc.handle(b"slow", |msg| async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let _ = msg.reply(b"done");
        msg.ack();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Requester on a separate connection — we'll drop it mid-request
    {
        let requester_client = connect(&addr).await;
        let requester = requester_client
            .service("dropper")
            .build()
            .await
            .expect("requester build");

        // Start request with a very short timeout to simulate "disconnect"
        // before the service can reply (service takes 500ms, we timeout at 100ms)
        let _ = requester
            .request("delayed", b"slow", Bytes::from_static(b"x"), 100)
            .await;

        // Drop requester and its connection
        requester.close();
    }

    // The service should NOT crash — give it time to finish processing
    tokio::time::sleep(Duration::from_millis(600)).await;

    // Verify service still works by making a fresh request
    let fresh_requester = connect(&addr).await;
    let fresh_svc = fresh_requester
        .service("fresh-caller")
        .build()
        .await
        .expect("fresh build");
    let resp = fresh_svc
        .request("delayed", b"slow", Bytes::from_static(b"y"), 2000)
        .await
        .expect("fresh request after disconnect");
    assert_eq!(&resp[..], b"done");

    svc.close();
    fresh_svc.close();
}

/// #177: Service process crashes, requester gets timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn service_crash_requester_timeout() {
    let addr = start_server().await;

    // Service on client A
    let client_a = connect(&addr).await;
    let svc = client_a
        .service("crashable")
        .build()
        .await
        .expect("crashable build");
    svc.handle(b"work", |msg| async move {
        let _ = msg.reply(b"ok");
        msg.ack();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Requester on client B
    let client_b = connect(&addr).await;
    let requester = client_b
        .service("crash-caller")
        .build()
        .await
        .expect("crash-caller build");

    // Verify it works first
    let resp = requester
        .request("crashable", b"work", Bytes::from_static(b"test"), 2000)
        .await
        .expect("initial request");
    assert_eq!(&resp[..], b"ok");

    // "Crash" the service by closing it and dropping the client
    svc.close();
    drop(client_a);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Next request should timeout (no one to reply)
    let result = requester
        .request("crashable", b"work", Bytes::from_static(b"test2"), 500)
        .await;

    assert!(result.is_err());
    match result.unwrap_err() {
        arbitro_client_tokio::ClientError::Timeout => {}
        other => panic!("expected Timeout, got: {:?}", other),
    }

    requester.close();
}
