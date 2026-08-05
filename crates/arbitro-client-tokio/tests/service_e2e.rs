//! Service RPC E2E tests.
//!
//! Tests the full request/reply lifecycle through a real ArbitroServer:
//! - basic round-trip
//! - timeout
//! - multiple handlers + concurrent requests
//! - cross-connection (two clients)
//! - requester disconnect
//! - service crash → requester timeout
//! - two instances SHARE requests (queue-grouped worker consumer)

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
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
    svc.handle(b"ping", |req| async move {
        let mut reply = b"re:".to_vec();
        reply.extend_from_slice(req.data());
        Ok(reply)
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
    svc.handle(b"noreply", |_req| async move {
        // Deliberately return empty — the framework acks without replying,
        // so the requester will time out waiting for a reply.
        Ok(vec![])
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
        other => panic!("expected Timeout, got: {other:?}"),
    }

    svc.close();
    requester.close();
}

/// #174: Service with multiple handlers, concurrent requests.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn service_multiple_handlers_concurrent() {
    let addr = start_server().await;
    let client = connect(&addr).await;

    let svc = client
        .service("multi")
        .build()
        .await
        .expect("service build");

    // Handler: "add" — parse two u32s from payload, return sum
    svc.handle(b"add", |req| async move {
        let data = req.data();
        if data.len() != 8 {
            return Ok(vec![]);
        }
        let a = u32::from_le_bytes(data[..4].try_into().unwrap());
        let b = u32::from_le_bytes(data[4..8].try_into().unwrap());
        Ok((a + b).to_le_bytes().to_vec())
    });

    // Handler: "upper" — uppercase ASCII payload
    svc.handle(b"upper", |req| async move {
        Ok(req.data().iter().map(|b| b.to_ascii_uppercase()).collect())
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
        svc.request("multi", b"upper", Bytes::from_static(b"hello"), 5000),
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

/// Two instances of the SAME service must SHARE the request load, not
/// each receive every request.
///
/// Regression test for two independent defects in `ServiceBuilder::build`,
/// each of which alone breaks multi-instance services:
///
///   1. `deliver_mode` was hardcoded to 0 (Fanout) on the worker consumer.
///      The broker treats that byte as the sole determinant of fan-out and
///      discards the group under Fanout, so every instance ran every
///      handler and published every reply — `a + b == 2 * N`.
///   2. The worker consumer NAME was service-wide, so every instance
///      resolved to the same consumer id and therefore the same
///      subscription id. The broker's binding table is keyed by
///      subscription id, so each new instance's `subscribe` retired the
///      previous one's binding: only the last instance to start received
///      anything — `a == 0, b == N`.
///
/// Both failure modes are visible in the two assertions below, so this
/// test pins the fix rather than one half of it:
///   * `a + b == N` — no request handled twice (catches defect 1),
///   * `a > 0 && b > 0` — work actually spread (catches defect 2).
///
/// The reply consumer is deliberately NOT changed: it stays per-instance
/// and Fanout (`_svc-<service>-reply-<instance_id>`, filtered to that
/// instance's own subjects), which is why every request below still gets
/// exactly one reply back on the instance that issued it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn service_two_instances_share_requests() {
    const N: usize = 20;

    let addr = start_server().await;

    let hits_a = Arc::new(AtomicUsize::new(0));
    let hits_b = Arc::new(AtomicUsize::new(0));

    // Instance A — its own connection, like a separate process would have.
    let client_a = connect(&addr).await;
    let svc_a = client_a
        .service("sharing")
        .build()
        .await
        .expect("instance A build");
    {
        let hits = Arc::clone(&hits_a);
        svc_a.handle(b"work", move |_req| {
            let hits = Arc::clone(&hits);
            async move {
                hits.fetch_add(1, Ordering::SeqCst);
                Ok(b"a".to_vec())
            }
        });
    }

    // Instance B — same service name, second connection.
    let client_b = connect(&addr).await;
    let svc_b = client_b
        .service("sharing")
        .build()
        .await
        .expect("instance B build");
    {
        let hits = Arc::clone(&hits_b);
        svc_b.handle(b"work", move |_req| {
            let hits = Arc::clone(&hits);
            async move {
                hits.fetch_add(1, Ordering::SeqCst);
                Ok(b"b".to_vec())
            }
        });
    }

    // Let both dispatch loops register their handlers.
    tokio::time::sleep(Duration::from_millis(120)).await;

    // Requester on a third connection.
    let client_c = connect(&addr).await;
    let requester = client_c
        .service("sharing-caller")
        .build()
        .await
        .expect("requester build");

    for i in 0..N {
        let reply = requester
            .request(
                "sharing",
                b"work",
                Bytes::from(format!("req-{i}")),
                5_000,
            )
            .await
            .unwrap_or_else(|e| panic!("request {i} failed: {e:?}"));
        // Whichever instance took it, exactly one reply comes back.
        assert!(
            &reply[..] == b"a" || &reply[..] == b"b",
            "unexpected reply body for request {i}: {reply:?}"
        );
    }

    // Handlers run in spawned tasks; a duplicate delivery would land here
    // shortly after the reply the requester already accepted. Wait long
    // enough that a Fanout regression cannot hide behind the race.
    tokio::time::sleep(Duration::from_millis(400)).await;

    let a = hits_a.load(Ordering::SeqCst);
    let b = hits_b.load(Ordering::SeqCst);

    assert_eq!(
        a + b,
        N,
        "each request must be handled exactly once across the two \
         instances (got a={a}, b={b}, expected total {N}). A total of \
         {} means the worker consumer is fanning out instead of \
         queue-sharing.",
        2 * N
    );
    assert!(
        a > 0 && b > 0,
        "load must be spread across both instances, got a={a}, b={b}"
    );

    svc_a.close();
    svc_b.close();
    requester.close();
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
    processor.handle(b"double", |req| async move {
        let val = u32::from_le_bytes(req.data()[..4].try_into().unwrap_or([0; 4]));
        Ok((val * 2).to_le_bytes().to_vec())
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
    let svc = client
        .service("delayed")
        .build()
        .await
        .expect("service build");
    svc.handle(b"slow", |_req| async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        Ok(b"done".to_vec())
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
    svc.handle(b"work", |_req| async move { Ok(b"ok".to_vec()) });

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
        other => panic!("expected Timeout, got: {other:?}"),
    }

    requester.close();
}
