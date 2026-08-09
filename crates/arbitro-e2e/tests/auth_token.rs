//! Connection authentication — the shared bearer token, end to end.
//!
//! Authentication happens **once**, in the handshake, and never again: the TCP
//! stream is the security boundary, so once the peer is established every
//! later frame on that socket is from the same peer. Nothing on the delivery
//! path re-checks anything. These tests pin that contract at the two edges
//! that can actually break it — the first connect and a reconnect.

mod test_helper;
use test_helper::{TestServer, TestServerBuilder};

use arbitro_client_tokio::{Client, ClientConfig, ReconnectPolicy};
use bytes::Bytes;
use std::time::Duration;

const TOKEN: &str = "s3cret-token";

fn cfg(addr: &str, token: Option<&str>) -> ClientConfig {
    ClientConfig {
        addr: addr.to_string(),
        // No retries: every test here asserts on the FIRST outcome. With the
        // default (effectively unbounded) policy a rejected connect would
        // stall the test instead of failing it.
        reconnect: ReconnectPolicy {
            max_attempts: Some(0),
            ..Default::default()
        },
        auth_token: token.map(str::to_string),
        ..Default::default()
    }
}

/// The happy path: right token in, broker serves the connection normally.
#[tokio::test]
async fn correct_token_connects_and_works() {
    let server = TestServerBuilder::new().auth_token(TOKEN).spawn().await;

    let client = Client::connect(cfg(&server.addr, Some(TOKEN)))
        .await
        .expect("correct token must be accepted");

    // Prove the session is genuinely usable, not merely "not closed yet" —
    // a connection that authenticated but wasn't marked done would accept
    // the socket and then drop the first real frame.
    let resp = client
        .create_stream(b"auth_ok", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .expect("stream create must succeed on an authenticated connection");
    let stream_id = TestServer::parse_id(&resp);
    client
        .publish_wait(stream_id, b"auth_ok.a", Bytes::from_static(b"hello"))
        .await
        .expect("publish must succeed on an authenticated connection");
}

/// A wrong token is rejected, and rejection is *terminal* — the client must
/// not sit in a backoff loop re-offering a credential that cannot ever work.
#[tokio::test]
async fn wrong_token_is_rejected_and_does_not_hang() {
    let server = TestServerBuilder::new().auth_token(TOKEN).spawn().await;

    // The bound matters as much as the failure: without terminal
    // classification the client redials forever and this never returns.
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(cfg(&server.addr, Some("wrong-token"))),
    )
    .await;

    match result {
        Err(_) => panic!("connect with a wrong token hung instead of failing"),
        Ok(Ok(client)) => {
            // The broker closes without replying on success, so `connect`
            // itself may return before the rejection lands. What must NOT
            // happen is the connection working.
            let used = client
                .create_stream(b"auth_bad", b">", 0, 0, 0, 1, 0, 0, 0, 0)
                .await;
            assert!(
                used.is_err(),
                "a connection with a wrong token must not be usable"
            );
        }
        Ok(Err(_)) => { /* rejected at connect — the clearest outcome */ }
    }
}

/// Sending no token to a broker that requires one is rejected too. This is
/// `AuthRequired`, not `AuthFailed`: the distinction tells a client whether to
/// supply a credential or give up.
#[tokio::test]
async fn missing_token_against_auth_broker_is_rejected() {
    let server = TestServerBuilder::new().auth_token(TOKEN).spawn().await;

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(cfg(&server.addr, None)),
    )
    .await
    .expect("connect without a token hung instead of failing");

    if let Ok(client) = result {
        let used = client
            .create_stream(b"auth_none", b">", 0, 0, 0, 1, 0, 0, 0, 0)
            .await;
        assert!(
            used.is_err(),
            "an unauthenticated connection must not be usable against an auth broker"
        );
    }
}

/// A token-carrying client against a broker with auth DISABLED must still
/// work. This is the regression that matters most in practice: `Action::Auth`
/// had no dispatch arm, so an unsolicited Auth frame fell through to
/// `Unimplemented` + `Err`, which the read loop treats as a malformed frame
/// and drops the connection. Every token-configured client would have died on
/// frame one against a broker that simply doesn't want a token.
#[tokio::test]
async fn token_against_broker_without_auth_still_works() {
    let server = TestServerBuilder::new().spawn().await;

    let client = Client::connect(cfg(&server.addr, Some(TOKEN)))
        .await
        .expect("a token must be harmless against a broker with auth disabled");

    let resp = client
        .create_stream(b"auth_off", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .expect("unsolicited Auth frame must not kill the connection");
    let stream_id = TestServer::parse_id(&resp);
    client
        .publish_wait(stream_id, b"auth_off.a", Bytes::from_static(b"hi"))
        .await
        .expect("connection must stay usable after an ignored Auth frame");
}

/// No token, no auth configured: byte-for-byte the pre-auth behavior. Pins
/// that turning the feature on by default never happened.
#[tokio::test]
async fn no_auth_configured_needs_no_token() {
    let server = TestServerBuilder::new().spawn().await;

    let client = Client::connect(cfg(&server.addr, None))
        .await
        .expect("a broker without auth must accept a plain connection");

    let resp = client
        .create_stream(b"auth_plain", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .expect("plain connection must work");
    assert!(TestServer::parse_id(&resp) > 0);
}

/// The token must survive a reconnect. This is the failure mode that hides
/// for months: first connect authenticates, the broker restarts, and the
/// redial path forgets the credential — so the client silently loses its
/// session and nothing explains why. The token is written by
/// `write_handshake`, the single choke point used by both the first dial and
/// every redial, which is what makes this pass by construction.
#[tokio::test]
async fn token_is_resent_after_reconnect() {
    // A fixed address so the restarted broker is reachable at the same place.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    drop(listener);

    let mut server = TestServerBuilder::new()
        .auth_token(TOKEN)
        .spawn_on(&addr)
        .await;

    // `spawn_on` binds asynchronously, so the first dial races the listener.
    // The helper retries; a plain `connect` would fail on refused, not on auth.
    let client = server
        .connect_with_config(ClientConfig {
            auth_token: Some(TOKEN.to_string()),
            ..Default::default()
        })
        .await;

    client
        .create_stream(b"auth_rc", b">", 0, 0, 0, 1, 0, 0, 0, 0)
        .await
        .expect("pre-restart stream create");

    server.shutdown().await;
    let _server2 = TestServerBuilder::new()
        .auth_token(TOKEN)
        .spawn_on(&addr)
        .await;

    // Retry an operation that needs no prior state — the restarted broker is
    // empty, so anything referencing the old stream id would fail for a
    // reason that has nothing to do with authentication.
    //
    // A reconnect that dropped the token is rejected at every handshake, so
    // this never succeeds and the loop runs out.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut reconnected = false;
    while tokio::time::Instant::now() < deadline {
        if client
            .create_stream(b"auth_rc2", b">", 0, 0, 0, 1, 0, 0, 0, 0)
            .await
            .is_ok()
        {
            reconnected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        reconnected,
        "client never re-authenticated after the broker restarted — \
         the reconnect path dropped the token"
    );
}
