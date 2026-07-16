# arbitro-client-tokio

Rust client for the [Arbitro](https://github.com/arbitro-io/arbitro) message broker.

## Publish

```rust
use arbitro_client_tokio::Client;
use bytes::Bytes;

let client = Client::connect(b"127.0.0.1:9898").await?;

let stream_id = client
    .create_stream(b"ORDERS", b"orders.>", 0, 0, 0, 1, 0, 0, 0, 0)
    .await?;

// Fire-and-forget
client.publish(stream_id, b"orders.new", Bytes::from_static(b"payload"))?;

// Sync — waits for broker confirmation (RepOk with first_seq)
let reply = client.publish_sync(stream_id, b"orders.new", payload.into()).await?;

// With dedup (idempotency)
client.publish_with_id(stream_id, b"orders.new", b"order-abc-123", payload.into())?;

// Batch
client.publish_batch(stream_id, &[
    BatchEntry { subject: b"orders.a", payload: &payload, msg_id: b"" },
    BatchEntry { subject: b"orders.b", payload: &payload, msg_id: b"dedup-key" },
])?;

// Delayed — delivered after N ms
let reply = client.publish_delayed(stream_id, b"orders.reminder", payload.into(), 5000).await?;

```

## Publish with Headers

Attach arbitrary key-value metadata to messages. Headers are persisted alongside the payload and stripped on delivery -- consumers always receive only the user payload.

```rust
// Custom headers (tracing, routing metadata)
client.publish_with_headers(
    stream_id,
    b"orders.created",
    &[(b"trace-id", b"abc-123"), (b"source", b"checkout-svc")],
    payload.into(),
).await?;

// Headers with dedup (msg-id is a well-known header key)
client.publish_with_headers(
    stream_id,
    b"orders.created",
    &[(b"msg-id", b"order-abc-123"), (b"priority", b"high")],
    payload.into(),
).await?;
```

Headers use a zero-copy TLV wire format (defined in `arbitro-proto::wire::msg_headers`). The broker stores them with the entry using the `HAS_HEADERS` flag and strips them at delivery time so consumers see only the user payload.

## Subscribe

```rust
use arbitro_client_tokio::ConsumerBuilder;
use arbitro_client_tokio::AckPolicy;

let consumer = ConsumerBuilder::new(b"worker")
    .filter(b"orders.>")
    .max_inflight(100)
    .ack_policy(AckPolicy::Explicit)
    .create(&client, stream_id).await?;

let mut sub = client.subscribe(stream_id, consumer, b"").await?;
while let Some(msg) = sub.recv().await {
    process(&msg.payload);
    msg.ack();
}
```

## Persistent Redelivery Dedup (ackstore)

The broker delivers at-least-once, so a crash between "processed" and
"acked" can lead to a redelivery. To skip work already done — even across a
process restart — attach a durable dedup store keyed by
`(stream_name, consumer_name, seq)`:

```rust
use arbitro_client_tokio::ackstore::wal::Wal;
use arbitro_client_tokio::ackstore::WalConfig;

// Durable write-ahead log; survives restarts.
let wal = Wal::open(WalConfig::new("/var/lib/app/ackstore"))?;
let client = Client::connect_with_ackstore(cfg, wal).await?;

// subscribe_dedup carries the names the store needs to resolve a slot.
let mut sub = client
    .subscribe_dedup("orders", "worker", stream_id, consumer, b"")
    .await?;
while let Some(msg) = sub.recv().await {
    process(&msg.payload); // runs at most once per seq, across restarts
    msg.ack();             // records the seq into the WAL
}
```

On `ack`, the message's `seq` is recorded into the log. On delivery, a
lock-free `(min,max)` bounds probe skips any `seq` already recorded (the
redelivery is silently re-acked). The broker's ack cursor
(`AckBatchResp` / `AckStateRep`) drives `confirm_up_to`, dropping confirmed
seqs so the live set stays tiny with no periodic job. The key is the
**names**, not the numeric `consumer_id` — a consumer deleted and recreated
under the same name still recognizes already-completed work.

The [`Store`] trait is pluggable: `Wal` is the durable backend,
`MemoryStore` an in-process one; a future infra swap needs no hot-path
changes. (This WAL replaced the old optional SQLite cold tier — there is no
longer any `rusqlite` dependency.)

## Service (Request/Reply RPC)

Build named services with automatic stream/consumer creation, handler dispatch, and correlated request/reply.

```rust
use arbitro_client_tokio::{Client, ServiceBuilder};

let client = Client::connect(b"127.0.0.1:9898").await?;

// Build a service — creates backing stream + consumer automatically
let svc = client.service(b"calculator")
    .max_inflight(1024)
    .build().await?;

// Register method handlers.
// The handler returns Result<Vec<u8>, HandlerError> — the framework
// publishes the response to the requester and acks the delivery
// automatically. Return `Err(_)` to nack. Return `Ok(vec![])` to ack
// without replying.
svc.handle(b"add", |req| async move {
    Ok(format!("sum={}", compute_add(req.data())).into_bytes())
});

svc.handle(b"multiply", |req| async move {
    Ok(format!("product={}", compute_mul(req.data())).into_bytes())
});

// Send a request to another service (or self)
let response = svc.request(b"calculator", b"add", b"2+3", 5000).await?;
assert_eq!(response, b"sum=5");

// Fire-and-forget to another service
svc.send(b"audit", b"log", b"event-data").await?;

// Cross-service RPC (service B calling service A)
let svc_b = client.service(b"gateway").build().await?;
let resp = svc_b.request(b"calculator", b"multiply", b"3*4", 5000).await?;
```

The service pattern uses:
- Stream name: `_svc-<name>` (dashes for `validate_name` compliance)
- Consumer name: `_svc-<name>-worker`
- Subject filter: `_svc.<name>.>` (dots in subjects)
- Reply correlation: `_svc.<name>._r.<corr_id>`
- Reply-to encoding: `[0xFF][stream_id LE u32][reply subject bytes]`

`msg.reply()` always works -- no need to check `has_reply_to()`.

## Workflow Orchestration

Client-side workflow pipelines over Arbitro streams. The broker has no workflow-specific code -- everything uses streams, consumer groups, and idempotent publish.

### WorkflowBuilder API

| Method | Signature | Description |
|--------|-----------|-------------|
| `trigger` | `(subject: &[u8]) -> Self` | Subject pattern that triggers new instances. |
| `trigger_stream` | `(stream_id: u32) -> Self` | Auto-subscribe to this stream for the trigger subject. |
| `trigger_with_id` | `(client, id: &[u8], context: &[u8]) -> Result` | Trigger with an explicit instance ID (dedup-safe). |
| `source` | `(stream_name: &[u8], subject: &[u8]) -> Self` | External stream as event source for triggers. |
| `step` | `(name: &[u8], handler) -> Self` | Append a processing step. |
| `suspend_step` | `(name: &[u8], timeout_ms: u64, run, on_resume) -> Self` | Step that can suspend (park) and wait for external resume. |
| `on_timeout` | `(handler) -> Self` | Timeout handler for the preceding suspend step. |
| `compensate` | `(name: &[u8], handler) -> Self` | Rollback handler for the most recently added step. Runs in reverse on permanent failure. |
| `max_retries` | `(n: u8) -> Self` | Attempts before DLQ (default: 3). |
| `max_context_size` | `(bytes: usize) -> Self` | Max context payload in bytes (default: 256 KB). |
| `ack_wait_ms` | `(ms: u32) -> Self` | Ack timeout for failover (default: 30000). |
| `max_inflight` | `(n: u16) -> Self` | Concurrent tasks per worker (default: 10). |
| `start` | `() -> Result<WorkflowHandle>` | Register streams, consumer, and spawn processing loop. |

### WorkflowHandle API

| Method | Signature | Description |
|--------|-----------|-------------|
| `trigger` | `(&self, client, context: &[u8]) -> Result<u32>` | Trigger a new workflow instance. Returns the instance ID. |
| `trigger_with_id` | `(&self, client, id: &[u8], context: &[u8]) -> Result<()>` | Trigger with an explicit instance ID. |
| `resume` | `(&self, client, instance_id: &[u8], payload: &[u8]) -> Result<()>` | Resume a suspended workflow instance. |
| `cancel` | `(&self, client, instance_id: &[u8]) -> Result<()>` | Cancel a running or suspended workflow instance. |
| `stop` | `(&self)` | Cancel the processing loop. |
| `task_stream_id` | `(&self) -> u32` | Internal task stream ID. |
| `dlq_stream_id` | `(&self) -> u32` | Dead letter queue stream ID. |
| `name` | `(&self) -> &[u8]` | Workflow name. |

### Basic Example

```rust
use arbitro_client_tokio::{Client, StepResult, StepContext};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::connect(b"127.0.0.1:9898").await?;

    let orders_stream_id = client
        .create_stream(b"ORDERS", b"orders.>", 0, 0, 0, 1, 0, 0, 0, 0)
        .await?;

    let wf = client.workflow(b"order-process")
        .trigger(b"orders.created")
        .trigger_stream(orders_stream_id)
        .step(b"validate", |ctx: StepContext| async move {
            let validated = validate_order(&ctx.context)?;
            Ok(StepResult { context: validated })
        })
        .compensate(b"validate", |ctx: StepContext| async move {
            rollback_validation(&ctx.context).await;
            Ok(StepResult { context: ctx.context })
        })
        .step(b"charge", |ctx: StepContext| async move {
            let receipt = charge_payment(&ctx.context).await?;
            Ok(StepResult { context: receipt })
        })
        .compensate(b"charge", |ctx: StepContext| async move {
            refund_payment(&ctx.context).await;
            Ok(StepResult { context: ctx.context })
        })
        .step(b"ship", |ctx: StepContext| async move {
            let tracking = create_shipment(&ctx.context).await?;
            Ok(StepResult { context: tracking })
        })
        .max_retries(3)
        .max_context_size(256 * 1024)
        .ack_wait_ms(30_000)
        .max_inflight(10)
        .start().await?;

    let instance_id = wf.trigger(&client, b"order-123-payload").await?;
    println!("started instance {instance_id}");

    wf.stop();
    Ok(())
}
```

### Suspend / Resume / Cancel

```rust
use arbitro_client_tokio::{StepOutcome, ResumeContext, TimeoutContext};

let wf = client.workflow(b"payment-auth")
    .trigger(b"payments.initiated")
    .step(b"prepare", |ctx: StepContext| async move {
        let prepared = prepare_payment(&ctx.context).await?;
        Ok(StepResult { context: prepared })
    })
    .suspend_step(b"wait-auth", 30_000,
        |ctx: StepContext| async move {
            let state = send_auth_link(&ctx.context).await?;
            Ok(StepOutcome::Suspend { state, timeout_ms: 30_000 })
        },
        |resume: ResumeContext| async move {
            let result = process_payment(&resume.state, &resume.event).await?;
            Ok(StepResult { context: result })
        },
    )
    .on_timeout(|timeout: TimeoutContext| async move {
        let cancelled = cancel_auth(&timeout.state).await?;
        Ok(StepResult { context: cancelled })
    })
    .step(b"finalize", |ctx: StepContext| async move {
        Ok(StepResult { context: finalize_payment(&ctx.context).await? })
    })
    .start().await?;

// Trigger with explicit ID (dedup-safe)
wf.trigger_with_id(&client, "payment-abc-123", b"payload").await?;

// ... later, Stripe webhook confirms payment ...
wf.resume(&client, "payment-abc-123", b"stripe-event").await?;

// Or cancel a suspended instance
wf.cancel(&client, "payment-abc-123").await?;
```

### Source (External Stream Triggers)

```rust
let wf = client.workflow(b"event-driven")
    .source(b"external-events", b"events.>")
    .step(b"process", |ctx: StepContext| async move {
        Ok(StepResult { context: process_event(&ctx.context).await? })
    })
    .start().await?;
```

## Stream Management

```rust
client.delete_message(b"orders", 42).await?;
```

## Replication

Replication is transparent to the client -- `replicas` is set at `create_stream` time. The client publishes normally; the broker handles replication internally.

## License

MIT
