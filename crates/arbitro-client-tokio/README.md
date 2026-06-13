# arbitro-client-tokio

Rust client for the [Arbitro](https://github.com/arbitro-io/arbitro) message broker.

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
    // suspend_step(name, timeout_ms, run_handler, on_resume_handler)
    .suspend_step(b"wait-auth", 30_000,
        // run: called when step starts — return Suspend to park
        |ctx: StepContext| async move {
            let state = send_auth_link(&ctx.context).await?;
            Ok(StepOutcome::Suspend { state, timeout_ms: 30_000 })
        },
        // on_resume: called when wf.resume() arrives with an event
        |resume: ResumeContext| async move {
            let result = process_payment(&resume.state, &resume.event).await?;
            Ok(StepResult { context: result })
        },
    )
    // on_timeout: called if 30s pass without resume
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
    .source(b"external-events", b"events.>")  // stream_name + subject
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
