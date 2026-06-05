# arbitro-client-tokio

Pure-tokio Rust client for the [Arbitro](https://github.com/arbitro-io/arbitro) stateful flow broker. Built on arbitro-kit primitives (Mpsc, OneShotAsync, Pipe, Hub) with a reconnect state machine and heartbeat watchdog.

## Workflow Orchestration

Client-side linear pipelines over Arbitro streams. The broker has no workflow-specific code -- everything uses streams, consumer groups, and idempotent publish.

### WorkflowBuilder API

| Method | Signature | Description |
|--------|-----------|-------------|
| `trigger` | `(subject: &[u8]) -> Self` | Subject pattern that triggers new instances. Required. |
| `trigger_stream` | `(stream_id: u32) -> Self` | Auto-subscribe to this stream for the trigger subject. |
| `step` | `(name: &[u8], handler: Fn(StepContext) -> Future<Result<StepResult, String>>) -> Self` | Append a processing step. |
| `compensate` | `(name: &[u8], handler: Fn(StepContext) -> Future<Result<StepResult, String>>) -> Self` | Rollback handler for the most recently added step. Runs in reverse on permanent failure. |
| `max_retries` | `(n: u8) -> Self` | Attempts before DLQ (default: 3). |
| `max_context_size` | `(bytes: usize) -> Self` | Max context payload in bytes (default: 256 KB). |
| `ack_wait_ms` | `(ms: u32) -> Self` | Ack timeout for failover (default: 30000). |
| `max_inflight` | `(n: u16) -> Self` | Concurrent tasks per worker (default: 10). |
| `start` | `() -> Result<WorkflowHandle, ClientError>` | Register streams, consumer, and spawn processing loop. |

### WorkflowHandle API

| Method | Signature | Description |
|--------|-----------|-------------|
| `trigger` | `(&self, client: &Client, context: &[u8]) -> Result<u32, ClientError>` | Trigger a new workflow instance. Returns the instance ID. |
| `stop` | `(&self)` | Cancel the processing loop. |
| `task_stream_id` | `(&self) -> u32` | Internal task stream ID. |
| `dlq_stream_id` | `(&self) -> u32` | Dead letter queue stream ID. |
| `name` | `(&self) -> &[u8]` | Workflow name. |

### Complete Example

```rust
use arbitro_client_tokio::{Client, StepResult, StepContext};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::connect(b"127.0.0.1:9898").await?;

    // Create the source stream for auto-trigger
    let orders_stream_id = client
        .create_stream(b"ORDERS", b"orders.>", 0, 0, 0, 1, 0, 0, 0, 0)
        .await?;

    let wf = client.workflow(b"order-process")
        .trigger(b"orders.created")
        .trigger_stream(orders_stream_id)
        // Step 1: validate
        .step(b"validate", |ctx: StepContext| async move {
            let validated = validate_order(&ctx.context)?;
            Ok(StepResult { context: validated })
        })
        .compensate(b"validate", |ctx: StepContext| async move {
            rollback_validation(&ctx.context).await;
            Ok(StepResult { context: ctx.context })
        })
        // Step 2: charge
        .step(b"charge", |ctx: StepContext| async move {
            let receipt = charge_payment(&ctx.context).await?;
            Ok(StepResult { context: receipt })
        })
        .compensate(b"charge", |ctx: StepContext| async move {
            refund_payment(&ctx.context).await;
            Ok(StepResult { context: ctx.context })
        })
        // Step 3: ship (no compensation needed)
        .step(b"ship", |ctx: StepContext| async move {
            let tracking = create_shipment(&ctx.context).await?;
            Ok(StepResult { context: tracking })
        })
        .max_retries(3)
        .max_context_size(256 * 1024)
        .ack_wait_ms(30_000)
        .max_inflight(10)
        .start().await?;

    // Manual trigger
    let instance_id = wf.trigger(&client, b"order-123-payload").await?;
    println!("started instance {instance_id}");

    // DLQ stream is available for monitoring
    println!("DLQ stream: {}", wf.dlq_stream_id());

    // Stop when done
    wf.stop();
    Ok(())
}
```

### Internals

- Tasks flow through `_wf_{name}_tasks` stream with a shared consumer group `_wf_{name}_workers`.
- Each step transition publishes with `msg_id` format `wf:{instance}:{step}:{attempt}` for deduplication.
- On permanent failure (attempts >= `max_retries`), the task is published to `_wf_{name}_dlq` and compensation handlers run in reverse.
- `ack_wait_ms` enables failover: if a worker dies, the broker redelivers to another group member.
- `msg_id` is persisted in the journal via `HAS_HEADERS` and rebuilt on broker restart.

## License

MIT
