---
description: Workflow for auditing broker security, tenant isolation, and data integrity
arguments: [LOW, MEDIUM, HIGH, Custom]
---

# SECURITY AUDIT WORKFLOW

This workflow defines the steps to validate the security posture of the Arbitro broker.
Node: Agent ask for Level of Auditation  LOW, MEDIUM, HIGH or Custom Note

## 1. Tenant Isolation & Identity
- [ ] **Audit Level**: Agent must ask for Level LOW, MEDIUM, HIGH or Custom Note for audit

## 1.1 Tenant Isolation & Identity
- [ ] **Consumer Name Scoping**: Verify that consumers with identical names in different streams do NOT collapse into the same `ConsumerId` unless intended.
- [ ] **Limit Leaks**: Assert that `max_subject_inflight` and `max_inflight` caps are strictly enforced per `ConsumerId` and do not leak across connections.
- [ ] **Cross-Stream Access**: Validate that a connection bound to Stream A cannot `claim` or `ack` messages from Stream B without explicit permissions.

## 2. Data Integrity & Persistence
- [ ] **Journal Integrity**: Verify that every journal entry includes a checksum (CRC32/CRC64). Test with corrupted bits to ensure the recovery path rejects invalid data.
- [ ] **Sequence Consistency**: Assert that `seq` numbers are monotonically increasing and that the `Store` rejects out-of-order or duplicate writes.
- [ ] **Replay Safety**: Perform a crash-replay test. Ensure the state after recovery exactly matches the state before the crash.

## 3. Transport & Protocol Security
- [ ] **Frame Validation**: Audit the `wire_parse.rs` logic. Ensure that malformed frames (invalid length, unknown action codes) trigger immediate connection termination, not panics.
- [ ] **Zero-Copy Bounds**: Verify that `zerocopy` casts (`ref_from_bytes`) are performed only on validated, aligned slices to prevent out-of-bounds or alignment UB.
- [ ] **TLS Audit**: If TLS is enabled, verify cipher suites and certificate validation logic. Ensure the broker rejects non-encrypted connections if configured.

## 4. Availability & DoS Protection
- [ ] **Backpressure Stress**: Saturate the MPSC shard channel. Verify that the sender `.await` blocks and that the system does not crash or leak memory.
- [ ] **Memory Exhaustion**: Audit `Accumulator` and `Inflight` maps. Ensure they have hard caps and do not grow unboundedly with slow consumers.
- [ ] **Resource Poisoning**: Verify that a panic in one shard thread is caught (or handled via graceful shutdown) and does not compromise other shards or the main router.

## 5. Observability & Audit
- [ ] **Audit Logs**: Verify that management actions (CreateStream, DeleteConsumer, Bind) are logged with sufficient metadata (timestamp, connection_id).
- [ ] **Anomaly Detection**: Use `engine.metrics()` to verify that abnormally high `Nack` or `RepError` rates are visible to monitoring systems.