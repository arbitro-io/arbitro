//! File-based metadata command log — append-only, zerocopy, raft-compatible.
//!
//! Every management mutation (create/delete stream/consumer) is persisted as
//! raw bytes using length-prefix framing:
//!
//! ```text
//! [4 len_le][len bytes payload] [4 len_le][len bytes payload] ...
//! ```
//!
//! The payload is `[1 command_type][body...]` — identical to what
//! `StateMachine::apply(&[u8])` receives in arbitro-raft.
//!
//! No serde, no JSON, no copies. The same raw bytes that travel through raft
//! are appended to the local file. On replay, each entry is parsed with
//! `MetadataCommandView` (zero-copy view).
//!
//! Cold path only — called on create/delete stream/consumer.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use arbitro_proto::lifecycle::LifeCycle;
use arbitro_proto::metadata::{MetadataApplier, MetadataCommandView};
use tracing::{info, warn};

/// Append-only command log with length-prefix framing.
///
/// Thread safety: the server ensures only one thread writes metadata
/// (the shard worker or a dedicated metadata task). No Mutex needed.
pub struct CommandLog {
    path: PathBuf,
    file: File,
}

impl CommandLog {
    /// Open or create a command log at the given path.
    pub fn open(path: impl Into<PathBuf>) -> std::io::Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        Ok(Self { path, file })
    }

    /// Append a raw metadata command to the log.
    ///
    /// Framing: `[4 len_le][payload]`. The payload is the full metadata
    /// command bytes (`[1 type][body...]`).
    ///
    /// Flushes after each write for durability. Cold path only.
    pub fn record(&mut self, command: &[u8]) -> std::io::Result<()> {
        let len = command.len() as u32;
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(command)?;
        self.file.flush()?;
        Ok(())
    }

    /// Replay all commands from the log into the applier.
    ///
    /// Returns the number of commands successfully applied.
    /// Tolerates truncated trailing entries (incomplete write before crash).
    pub fn replay(&self, applier: &mut dyn MetadataApplier) -> std::io::Result<u32> {
        if !self.path.exists() {
            return Ok(0);
        }

        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);
        let mut count = 0u32;
        let mut len_buf = [0u8; 4];

        info!("Replaying metadata log from {:?}", self.path);

        loop {
            // Read length prefix
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break, // clean EOF
                Err(e) => return Err(e),
            }
            let len = u32::from_le_bytes(len_buf) as usize;

            if len == 0 {
                warn!("Zero-length entry at offset, skipping");
                continue;
            }

            // Read payload
            let mut payload = vec![0u8; len];
            match reader.read_exact(&mut payload) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    warn!("Truncated entry at end of log (crash recovery). Stopping replay.");
                    break;
                }
                Err(e) => return Err(e),
            }

            // Validate and apply
            if MetadataCommandView::new(&payload).is_some() {
                applier.apply(&payload);
                count += 1;
            } else {
                warn!("Invalid metadata command at entry {}, skipping", count);
            }
        }

        info!("Successfully replayed {} metadata commands", count);
        Ok(count)
    }
}

// ── Shared handle ──────────────────────────────────────────────────────────

/// Thread-safe, clone-friendly handle to a CommandLog.
///
/// Wraps `Arc<Mutex<CommandLog>>`. The Mutex is uncontended in practice —
/// metadata ops are cold path and serialized through the dispatch layer.
#[derive(Clone)]
pub struct SharedCommandLog {
    inner: Arc<Mutex<CommandLog>>,
}

impl SharedCommandLog {
    pub fn new(log: CommandLog) -> Self {
        Self { inner: Arc::new(Mutex::new(log)) }
    }

    /// Record a raw metadata command. Cold path — Mutex is fine.
    pub fn record(&self, command: &[u8]) -> std::io::Result<()> {
        self.inner.lock().unwrap().record(command)
    }

    /// Replay all commands into the applier.
    pub fn replay(&self, applier: &mut dyn MetadataApplier) -> std::io::Result<u32> {
        self.inner.lock().unwrap().replay(applier)
    }
}

impl LifeCycle for SharedCommandLog {
    fn on_init(&mut self) {
        let log = self.inner.lock().unwrap();
        info!("CommandLog: init (log at {:?})", log.path);
    }

    fn on_shutdown(&mut self) {
        let mut log = self.inner.lock().unwrap();
        if let Err(e) = log.file.flush() {
            warn!("CommandLog: flush on shutdown failed: {}", e);
        }
        info!("CommandLog: shutdown complete");
    }
}

impl LifeCycle for CommandLog {
    fn on_init(&mut self) {
        info!("CommandLog: init (log at {:?})", self.path);
    }

    fn on_shutdown(&mut self) {
        if let Err(e) = self.file.flush() {
            warn!("CommandLog: flush on shutdown failed: {}", e);
        }
        info!("CommandLog: shutdown complete");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arbitro_proto::metadata::*;
    use arbitro_proto::wire::stream::{CreateStreamFixed, CreateStreamView};
    use arbitro_proto::wire::manager::{DeleteConsumerAction, DeleteConsumerView};
    use zerocopy::IntoBytes;
    use zerocopy::byteorder::little_endian::{U16, U32, U64};

    /// Test applier that collects raw commands.
    struct CollectApplier {
        commands: Vec<Vec<u8>>,
    }

    impl CollectApplier {
        fn new() -> Self { Self { commands: Vec::new() } }
    }

    impl MetadataApplier for CollectApplier {
        fn apply(&mut self, command: &[u8]) {
            self.commands.push(command.to_vec());
        }
    }

    fn tmp_path() -> PathBuf {
        let dir = std::env::temp_dir().join("arbitro-test-cmdlog");
        std::fs::create_dir_all(&dir).unwrap();
        dir.join(format!("test-{}.log", std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()))
    }

    #[test]
    fn record_and_replay() {
        let path = tmp_path();

        // Build a CreateStream command
        let fixed = CreateStreamFixed {
            name_len: U16::new(6),
            filter_len: U16::new(8),
            max_msgs: U64::new(1000),
            max_bytes: U64::new(0),
            max_age_secs: U64::new(0),
            replicas: 1,
            journal_kind: 0,
            retention: 0,
            _pad: 0,
        };
        let mut body = Vec::new();
        body.extend_from_slice(fixed.as_bytes());
        body.extend_from_slice(b"orders");
        body.extend_from_slice(b"orders.>");
        let cmd1 = build_create_stream(&body);

        // Build a DeleteConsumer command
        let del = DeleteConsumerAction {
            consumer_id: U32::new(42),
            _pad: U32::new(0),
        };
        let cmd2 = build_delete_consumer(del.as_bytes());

        // Record
        {
            let mut log = CommandLog::open(&path).unwrap();
            log.record(&cmd1).unwrap();
            log.record(&cmd2).unwrap();
        }

        // Replay
        {
            let log = CommandLog::open(&path).unwrap();
            let mut applier = CollectApplier::new();
            let count = log.replay(&mut applier).unwrap();
            assert_eq!(count, 2);

            // Verify first command
            let view1 = MetadataCommandView::new(&applier.commands[0]).unwrap();
            assert_eq!(view1.command_type(), CMD_CREATE_STREAM);
            let sv = CreateStreamView::new(view1.body());
            assert_eq!(sv.name(), b"orders");
            assert_eq!(sv.filter(), b"orders.>");

            // Verify second command
            let view2 = MetadataCommandView::new(&applier.commands[1]).unwrap();
            assert_eq!(view2.command_type(), CMD_DELETE_CONSUMER);
            let dv = DeleteConsumerView::new(view2.body());
            assert_eq!(dv.consumer_id(), 42);
        }

        // Cleanup
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn empty_log_replays_zero() {
        let path = tmp_path();
        let log = CommandLog::open(&path).unwrap();
        let mut applier = CollectApplier::new();
        // File exists but is empty
        let count = log.replay(&mut applier).unwrap();
        assert_eq!(count, 0);
        assert!(applier.commands.is_empty());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn truncated_entry_is_tolerated() {
        let path = tmp_path();

        // Write one valid entry + a truncated one
        {
            let fixed = CreateStreamFixed {
                name_len: U16::new(4),
                filter_len: U16::new(1),
                max_msgs: U64::new(0),
                max_bytes: U64::new(0),
                max_age_secs: U64::new(0),
                replicas: 1,
                journal_kind: 0,
                retention: 0,
                _pad: 0,
            };
            let mut body = Vec::new();
            body.extend_from_slice(fixed.as_bytes());
            body.extend_from_slice(b"test");
            body.extend_from_slice(b">");
            let cmd = build_create_stream(&body);

            let mut log = CommandLog::open(&path).unwrap();
            log.record(&cmd).unwrap();

            // Write a length header for 100 bytes but only 5 bytes of payload (truncated)
            use std::io::Write;
            log.file.write_all(&100u32.to_le_bytes()).unwrap();
            log.file.write_all(&[1, 2, 3, 4, 5]).unwrap();
            log.file.flush().unwrap();
        }

        // Replay should get 1 valid entry, tolerate the truncated one
        {
            let log = CommandLog::open(&path).unwrap();
            let mut applier = CollectApplier::new();
            let count = log.replay(&mut applier).unwrap();
            assert_eq!(count, 1);

            let view = MetadataCommandView::new(&applier.commands[0]).unwrap();
            assert_eq!(view.command_type(), CMD_CREATE_STREAM);
        }

        let _ = std::fs::remove_file(&path);
    }
}
