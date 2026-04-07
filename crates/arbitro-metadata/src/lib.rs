use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::Mutex;

use arbitro_proto::metadata::MetadataCommand;
use tracing::{info, error};

/// Interface for applying metadata commands to a state machine (the Engine).
/// This breaks the cyclic dependency between arbitro-engine and arbitro-metadata.
pub trait MetadataApplier {
    fn apply_command(&self, command: MetadataCommand);
}

/// Persistent Command Ledger for Metadata.
/// 
/// Handles append-only logging of management actions (MetadataCommand)
/// to a local file for crash recovery and deterministic state replay.
pub struct MetadataLog {
    path: PathBuf,
    file: Mutex<File>,
}

impl MetadataLog {
    /// Open or create a metadata log at the given path.
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

        Ok(Self {
            path,
            file: Mutex::new(file),
        })
    }

    /// Append a command to the log. Cold path (Management only).
    pub fn record(&self, command: &MetadataCommand) -> std::io::Result<()> {
        let mut line = serde_json::to_string(command).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        })?;
        line.push('\n');

        let mut file = self.file.lock().unwrap();
        file.write_all(line.as_bytes())?;
        file.flush()?; // Ensure durability for metadata changes
        Ok(())
    }

    /// Replay all commands from the log into the applier.
    /// Returns the number of commands successfully applied.
    pub fn replay(&self, applier: &dyn MetadataApplier) -> std::io::Result<u32> {
        if !self.path.exists() {
            return Ok(0);
        }
        
        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);
        let mut count = 0;

        info!("Replaying metadata log from {:?}", self.path);

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }

            let command: MetadataCommand = match serde_json::from_str(&line) {
                Ok(cmd) => cmd,
                Err(e) => {
                    error!("Failed to parse metadata command: {}. Line: {}", e, line);
                    continue;
                }
            };

            applier.apply_command(command);
            count += 1;
        }

        info!("Successfully replayed {} metadata commands", count);
        Ok(count)
    }
}
