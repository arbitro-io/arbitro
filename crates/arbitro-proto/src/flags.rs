/// Envelope flags (1 byte).
pub mod envelope {
    pub const NO_ACK: u8     = 0b0000_0001;
    pub const BATCH: u8      = 0b0000_0010;
    pub const COMPRESSED: u8 = 0b0000_0100;
}

/// Entry flags (1 byte per batch entry).
pub mod entry {
    pub const HAS_REPLY_TO: u8 = 0b0000_0001;
    pub const PERSISTENT: u8   = 0b0000_0010;
}
