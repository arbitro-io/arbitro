//! Magic signature for the v2 store format.
//!
//! Appears at the start of every persisted `BatchHeader`. Not present in
//! transport frames (TCP provides integrity). Purpose:
//!   * Identifies format when opening a log file (`hexdump` shows `ARB2`).
//!   * Allows recovery after partial writes by rescanning for the magic.
//!   * Reserves a clean path for future format changes (`ARB3` = v3).

/// `"ARB2"` as little-endian u32 — readable byte-wise in `hexdump`:
/// `32 42 52 41` reversed → `'A' 'R' 'B' '2'` on BE view. We store as LE
/// u32 so the on-disk bytes read `41 52 42 32` = `"ARB2"` directly.
pub const ARBITRO_MAGIC_V2: u32 = u32::from_le_bytes(*b"ARB2");

pub const MAGIC_SIZE: usize = core::mem::size_of::<u32>();

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn magic_is_readable_ascii() {
        let bytes = ARBITRO_MAGIC_V2.to_le_bytes();
        assert_eq!(&bytes, b"ARB2");
    }
}
