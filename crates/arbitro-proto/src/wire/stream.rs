use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// 40B fixed — Create a stream. Variable name + filter follow.
///
/// Mirror of `v2::manager::stream_mgmt::CreateStreamBody`. The
/// command log persists v2 wire bodies bytewise, and the recovery
/// pass parses them through `CreateStreamView` — so this struct
/// MUST stay in lock-step with the v2 body (same fields, same
/// order, same size).
///
/// ```text
/// [2 name_len][2 filter_len][8 max_msgs][8 max_bytes][8 max_age_secs]
/// [1 replicas][1 journal_kind][1 retention][1 discard]
/// [4 idempotency_window_ms][4 _pad]
/// ```
///
/// `filter`: subject pattern this stream captures (e.g. `"orders.>"`).
/// `discard`: 0 = Old (ring-buffer, default), 1 = New (reject when full).
/// `idempotency_window_ms`: 0 = idempotency disabled (legacy default),
/// >0 = dedup window in milliseconds.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct CreateStreamFixed {
    pub name_len: U16,
    pub filter_len: U16,
    pub max_msgs: U64,
    pub max_bytes: U64,
    pub max_age_secs: U64,
    pub replicas: u8,
    pub journal_kind: u8,
    pub retention: u8,
    pub discard: u8,
    pub idempotency_window_ms: U32,
    pub _pad: U32,
}

pub const CREATE_STREAM_FIXED_SIZE: usize = core::mem::size_of::<CreateStreamFixed>();
const _: () = assert!(CREATE_STREAM_FIXED_SIZE == 40);

/// 8B fixed — Delete a stream. Variable name follows.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct DeleteStreamFixed {
    pub name_len: U16,
    pub _pad: [u8; 6],
}

pub const DELETE_STREAM_FIXED_SIZE: usize = core::mem::size_of::<DeleteStreamFixed>();
const _: () = assert!(DELETE_STREAM_FIXED_SIZE == 8);

/// 8B fixed — Get stream info. Variable name follows.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct GetStreamFixed {
    pub name_len: U16,
    pub _pad: [u8; 6],
}
const _: () = assert!(core::mem::size_of::<GetStreamFixed>() == 8);

/// 8B fixed — Purge all messages from a stream. Variable name follows.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct PurgeStreamFixed {
    pub name_len: U16,
    pub _pad: [u8; 6],
}

pub const PURGE_STREAM_FIXED_SIZE: usize = core::mem::size_of::<PurgeStreamFixed>();
const _: () = assert!(PURGE_STREAM_FIXED_SIZE == 8);

/// 8B fixed — Drain messages by subject from a stream. Variable name + subject follow.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct DrainSubjectFixed {
    pub name_len: U16,
    pub subj_len: U16,
    pub _pad: [u8; 4],
}

pub const DRAIN_SUBJECT_FIXED_SIZE: usize = core::mem::size_of::<DrainSubjectFixed>();
const _: () = assert!(DRAIN_SUBJECT_FIXED_SIZE == 8);

// ── Lazy views ──────────────────────────────────────────────────────────────

pub struct CreateStreamView<'a> {
    buf: &'a [u8],
}

impl<'a> CreateStreamView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    /// Fallible constructor: checks `buf.len() >= CREATE_STREAM_FIXED_SIZE`
    /// before wrapping it. Prefer this over `new()` for untrusted input
    /// (e.g. metadata-log replay).
    #[inline(always)]
    pub fn try_new(buf: &'a [u8]) -> Option<Self> {
        if buf.len() < CREATE_STREAM_FIXED_SIZE {
            return None;
        }
        Some(Self { buf })
    }

    #[inline(always)]
    fn fixed(&self) -> &CreateStreamFixed {
        CreateStreamFixed::ref_from_bytes(&self.buf[..CREATE_STREAM_FIXED_SIZE]).unwrap()
    }

    #[inline(always)]
    fn try_fixed(&self) -> Option<&CreateStreamFixed> {
        CreateStreamFixed::ref_from_bytes(self.buf.get(..CREATE_STREAM_FIXED_SIZE)?).ok()
    }

    #[inline(always)]
    pub fn name(&self) -> &'a [u8] {
        let nl = self.fixed().name_len.get() as usize;
        &self.buf[CREATE_STREAM_FIXED_SIZE..CREATE_STREAM_FIXED_SIZE + nl]
    }

    /// Checked variant of `name()`: returns `None` instead of panicking
    /// when `name_len` doesn't fit in the buffer.
    #[inline(always)]
    pub fn try_name(&self) -> Option<&'a [u8]> {
        let nl = self.try_fixed()?.name_len.get() as usize;
        self.buf
            .get(CREATE_STREAM_FIXED_SIZE..CREATE_STREAM_FIXED_SIZE.checked_add(nl)?)
    }

    #[inline(always)]
    pub fn filter(&self) -> &'a [u8] {
        let nl = self.fixed().name_len.get() as usize;
        let fl = self.fixed().filter_len.get() as usize;
        let start = CREATE_STREAM_FIXED_SIZE + nl;
        &self.buf[start..start + fl]
    }

    /// Checked variant of `filter()`.
    #[inline(always)]
    pub fn try_filter(&self) -> Option<&'a [u8]> {
        let f = self.try_fixed()?;
        let nl = f.name_len.get() as usize;
        let fl = f.filter_len.get() as usize;
        let start = CREATE_STREAM_FIXED_SIZE.checked_add(nl)?;
        self.buf.get(start..start.checked_add(fl)?)
    }

    #[inline(always)]
    pub fn max_msgs(&self) -> u64 {
        self.fixed().max_msgs.get()
    }

    #[inline(always)]
    pub fn max_bytes(&self) -> u64 {
        self.fixed().max_bytes.get()
    }

    #[inline(always)]
    pub fn max_age_secs(&self) -> u64 {
        self.fixed().max_age_secs.get()
    }

    #[inline(always)]
    pub fn replicas(&self) -> u8 {
        self.fixed().replicas
    }

    #[inline(always)]
    pub fn journal_kind(&self) -> u8 {
        self.fixed().journal_kind
    }

    #[inline(always)]
    pub fn retention(&self) -> u8 {
        self.fixed().retention
    }

    #[inline(always)]
    pub fn discard(&self) -> u8 {
        self.fixed().discard
    }

    /// Per-stream idempotency window in milliseconds. `0` means the
    /// stream is NOT idempotent (legacy default — no dedup). A
    /// non-zero value activates the dedup window on the publish hot
    /// path. Recovery reads this back and calls
    /// `NameRegistry::set_stream_idempotency` to rebuild the per-stream
    /// state lost by a restart.
    #[inline(always)]
    pub fn idempotency_window_ms(&self) -> u32 {
        self.fixed().idempotency_window_ms.get()
    }
}

pub struct DeleteStreamView<'a> {
    buf: &'a [u8],
}

impl<'a> DeleteStreamView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    /// Fallible constructor: checks `buf.len() >= DELETE_STREAM_FIXED_SIZE`
    /// before wrapping it.
    #[inline(always)]
    pub fn try_new(buf: &'a [u8]) -> Option<Self> {
        if buf.len() < DELETE_STREAM_FIXED_SIZE {
            return None;
        }
        Some(Self { buf })
    }

    #[inline(always)]
    pub fn name(&self) -> &'a [u8] {
        let f = DeleteStreamFixed::ref_from_bytes(&self.buf[..DELETE_STREAM_FIXED_SIZE]).unwrap();
        let nl = f.name_len.get() as usize;
        &self.buf[DELETE_STREAM_FIXED_SIZE..DELETE_STREAM_FIXED_SIZE + nl]
    }

    /// Checked variant of `name()`: returns `None` instead of panicking
    /// on a truncated buffer.
    #[inline(always)]
    pub fn try_name(&self) -> Option<&'a [u8]> {
        let f = DeleteStreamFixed::ref_from_bytes(self.buf.get(..DELETE_STREAM_FIXED_SIZE)?).ok()?;
        let nl = f.name_len.get() as usize;
        self.buf
            .get(DELETE_STREAM_FIXED_SIZE..DELETE_STREAM_FIXED_SIZE.checked_add(nl)?)
    }
}

pub struct GetStreamView<'a> {
    buf: &'a [u8],
}

impl<'a> GetStreamView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    /// Fallible constructor: checks the buffer is large enough to hold
    /// a `GetStreamFixed` before wrapping it.
    #[inline(always)]
    pub fn try_new(buf: &'a [u8]) -> Option<Self> {
        if buf.len() < core::mem::size_of::<GetStreamFixed>() {
            return None;
        }
        Some(Self { buf })
    }

    #[inline(always)]
    pub fn name(&self) -> &'a [u8] {
        let f = GetStreamFixed::ref_from_bytes(&self.buf[..core::mem::size_of::<GetStreamFixed>()])
            .unwrap();
        let nl = f.name_len.get() as usize;
        &self.buf
            [core::mem::size_of::<GetStreamFixed>()..core::mem::size_of::<GetStreamFixed>() + nl]
    }

    /// Checked variant of `name()`: returns `None` instead of panicking
    /// on a truncated buffer.
    #[inline(always)]
    pub fn try_name(&self) -> Option<&'a [u8]> {
        let fixed_size = core::mem::size_of::<GetStreamFixed>();
        let f = GetStreamFixed::ref_from_bytes(self.buf.get(..fixed_size)?).ok()?;
        let nl = f.name_len.get() as usize;
        self.buf.get(fixed_size..fixed_size.checked_add(nl)?)
    }
}

pub struct PurgeStreamView<'a> {
    buf: &'a [u8],
}

impl<'a> PurgeStreamView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    /// Fallible constructor: checks `buf.len() >= PURGE_STREAM_FIXED_SIZE`
    /// before wrapping it.
    #[inline(always)]
    pub fn try_new(buf: &'a [u8]) -> Option<Self> {
        if buf.len() < PURGE_STREAM_FIXED_SIZE {
            return None;
        }
        Some(Self { buf })
    }

    #[inline(always)]
    pub fn name(&self) -> &'a [u8] {
        let f = PurgeStreamFixed::ref_from_bytes(&self.buf[..PURGE_STREAM_FIXED_SIZE]).unwrap();
        let nl = f.name_len.get() as usize;
        &self.buf[PURGE_STREAM_FIXED_SIZE..PURGE_STREAM_FIXED_SIZE + nl]
    }

    /// Checked variant of `name()`: returns `None` instead of panicking
    /// on a truncated buffer.
    #[inline(always)]
    pub fn try_name(&self) -> Option<&'a [u8]> {
        let f = PurgeStreamFixed::ref_from_bytes(self.buf.get(..PURGE_STREAM_FIXED_SIZE)?).ok()?;
        let nl = f.name_len.get() as usize;
        self.buf
            .get(PURGE_STREAM_FIXED_SIZE..PURGE_STREAM_FIXED_SIZE.checked_add(nl)?)
    }
}

pub struct DrainSubjectView<'a> {
    buf: &'a [u8],
}

impl<'a> DrainSubjectView<'a> {
    #[inline(always)]
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    /// Fallible constructor: checks `buf.len() >= DRAIN_SUBJECT_FIXED_SIZE`
    /// before wrapping it.
    #[inline(always)]
    pub fn try_new(buf: &'a [u8]) -> Option<Self> {
        if buf.len() < DRAIN_SUBJECT_FIXED_SIZE {
            return None;
        }
        Some(Self { buf })
    }

    #[inline(always)]
    fn fixed(&self) -> &DrainSubjectFixed {
        DrainSubjectFixed::ref_from_bytes(&self.buf[..DRAIN_SUBJECT_FIXED_SIZE]).unwrap()
    }

    #[inline(always)]
    fn try_fixed(&self) -> Option<&DrainSubjectFixed> {
        DrainSubjectFixed::ref_from_bytes(self.buf.get(..DRAIN_SUBJECT_FIXED_SIZE)?).ok()
    }

    #[inline(always)]
    pub fn name(&self) -> &'a [u8] {
        let nl = self.fixed().name_len.get() as usize;
        &self.buf[DRAIN_SUBJECT_FIXED_SIZE..DRAIN_SUBJECT_FIXED_SIZE + nl]
    }

    /// Checked variant of `name()`.
    #[inline(always)]
    pub fn try_name(&self) -> Option<&'a [u8]> {
        let nl = self.try_fixed()?.name_len.get() as usize;
        self.buf
            .get(DRAIN_SUBJECT_FIXED_SIZE..DRAIN_SUBJECT_FIXED_SIZE.checked_add(nl)?)
    }

    #[inline(always)]
    pub fn subject(&self) -> &'a [u8] {
        let nl = self.fixed().name_len.get() as usize;
        let sl = self.fixed().subj_len.get() as usize;
        let start = DRAIN_SUBJECT_FIXED_SIZE + nl;
        &self.buf[start..start + sl]
    }

    /// Checked variant of `subject()`.
    #[inline(always)]
    pub fn try_subject(&self) -> Option<&'a [u8]> {
        let f = self.try_fixed()?;
        let nl = f.name_len.get() as usize;
        let sl = f.subj_len.get() as usize;
        let start = DRAIN_SUBJECT_FIXED_SIZE.checked_add(nl)?;
        self.buf.get(start..start.checked_add(sl)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_stream_view_try_new_rejects_short_buffer() {
        let short = [0u8; CREATE_STREAM_FIXED_SIZE - 1];
        assert!(CreateStreamView::try_new(&short).is_none());
        assert!(CreateStreamView::try_new(&[0u8; CREATE_STREAM_FIXED_SIZE]).is_some());
    }

    #[test]
    fn create_stream_view_try_name_rejects_truncated_trailer() {
        let fixed = CreateStreamFixed {
            name_len: U16::new(50),
            filter_len: U16::new(0),
            max_msgs: U64::new(0),
            max_bytes: U64::new(0),
            max_age_secs: U64::new(0),
            replicas: 1,
            journal_kind: 0,
            retention: 0,
            discard: 0,
            idempotency_window_ms: U32::new(0),
            _pad: U32::new(0),
        };
        let buf = fixed.as_bytes().to_vec(); // no trailer bytes
        let view = CreateStreamView::try_new(&buf).unwrap();
        assert!(view.try_name().is_none());
    }

    #[test]
    fn delete_stream_view_try_new_rejects_short_buffer() {
        assert!(DeleteStreamView::try_new(&[0u8; DELETE_STREAM_FIXED_SIZE - 1]).is_none());
    }

    #[test]
    fn get_stream_view_try_new_rejects_short_buffer() {
        let fixed_size = core::mem::size_of::<GetStreamFixed>();
        assert!(GetStreamView::try_new(&vec![0u8; fixed_size - 1]).is_none());
    }

    #[test]
    fn purge_stream_view_try_new_rejects_short_buffer() {
        assert!(PurgeStreamView::try_new(&[0u8; PURGE_STREAM_FIXED_SIZE - 1]).is_none());
    }

    #[test]
    fn drain_subject_view_try_new_rejects_short_buffer() {
        assert!(DrainSubjectView::try_new(&[0u8; DRAIN_SUBJECT_FIXED_SIZE - 1]).is_none());
        let view = DrainSubjectView::try_new(&[0u8; DRAIN_SUBJECT_FIXED_SIZE]).unwrap();
        assert_eq!(view.try_name(), Some(&[][..]));
        assert_eq!(view.try_subject(), Some(&[][..]));
    }
}
