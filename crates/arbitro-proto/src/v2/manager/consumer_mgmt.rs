//! Consumer management frames (v2): Create / Delete / Get / ListConsumers.

use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::action::Action;
use crate::v2::header::{Header, HEADER_SIZE};

// ── CreateConsumer ─────────────────────────────────────────────────────
//
// Body (28 B fixed) + tail = [name][group][subject][subject_limits?].
//
//   name_len        u16
//   subj_len        u16
//   stream_id       u32
//   max_inflight    u16
//   ack_policy      u8
//   deliver_policy  u8
//   deliver_mode    u8
//   _pad            u8
//   group_len       u16
//   ack_wait_ms     u32
//   start_seq       u64
//
// Optional subject-limits trailer (right after [name][group][subject]):
//   count          u16
//   N × entry:
//     limit         u32     (max inflight for this pattern)
//     pattern_len   u16
//     pattern_bytes [u8; pattern_len]
//
// The trailer is **optional**: if `tail.len() == name + group + subject`,
// there are no per-subject limits and the consumer behaves as before.
// The trailer layout intentionally mirrors `wire::manager::CreateConsumerView`
// so persisting `&frame[HEADER_SIZE..]` produces a valid metadata-log entry
// (recovery decodes it via `CreateConsumerView::subject_limits`).
//
// Per-subject limits are only meaningful with `ack_policy == Explicit`;
// pairing them with `AckPolicy::None` is silently dropped by the server.

#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct CreateConsumerBody {
    pub name_len: U16,
    pub subj_len: U16,
    pub stream_id: U32,
    pub max_inflight: U16,
    pub ack_policy: u8,
    pub deliver_policy: u8,
    pub deliver_mode: u8,
    pub _pad: u8,
    pub group_len: U16,
    pub ack_wait_ms: U32,
    pub start_seq: U64,
}
pub const CREATE_CONSUMER_BODY_FIXED: usize = core::mem::size_of::<CreateConsumerBody>();
const _: () = assert!(CREATE_CONSUMER_BODY_FIXED == 28);

/// Per-subject inflight cap carried in the CreateConsumer tail.
#[derive(Debug, Clone, Copy)]
pub struct SubjectLimit<'a> {
    pub pattern: &'a [u8],
    pub limit: u32,
}

/// Header bytes per limit entry on the wire: `[limit u32][pattern_len u16]`.
pub const SUBJECT_LIMIT_HEADER_SIZE: usize = 4 + 2;
/// Length of the count prefix that opens the limits trailer.
pub const SUBJECT_LIMITS_COUNT_SIZE: usize = 2;

/// Total tail bytes consumed by `limits`, including the count prefix.
/// Returns 0 when `limits` is empty (no trailer is written).
#[inline]
pub fn subject_limits_tail_len(limits: &[SubjectLimit<'_>]) -> usize {
    if limits.is_empty() {
        return 0;
    }
    let mut total = SUBJECT_LIMITS_COUNT_SIZE;
    for l in limits {
        total += SUBJECT_LIMIT_HEADER_SIZE + l.pattern.len();
    }
    total
}

#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct CreateConsumerFrame {
    pub header: Header,
    pub body: CreateConsumerBody,
    pub tail: [u8],
}

impl CreateConsumerFrame {
    #[inline(always)]
    pub const fn wire_size(
        name_len: usize,
        group_len: usize,
        subj_len: usize,
        subject_limits_tail_len: usize,
    ) -> usize {
        HEADER_SIZE
            + CREATE_CONSUMER_BODY_FIXED
            + name_len
            + group_len
            + subj_len
            + subject_limits_tail_len
    }

    #[inline(always)]
    pub fn name(&self) -> &[u8] {
        let n = self.body.name_len.get() as usize;
        &self.tail[..n]
    }

    #[inline(always)]
    pub fn group(&self) -> &[u8] {
        let n = self.body.name_len.get() as usize;
        let g = self.body.group_len.get() as usize;
        &self.tail[n..n + g]
    }

    #[inline(always)]
    pub fn subject(&self) -> &[u8] {
        let n = self.body.name_len.get() as usize;
        let g = self.body.group_len.get() as usize;
        let s = self.body.subj_len.get() as usize;
        &self.tail[n + g..n + g + s]
    }

    /// Parse subject-limit entries from the optional tail trailer.
    ///
    /// Returns `Some(Vec::new())` when the trailer is absent (no extra
    /// bytes after `[name][group][subject]`). Returns `None` on a
    /// malformed/truncated trailer.
    pub fn subject_limits(&self) -> Option<Vec<(Vec<u8>, u32)>> {
        let n = self.body.name_len.get() as usize;
        let g = self.body.group_len.get() as usize;
        let s = self.body.subj_len.get() as usize;
        let trailer_start = n + g + s;

        // No trailer at all → empty list.
        if trailer_start == self.tail.len() {
            return Some(Vec::new());
        }
        // Must have at least a count prefix.
        if trailer_start + SUBJECT_LIMITS_COUNT_SIZE > self.tail.len() {
            return None;
        }

        let count =
            u16::from_le_bytes([self.tail[trailer_start], self.tail[trailer_start + 1]]) as usize;
        let mut cursor = trailer_start + SUBJECT_LIMITS_COUNT_SIZE;

        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            if cursor + SUBJECT_LIMIT_HEADER_SIZE > self.tail.len() {
                return None;
            }
            let limit = u32::from_le_bytes([
                self.tail[cursor],
                self.tail[cursor + 1],
                self.tail[cursor + 2],
                self.tail[cursor + 3],
            ]);
            let pattern_len =
                u16::from_le_bytes([self.tail[cursor + 4], self.tail[cursor + 5]]) as usize;
            cursor += SUBJECT_LIMIT_HEADER_SIZE;
            if cursor + pattern_len > self.tail.len() {
                return None;
            }
            let pattern = self.tail[cursor..cursor + pattern_len].to_vec();
            cursor += pattern_len;
            out.push((pattern, limit));
        }
        Some(out)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn encode_into<'a>(
        out: &'a mut [u8],
        seq: u64,
        stream_id: u32,
        name: &[u8],
        group: &[u8],
        subject: &[u8],
        max_inflight: u16,
        ack_policy: u8,
        deliver_policy: u8,
        deliver_mode: u8,
        ack_wait_ms: u32,
        start_seq: u64,
        subject_limits: &[SubjectLimit<'_>],
    ) -> &'a mut Self {
        let limits_tail = subject_limits_tail_len(subject_limits);
        debug_assert_eq!(
            out.len(),
            Self::wire_size(name.len(), group.len(), subject.len(), limits_tail),
        );
        let msg_len =
            (CREATE_CONSUMER_BODY_FIXED + name.len() + group.len() + subject.len() + limits_tail)
                as u32;
        let frame = Self::mut_from_bytes(out).expect("CreateConsumerFrame layout");
        frame.header = Header::new(Action::CreateConsumer.as_u16(), msg_len, seq);
        frame.body = CreateConsumerBody {
            name_len: U16::new(name.len() as u16),
            subj_len: U16::new(subject.len() as u16),
            stream_id: U32::new(stream_id),
            max_inflight: U16::new(max_inflight),
            ack_policy,
            deliver_policy,
            deliver_mode,
            _pad: 0,
            group_len: U16::new(group.len() as u16),
            ack_wait_ms: U32::new(ack_wait_ms),
            start_seq: U64::new(start_seq),
        };
        let n = name.len();
        let g = group.len();
        let s = subject.len();
        frame.tail[..n].copy_from_slice(name);
        frame.tail[n..n + g].copy_from_slice(group);
        frame.tail[n + g..n + g + s].copy_from_slice(subject);

        // Limits trailer (only written when non-empty). Layout mirrors
        // wire::manager::CreateConsumerView::subject_limits exactly so
        // the metadata log can replay the same bytes.
        if !subject_limits.is_empty() {
            let mut cursor = n + g + s;
            frame.tail[cursor..cursor + 2]
                .copy_from_slice(&(subject_limits.len() as u16).to_le_bytes());
            cursor += SUBJECT_LIMITS_COUNT_SIZE;
            for l in subject_limits {
                frame.tail[cursor..cursor + 4].copy_from_slice(&l.limit.to_le_bytes());
                frame.tail[cursor + 4..cursor + 6]
                    .copy_from_slice(&(l.pattern.len() as u16).to_le_bytes());
                cursor += SUBJECT_LIMIT_HEADER_SIZE;
                frame.tail[cursor..cursor + l.pattern.len()].copy_from_slice(l.pattern);
                cursor += l.pattern.len();
            }
        }
        frame
    }
}

// DeleteConsumer migrated to serde — see `v2::cold` module.

// ConsumerStats / ListConsumers migrated to serde — see `v2::cold` module.
// PauseConsumer / ResumeConsumer / GetConsumer / DeleteConsumer also migrated.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_consumer_roundtrip() {
        let size = CreateConsumerFrame::wire_size(4, 0, 5, 0);
        let mut buf = vec![0u8; size];
        CreateConsumerFrame::encode_into(
            &mut buf,
            1,
            7,
            b"name",
            b"",
            b"a.b.c",
            16,
            1,
            2,
            3,
            30_000,
            0,
            &[],
        );
        let f = CreateConsumerFrame::ref_from_bytes(&buf).unwrap();
        assert_eq!(f.header.action.get(), Action::CreateConsumer.as_u16());
        assert_eq!(f.name(), b"name");
        assert_eq!(f.group(), b"");
        assert_eq!(f.subject(), b"a.b.c");
        assert_eq!(f.body.stream_id.get(), 7);
        assert_eq!(f.body.max_inflight.get(), 16);
        // No trailer present → empty limits list.
        assert_eq!(f.subject_limits().unwrap(), Vec::<(Vec<u8>, u32)>::new());
        assert_eq!(f.as_bytes(), &buf[..]);
    }

    #[test]
    fn create_consumer_with_subject_limits_roundtrip() {
        let limits = [
            SubjectLimit {
                pattern: b"vip.>",
                limit: 10,
            },
            SubjectLimit {
                pattern: b"free.*",
                limit: 2,
            },
        ];
        let tail_len = subject_limits_tail_len(&limits);
        let size = CreateConsumerFrame::wire_size(4, 3, 5, tail_len);
        let mut buf = vec![0u8; size];
        CreateConsumerFrame::encode_into(
            &mut buf, 1, 7, b"name", b"grp", b"a.b.c", 16, 1, 2, 3, 30_000, 0, &limits,
        );
        let f = CreateConsumerFrame::ref_from_bytes(&buf).unwrap();
        assert_eq!(f.name(), b"name");
        assert_eq!(f.group(), b"grp");
        assert_eq!(f.subject(), b"a.b.c");
        let parsed = f.subject_limits().unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].0.as_slice(), b"vip.>");
        assert_eq!(parsed[0].1, 10);
        assert_eq!(parsed[1].0.as_slice(), b"free.*");
        assert_eq!(parsed[1].1, 2);
    }

    /// The wire trailer must be byte-for-byte compatible with the metadata
    /// view (`wire::manager::CreateConsumerView::subject_limits`) so the
    /// command log can replay raw wire bytes without translation.
    #[test]
    fn create_consumer_trailer_matches_metadata_view() {
        use crate::wire::manager::CreateConsumerView;

        let limits = [
            SubjectLimit {
                pattern: b"vip.>",
                limit: 10,
            },
            SubjectLimit {
                pattern: b"free.*",
                limit: 2,
            },
        ];
        let tail_len = subject_limits_tail_len(&limits);
        let size = CreateConsumerFrame::wire_size(4, 3, 5, tail_len);
        let mut buf = vec![0u8; size];
        CreateConsumerFrame::encode_into(
            &mut buf, 1, 7, b"name", b"grp", b"a.b.c", 16, 1, 2, 3, 30_000, 0, &limits,
        );

        // Metadata log stores `&frame[HEADER_SIZE..]` (body + tail).
        let metadata_body = &buf[HEADER_SIZE..];
        let cv = CreateConsumerView::new(metadata_body);
        let collected: Vec<(Vec<u8>, u32)> = cv
            .subject_limits()
            .map(|e| (e.pattern.to_vec(), e.limit))
            .collect();
        assert_eq!(collected.len(), 2);
        assert_eq!(collected[0].0, b"vip.>");
        assert_eq!(collected[0].1, 10);
        assert_eq!(collected[1].0, b"free.*");
        assert_eq!(collected[1].1, 2);
    }

    // delete_consumer / list_consumers / consumer_stats tests removed —
    // frames migrated to v2::cold and tested there.
}
