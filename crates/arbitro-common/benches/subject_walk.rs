//! Resolving a four-level subject: by hash, or by raw bytes.
//!
//! The engine's trie compares raw segment bytes with a linear scan per
//! level (`TrieNode::literals: Vec<(Box<[u8]>, u32)>`). The alternative is
//! to hash each segment and probe a map per level. This measures both over
//! the SAME tree shape and the same tokenizer, so the difference is the
//! comparison and nothing else.
//!
//! Also measured, for scale: hashing the whole subject once, which is what
//! the exact-match path already does and which costs one probe total
//! instead of one per level.
//!
//! `width` is how many children a node has -- how many distinct segments
//! exist at that level. That is what a byte scan pays and a hash does not.

use std::collections::HashMap;
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

type FS = foldhash::fast::FixedState;

/// Real broker subjects, not toy ones. A tenant or entity id is a UUID --
/// 36 characters -- so a realistic subject is ~70 bytes, not 22, and one
/// of its segments is three times longer than the whole short subject.
///
/// That is the case where hashing and byte-comparison can swap places: a
/// hash must read every byte of a 36-char segment, while a comparison can
/// stop at the first one that differs.
const SHORT: &[u8] = b"orders.eu.west.created";
const UUID: &[u8] = b"orders.eu-west-1.tenant-4f3a9c22-8b1e-4d7a-9c3f-2e6b8a1d5f04.created";
const DEEP: &[u8] =
    b"iot.factory-3.line-7.sensor.a1b2c3d4-e5f6-7890-abcd-ef1234567890.telemetry";

const SUBJECT: &[u8] = SHORT;
const LEVELS: usize = 4;

#[inline]
fn hash32(b: &[u8]) -> u32 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = FS::default().build_hasher();
    h.write(b);
    h.finish() as u32
}

/// Split on '.' without allocating -- the same shape as the trie's
/// `next_token`.
#[inline]
fn segments<'a>(s: &'a [u8], out: &mut [&'a [u8]; LEVELS]) -> usize {
    let mut n = 0;
    let mut start = 0;
    for i in 0..=s.len() {
        if i == s.len() || s[i] == b'.' {
            if n < LEVELS {
                out[n] = &s[start..i];
                n += 1;
            }
            start = i + 1;
        }
    }
    n
}

/// One level of a byte trie: segment bytes -> child, linear scan.
type ByteLevel = Vec<(Box<[u8]>, u32)>;
/// One level of a hash trie: segment hash -> child.
type HashLevel = HashMap<u32, u32, FS>;

/// Build both trees with `width` children per level, the real subject
/// sitting LAST at every level -- the worst case for a scan, and the case
/// a hash does not care about.
fn build(width: usize) -> (Vec<ByteLevel>, Vec<HashLevel>) {
    let mut seg = [&b""[..]; LEVELS];
    let n = segments(SUBJECT, &mut seg);
    assert_eq!(n, LEVELS);

    let mut bytes = Vec::with_capacity(LEVELS);
    let mut hashes = Vec::with_capacity(LEVELS);
    for (lvl, real) in seg.iter().enumerate().take(LEVELS) {
        let mut bl: ByteLevel = Vec::with_capacity(width);
        let mut hl: HashLevel = HashMap::with_capacity_and_hasher(width, FS::default());
        for k in 0..width.saturating_sub(1) {
            let filler = format!("lvl{lvl}seg{k}").into_bytes();
            bl.push((filler.clone().into_boxed_slice(), k as u32));
            hl.insert(hash32(&filler), k as u32);
        }
        bl.push((Box::from(*real), width as u32));
        hl.insert(hash32(real), width as u32);
        bytes.push(bl);
        hashes.push(hl);
    }
    (bytes, hashes)
}

/// Hash and tokenize each real subject, so the effect of length shows.
fn real_subjects(c: &mut Criterion) {
    let mut g = c.benchmark_group("real");
    for (name, subj) in [("short_22B", SHORT), ("uuid_68B", UUID), ("deep_74B", DEEP)] {
        g.bench_function(BenchmarkId::new("hash_whole", name), |b| {
            b.iter(|| black_box(hash32(black_box(subj))))
        });
        g.bench_function(BenchmarkId::new("scan_bytes", name), |b| {
            b.iter(|| {
                let mut acc = 0u32;
                for &c in black_box(subj) {
                    acc = acc.wrapping_add(c as u32);
                }
                black_box(acc)
            })
        });
        g.bench_function(BenchmarkId::new("split_std", name), |b| {
            b.iter(|| {
                let mut n = 0usize;
                for part in black_box(subj).split(|&c| c == b'.') {
                    n += part.len();
                }
                black_box(n)
            })
        });
        // Hash every segment, which is what a per-level hash trie pays.
        g.bench_function(BenchmarkId::new("hash_each_segment", name), |b| {
            b.iter(|| {
                let mut acc = 0u32;
                for part in black_box(subj).split(|&c| c == b'.') {
                    acc ^= hash32(part);
                }
                black_box(acc)
            })
        });
        // Compare every segment against a sibling that differs at byte 0 --
        // the early-exit case a hash cannot have.
        g.bench_function(BenchmarkId::new("cmp_each_segment_miss", name), |b| {
            let other = b"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";
            b.iter(|| {
                let mut n = 0u32;
                for part in black_box(subj).split(|&c| c == b'.') {
                    n += (part == &other[..part.len().min(other.len())]) as u32;
                }
                black_box(n)
            })
        });
        // ...and against itself: the hit case, which must read every byte.
        g.bench_function(BenchmarkId::new("cmp_each_segment_hit", name), |b| {
            let parts: Vec<&[u8]> = subj.split(|&c| c == b'.').collect();
            b.iter(|| {
                let mut n = 0u32;
                for (i, part) in black_box(subj).split(|&c| c == b'.').enumerate() {
                    n += (part == parts[i]) as u32;
                }
                black_box(n)
            })
        });
    }
    g.finish();
}

/// ONE segment, isolated -- no splitting, no descent.
///
/// The per-segment numbers above include the tokenizer; these do not, so
/// the cost of the comparison itself can be read directly.
fn one_segment(c: &mut Criterion) {
    let mut g = c.benchmark_group("segment");
    let cases: [(&str, &[u8]); 4] = [
        ("6B_orders", b"orders"),
        ("10B_eu-west-1", b"eu-west-1"),
        ("36B_uuid", b"4f3a9c22-8b1e-4d7a-9c3f-2e6b8a1d5f04"),
        ("43B_tenant_uuid", b"tenant-4f3a9c22-8b1e-4d7a-9c3f-2e6b8a1d5f04"),
    ];
    for (name, seg) in cases {
        // A sibling that differs at byte 0 -- the scan exits immediately.
        let miss_first: Vec<u8> = std::iter::once(b'z').chain(seg[1..].iter().copied()).collect();
        // A sibling identical until the last byte -- the scan reads it all.
        let mut miss_last = seg.to_vec();
        *miss_last.last_mut().unwrap() = b'z';

        g.bench_function(BenchmarkId::new("hash", name), |b| {
            b.iter(|| black_box(hash32(black_box(seg))))
        });
        g.bench_function(BenchmarkId::new("cmp_equal", name), |b| {
            b.iter(|| black_box(black_box(seg) == seg))
        });
        g.bench_function(BenchmarkId::new("cmp_differs_at_0", name), |b| {
            b.iter(|| black_box(black_box(seg) == &miss_first[..]))
        });
        g.bench_function(BenchmarkId::new("cmp_differs_at_end", name), |b| {
            b.iter(|| black_box(black_box(seg) == &miss_last[..]))
        });
        g.bench_function(BenchmarkId::new("scan_bytes", name), |b| {
            b.iter(|| {
                let mut acc = 0u32;
                for &c in black_box(seg) {
                    acc = acc.wrapping_add(c as u32);
                }
                black_box(acc)
            })
        });
    }
    g.finish();
}

/// Looking a subject up in a map: keyed by its hash, or by its bytes.
///
/// The byte-keyed map does it in ONE call -- it hashes the bytes itself
/// and then compares the stored key. The hash-keyed map needs the hash
/// first, so its honest cost is hash + probe, and that is what is measured.
///
/// `identity` is the third shape: a map whose key is already a hash and
/// whose hasher returns it unchanged, so the value is never hashed twice.
fn map_lookup(c: &mut Criterion) {
    use std::hash::{BuildHasherDefault, Hasher};

    #[derive(Default)]
    struct Ident(u64);
    impl Hasher for Ident {
        fn finish(&self) -> u64 {
            self.0
        }
        fn write_u32(&mut self, v: u32) {
            self.0 = v as u64;
        }
        fn write(&mut self, _: &[u8]) {
            unreachable!()
        }
    }

    let mut g = c.benchmark_group("map_lookup");
    for (name, subj) in [("short_22B", SHORT), ("uuid_68B", UUID), ("deep_74B", DEEP)] {
        // 1000 entries so the probe is realistic, not a one-bucket table.
        let mut by_hash: HashMap<u32, u32, FS> = HashMap::with_hasher(FS::default());
        let mut by_bytes: HashMap<Box<[u8]>, u32, FS> = HashMap::with_hasher(FS::default());
        let mut by_ident: HashMap<u32, u32, BuildHasherDefault<Ident>> = HashMap::default();
        for k in 0..1000u32 {
            let filler = format!("filler.subject.number.{k}").into_bytes();
            by_hash.insert(hash32(&filler), k);
            by_ident.insert(hash32(&filler), k);
            by_bytes.insert(filler.into_boxed_slice(), k);
        }
        by_hash.insert(hash32(subj), 7);
        by_ident.insert(hash32(subj), 7);
        by_bytes.insert(Box::from(subj), 7);

        g.bench_function(BenchmarkId::new("bytes_key", name), |b| {
            b.iter(|| black_box(by_bytes.get(black_box(subj)).copied()))
        });
        g.bench_function(BenchmarkId::new("hash_then_probe", name), |b| {
            b.iter(|| {
                let h = hash32(black_box(subj));
                black_box(by_hash.get(&h).copied())
            })
        });
        g.bench_function(BenchmarkId::new("hash_then_probe_identity", name), |b| {
            b.iter(|| {
                let h = hash32(black_box(subj));
                black_box(by_ident.get(&h).copied())
            })
        });
        // The probe alone, hash already in hand -- what a cached subject
        // hash would cost.
        let pre = hash32(subj);
        g.bench_function(BenchmarkId::new("probe_only", name), |b| {
            b.iter(|| black_box(by_hash.get(&black_box(pre)).copied()))
        });
    }
    g.finish();
}

fn walk(c: &mut Criterion) {
    let mut g = c.benchmark_group("subject");

    // The floor: touch every byte once and do nothing else.
    g.bench_function("scan_bytes_only", |b| {
        b.iter(|| {
            let mut acc = 0u32;
            for &c in black_box(SUBJECT) {
                acc = acc.wrapping_add(c as u32);
            }
            black_box(acc)
        })
    });

    // Same walk, but only counting the separators -- what tokenizing
    // fundamentally has to do.
    g.bench_function("scan_count_dots", |b| {
        b.iter(|| {
            let mut n = 0u32;
            for &c in black_box(SUBJECT) {
                n += (c == b'.') as u32;
            }
            black_box(n)
        })
    });

    // The same thing with `memchr`-style search instead of a byte loop.
    g.bench_function("split_via_iterator", |b| {
        b.iter(|| {
            let mut n = 0usize;
            let mut last = &b""[..];
            for part in black_box(SUBJECT).split(|&c| c == b'.') {
                n += 1;
                last = part;
            }
            black_box((n, last))
        })
    });

    // Tokenizing alone, so the walks below can be read net of it.
    g.bench_function("split_only", |b| {
        b.iter(|| {
            let mut seg = [&b""[..]; LEVELS];
            let n = segments(black_box(SUBJECT), &mut seg);
            black_box((n, seg[3]))
        })
    });

    // What the exact-match path does: hash the whole subject, one probe.
    g.bench_function("hash_whole_subject", |b| {
        b.iter(|| black_box(hash32(black_box(SUBJECT))))
    });

    for &width in &[4usize, 16, 64] {
        let (bytes, hashes) = build(width);

        g.bench_function(BenchmarkId::new("walk_bytes", width), |b| {
            b.iter(|| {
                let mut seg = [&b""[..]; LEVELS];
                segments(black_box(SUBJECT), &mut seg);
                let mut child = 0u32;
                for (lvl, s) in seg.iter().enumerate() {
                    let mut found = None;
                    for (name, c) in bytes[lvl].iter() {
                        if &**name == *s {
                            found = Some(*c);
                            break;
                        }
                    }
                    child = match found {
                        Some(c) => c,
                        None => break,
                    };
                }
                black_box(child)
            })
        });

        g.bench_function(BenchmarkId::new("walk_hashed", width), |b| {
            b.iter(|| {
                let mut seg = [&b""[..]; LEVELS];
                segments(black_box(SUBJECT), &mut seg);
                let mut child = 0u32;
                for (lvl, s) in seg.iter().enumerate() {
                    child = match hashes[lvl].get(&hash32(s)) {
                        Some(c) => *c,
                        None => break,
                    };
                }
                black_box(child)
            })
        });
    }
    g.finish();
}

criterion_group!(benches, walk, real_subjects, one_segment, map_lookup);
criterion_main!(benches);
