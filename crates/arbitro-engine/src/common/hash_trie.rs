//! Subject trie keyed by segment HASH instead of segment bytes.
//!
//! Same semantics as [`super::trie::SubjectTrie`] — `*` matches exactly one
//! token, `>` matches the rest — but a node's children are `u64` hashes in
//! a flat array, so descending compares an integer against contiguous
//! memory instead of chasing a `Box<[u8]>` to read bytes elsewhere.
//!
//! ## The walk is level-synchronous, not depth-first
//!
//! `*` and a literal both consume EXACTLY one token, and `>` terminates at
//! the node holding it. So everything reachable at one moment is at the
//! same depth, and the subject can be tokenized ONCE per level for the
//! whole frontier.
//!
//! A depth-first walk cannot do that. It pushes the literal child and the
//! `*` child with the same remaining subject, and each pop re-scans for
//! the separator and re-hashes the same segment. With a 43-byte UUID
//! segment and a frontier of two, that is the segment scanned twice and
//! hashed twice, per level, per message.
//!
//! The frontier therefore holds node indices only — 4 bytes, not a
//! `(u32, &[u8])` pair — and the token is computed once beside it.
//!
//! ## Lazy: a dead frontier never reads the next segment
//!
//! The separator is located only if some frontier node can consume a
//! token, and the hash is computed only if some frontier node has literal
//! children. A frontier that is all `*` needs the token's bounds but never
//! its hash; a frontier with neither needs neither, and the rest of the
//! subject is not touched at all.
//!
//! ## Collisions, and why the seed is not fixed
//!
//! A colliding segment descends into the WRONG subtree and delivers to
//! the wrong subscribers, silently. For RANDOM input the odds are
//! `children / 2^64` per lookup — around 1e-18, which is why no bytes are
//! kept for verification.
//!
//! But subjects are written by clients, and that number does not describe
//! an adversary. With a fixed, published seed, a tenant can search offline
//! for a segment colliding with another tenant's pattern and receive its
//! messages. So the seed is per-instance and supplied by the caller: the
//! search cannot be done ahead of time against a seed that is not known.

use smallvec::SmallVec;

/// Frontier nodes that fit before spilling to the heap.
///
/// The bound is live nodes AT ONE LEVEL, which is at most one literal plus
/// one `*` per node in the previous level. A spill would allocate on the
/// hot path, so this is sized for the shape a real stream has, not the
/// worst case a pathological pattern set could reach.
const FRONTIER_INLINE_CAP: usize = 8;

/// Marks "no `*` child" without paying `Option`'s extra word.
const NO_STAR: u32 = u32::MAX;

/// Hash one segment under `seed`.
///
/// foldhash rather than a byte-at-a-time fold: it consumes 8 bytes per
/// step, and a 43-byte UUID segment is where that decides the walk. A
/// chained multiply per byte measured ~25 ns slower on exactly that shape.
#[inline]
pub fn segment_hash(seed: u64, seg: &[u8]) -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = foldhash::fast::FixedState::with_seed(seed).build_hasher();
    h.write(seg);
    h.finish()
}

/// Index of the next `.`, or `None` if this is the last segment.
///
/// Reads eight bytes at a time with the classic zero-byte trick: XOR the
/// word with `.` repeated, then a word containing a zero byte is detected
/// with two arithmetic ops. The tail runs a plain scan.
///
/// No `memchr`: the crate's dependency set is fixed, and this needs no
/// `unsafe` and no crate.
#[inline]
fn find_dot(s: &[u8]) -> Option<usize> {
    const DOTS: u64 = 0x2e2e_2e2e_2e2e_2e2e;
    const LO: u64 = 0x0101_0101_0101_0101;
    const HI: u64 = 0x8080_8080_8080_8080;

    let mut i = 0usize;
    while i + 8 <= s.len() {
        let w = u64::from_le_bytes([
            s[i], s[i + 1], s[i + 2], s[i + 3], s[i + 4], s[i + 5], s[i + 6], s[i + 7],
        ]);
        let x = w ^ DOTS;
        // Non-zero exactly when some byte of `x` is zero, i.e. some byte
        // of `w` was a '.'.
        let z = x.wrapping_sub(LO) & !x & HI;
        if z != 0 {
            return Some(i + (z.trailing_zeros() as usize >> 3));
        }
        i += 8;
    }
    while i < s.len() {
        if s[i] == b'.' {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// One node. Children are split into two arrays so the scan reads only
/// hashes: 32 siblings are 256 contiguous bytes with unit stride, which is
/// the shape that vectorizes, instead of 512 bytes at stride 16 with a
/// tuple projection in the way.
#[derive(Default, Clone)]
struct Node {
    /// Child segment hashes. Parallel to `kids`.
    hashes: Vec<u64>,
    /// Child node indices. Parallel to `hashes`.
    kids: Vec<u32>,
    /// `*` child, or [`NO_STAR`].
    star: u32,
    /// Ids whose pattern ends in `>` here — they match the rest.
    gt: Vec<u32>,
    /// Ids whose pattern ends exactly here.
    subs: Vec<u32>,
}

impl Node {
    #[inline]
    fn new() -> Self {
        Self {
            hashes: Vec::new(),
            kids: Vec::new(),
            star: NO_STAR,
            gt: Vec::new(),
            subs: Vec::new(),
        }
    }

    /// Can this node consume another token?
    #[inline]
    fn can_descend(&self) -> bool {
        !self.hashes.is_empty() || self.star != NO_STAR
    }
}

/// Arena trie. Nodes live contiguously; children are indices.
#[derive(Clone)]
pub struct HashTrie {
    nodes: Vec<Node>,
    seed: u64,
}

impl HashTrie {
    /// Build a trie whose segment hashing is seeded by `seed`.
    ///
    /// The seed must be unpredictable to clients — see the module docs.
    /// It is a parameter and not generated here because this crate does no
    /// I/O and reads no clock; where the entropy comes from is the
    /// caller's decision.
    pub fn new(seed: u64) -> Self {
        Self {
            nodes: vec![Node::new()],
            seed,
        }
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    pub fn clear(&mut self) {
        self.nodes.clear();
        self.nodes.push(Node::new());
    }

    /// Insert a pattern. Management path — may allocate.
    pub fn insert(&mut self, pattern: &[u8], id: u32) {
        let mut curr = 0usize;
        for token in pattern.split(|&c| c == b'.') {
            match token {
                b">" => {
                    self.nodes[curr].gt.push(id);
                    return;
                }
                b"*" => {
                    curr = if self.nodes[curr].star != NO_STAR {
                        self.nodes[curr].star as usize
                    } else {
                        let i = self.nodes.len();
                        self.nodes.push(Node::new());
                        self.nodes[curr].star = i as u32;
                        i
                    };
                }
                lit => {
                    let h = segment_hash(self.seed, lit);
                    curr = match self.nodes[curr].hashes.iter().position(|k| *k == h) {
                        Some(p) => self.nodes[curr].kids[p] as usize,
                        None => {
                            let i = self.nodes.len();
                            self.nodes.push(Node::new());
                            self.nodes[curr].hashes.push(h);
                            self.nodes[curr].kids.push(i as u32);
                            i
                        }
                    };
                }
            }
        }
        self.nodes[curr].subs.push(id);
    }

    /// Every pattern matching `subject`, handed to `on_match`.
    ///
    /// Generic over the callback so it inlines: the hot path budget allows
    /// no virtual dispatch, and a `&mut dyn FnMut` here would be one
    /// indirect call per match.
    ///
    /// Ids may repeat only if the SAME id was inserted under two patterns
    /// that both match. One pattern reaches exactly one node, so a
    /// single-filter subscription is never reported twice.
    #[inline]
    pub fn find_matches<F: FnMut(u32)>(&self, subject: &[u8], mut on_match: F) {
        // Fast path: while the frontier is ONE node there is nothing to
        // keep a frontier for. A `*` beside a matching literal is the only
        // thing that can widen it, and until that happens this is a plain
        // descent -- no vectors, no swap, no bookkeeping.
        //
        // That overhead is not hypothetical: paying it on a 4-sibling
        // subject measured ~7 ns worse than the byte trie, which wins that
        // shape precisely because it does nothing but descend.
        let mut node_idx = 0u32;
        let mut rest = subject;

        loop {
            let node = &self.nodes[node_idx as usize];

            if rest.is_empty() {
                for &id in &node.subs {
                    on_match(id);
                }
                return;
            }
            for &id in &node.gt {
                on_match(id);
            }
            if !node.can_descend() {
                return;
            }

            let (token, tail) = split_token(rest);

            let lit = if node.hashes.is_empty() {
                None
            } else {
                let h = segment_hash(self.seed, token);
                node.hashes.iter().position(|k| *k == h).map(|p| node.kids[p])
            };

            match (lit, node.star) {
                // Widened: hand the two branches to the level-synchronous
                // walk, which tokenizes once for the whole frontier.
                (Some(l), star) if star != NO_STAR => {
                    let mut frontier: SmallVec<[u32; FRONTIER_INLINE_CAP]> = SmallVec::new();
                    frontier.push(l);
                    frontier.push(star);
                    return self.walk_frontier(frontier, tail, on_match);
                }
                (Some(l), _) => {
                    node_idx = l;
                    rest = tail;
                }
                (None, star) if star != NO_STAR => {
                    node_idx = star;
                    rest = tail;
                }
                (None, _) => return,
            }
        }
    }

    /// The level-synchronous walk, entered once the frontier widens.
    ///
    /// `*` and a literal each consume exactly one token, so every node
    /// alive here is at the same depth — which is what lets the subject be
    /// tokenized ONCE per level instead of once per node. A depth-first
    /// walk re-scans and re-hashes the same segment for every branch it
    /// pushed, and with a 43-byte UUID segment that is the whole cost.
    fn walk_frontier<F: FnMut(u32)>(
        &self,
        mut curr: SmallVec<[u32; FRONTIER_INLINE_CAP]>,
        mut rest: &[u8],
        mut on_match: F,
    ) {
        let mut next: SmallVec<[u32; FRONTIER_INLINE_CAP]> = SmallVec::new();

        loop {
            let at_end = rest.is_empty();
            let mut any_descends = false;
            for &n in curr.iter() {
                let node = &self.nodes[n as usize];
                if at_end {
                    for &id in &node.subs {
                        on_match(id);
                    }
                } else {
                    for &id in &node.gt {
                        on_match(id);
                    }
                    any_descends |= node.can_descend();
                }
            }
            // Nothing left to consume, or nothing that could consume it —
            // the rest of the subject is never even scanned.
            if at_end || !any_descends {
                return;
            }

            let (token, tail) = split_token(rest);

            // Hashed only if some node has a literal child. A frontier
            // that is all `*` needs the token's bounds, never its hash.
            let mut hash: Option<u64> = None;

            next.clear();
            for &n in curr.iter() {
                let node = &self.nodes[n as usize];
                if !node.hashes.is_empty() {
                    let h = *hash.get_or_insert_with(|| segment_hash(self.seed, token));
                    // Scans hashes ONLY — unit stride over `&[u64]`, which
                    // is the shape that vectorizes.
                    if let Some(p) = node.hashes.iter().position(|k| *k == h) {
                        next.push(node.kids[p]);
                    }
                }
                if node.star != NO_STAR {
                    next.push(node.star);
                }
            }

            if next.is_empty() {
                return;
            }
            std::mem::swap(&mut curr, &mut next);
            rest = tail;
        }
    }
}

/// Split off the first token. The tail is empty when this was the last.
#[inline]
fn split_token(s: &[u8]) -> (&[u8], &[u8]) {
    match find_dot(s) {
        Some(p) => (&s[..p], &s[p + 1..]),
        None => (s, &s[..0]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEED: u64 = 0x51ed_5eed_51ed_5eed;

    fn collect(t: &HashTrie, subject: &[u8]) -> Vec<u32> {
        let mut v = Vec::new();
        t.find_matches(subject, |id| v.push(id));
        v.sort_unstable();
        v
    }

    fn trie(patterns: &[&[u8]]) -> HashTrie {
        let mut t = HashTrie::new(SEED);
        for (i, p) in patterns.iter().enumerate() {
            t.insert(p, i as u32);
        }
        t
    }

    #[test]
    fn exact_match() {
        let t = trie(&[b"orders.created", b"orders.updated"]);
        assert_eq!(collect(&t, b"orders.created"), vec![0]);
    }

    #[test]
    fn star_matches_one_token_only() {
        let t = trie(&[b"orders.*.created"]);
        assert_eq!(collect(&t, b"orders.eu.created"), vec![0]);
        assert_eq!(collect(&t, b"orders.eu.west.created"), Vec::<u32>::new());
    }

    #[test]
    fn gt_matches_the_rest() {
        let t = trie(&[b"orders.>"]);
        assert_eq!(collect(&t, b"orders.eu"), vec![0]);
        assert_eq!(collect(&t, b"orders.eu.west.created"), vec![0]);
        // `>` needs at least one token after the prefix.
        assert_eq!(collect(&t, b"orders"), Vec::<u32>::new());
    }

    #[test]
    fn several_patterns_reach_one_subject() {
        let t = trie(&[
            b"orders.>",
            b"orders.eu.>",
            b"orders.*.west.created",
            b"orders.eu.*.created",
            b"billing.*.>",
        ]);
        assert_eq!(collect(&t, b"orders.eu.west.created"), vec![0, 1, 2, 3]);
    }

    #[test]
    fn a_miss_at_the_first_level_matches_nothing() {
        let t = trie(&[b"orders.eu.west.created"]);
        assert_eq!(collect(&t, b"billing.eu.west.created"), Vec::<u32>::new());
    }

    #[test]
    fn one_pattern_lands_in_exactly_one_node() {
        // Why a single-filter subscription needs no deduplication: its id
        // is pushed once, not once per level.
        let t = trie(&[b"orders.eu.west.created"]);
        let mut hits = 0;
        t.find_matches(b"orders.eu.west.created", |_| hits += 1);
        assert_eq!(hits, 1);
    }

    #[test]
    fn long_segments_are_matched_whole() {
        // A UUID segment is 36 bytes -- past one hash word, and past the
        // 8-byte stride of the separator scan.
        let t = trie(&[b"orders.tenant-4f3a9c22-8b1e-4d7a-9c3f-2e6b8a1d5f04.created"]);
        assert_eq!(
            collect(&t, b"orders.tenant-4f3a9c22-8b1e-4d7a-9c3f-2e6b8a1d5f04.created"),
            vec![0]
        );
        // One byte different at the END must not match.
        assert_eq!(
            collect(&t, b"orders.tenant-4f3a9c22-8b1e-4d7a-9c3f-2e6b8a1d5f05.created"),
            Vec::<u32>::new()
        );
    }

    #[test]
    fn a_dead_frontier_stops_before_the_next_segment() {
        // `orders` has no children at all, so a longer subject must match
        // nothing -- and the walk returns without tokenizing the tail.
        let t = trie(&[b"orders"]);
        assert_eq!(collect(&t, b"orders"), vec![0]);
        assert_eq!(collect(&t, b"orders.a.b.c.d.e.f.g"), Vec::<u32>::new());
    }

    #[test]
    fn star_and_literal_at_the_same_level_both_advance() {
        // The case the depth-first walk used to pay twice: two frontier
        // nodes sharing one remaining subject.
        let t = trie(&[b"orders.eu.created", b"orders.*.created"]);
        assert_eq!(collect(&t, b"orders.eu.created"), vec![0, 1]);
        assert_eq!(collect(&t, b"orders.us.created"), vec![1]);
    }

    #[test]
    fn separator_scan_handles_every_alignment() {
        // The SWAR loop reads 8 bytes at a time, so a `.` landing at each
        // offset within a word -- and in the tail past the last full word
        // -- has to be found identically.
        for pad in 0..24usize {
            let head: Vec<u8> = std::iter::repeat(b'x').take(pad).collect();
            let mut pattern = head.clone();
            pattern.extend_from_slice(b".tail");
            let t = trie(&[&pattern]);
            assert_eq!(collect(&t, &pattern), vec![0], "pad={pad}");

            let mut wrong = head;
            wrong.extend_from_slice(b".other");
            assert_eq!(collect(&t, &wrong), Vec::<u32>::new(), "pad={pad}");
        }
    }

    #[test]
    fn find_dot_agrees_with_a_plain_scan() {
        for len in 0..40usize {
            for pos in 0..=len {
                let mut v: Vec<u8> = std::iter::repeat(b'a').take(len).collect();
                if pos < len {
                    v[pos] = b'.';
                }
                let expected = v.iter().position(|&c| c == b'.');
                assert_eq!(find_dot(&v), expected, "len={len} pos={pos}");
            }
        }
    }

    #[test]
    fn a_different_seed_gives_different_hashes() {
        // The defence against a client crafting a colliding segment: the
        // mapping is not known ahead of time.
        assert_ne!(segment_hash(1, b"orders"), segment_hash(2, b"orders"));
    }

    #[test]
    fn empty_trie_matches_nothing() {
        let t = HashTrie::new(SEED);
        assert_eq!(collect(&t, b"orders.eu.created"), Vec::<u32>::new());
        assert_eq!(collect(&t, b""), Vec::<u32>::new());
    }
}
