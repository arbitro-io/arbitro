//! Subject matching and overlap detection.
//!
//! Rules:
//! - `.` separates tokens
//! - `*` matches exactly one token
//! - `>` matches one or more tokens (must be last)
//!
//! All inputs are `&[u8]` — no UTF-8 assumption.

/// Check if `subject` matches `pattern`.
///
/// Hot path — called per message per consumer filter.
/// No allocations, single pass over both slices.
#[inline]
pub fn subject_matches(pattern: &[u8], subject: &[u8]) -> bool {
    let mut pat = pattern;
    let mut sub = subject;

    loop {
        // Find next token in pattern
        let (ptok, prest) = next_token(pat);
        let (stok, srest) = next_token(sub);

        match (ptok, stok) {
            // `>` matches everything remaining (must have at least one token)
            (b">", s) if !s.is_empty() => return true,
            // `*` matches exactly one token
            (b"*", s) if !s.is_empty() => {}
            // Literal must match exactly
            (p, s) if p == s && !p.is_empty() => {}
            // Both exhausted simultaneously = match
            (p, s) if p.is_empty() && s.is_empty() => return prest.is_empty() && srest.is_empty(),
            // Anything else = no match
            _ => return false,
        }

        pat = prest;
        sub = srest;
    }
}

/// Check if two patterns can match the same subject.
///
/// Cold path — called at CreateConsumer/CreateStream to validate invariants.
///
/// Conservative: returns `true` if overlap is possible.
/// Two patterns overlap if there exists any subject that matches both.
pub fn patterns_overlap(a: &[u8], b: &[u8]) -> bool {
    let mut pa = a;
    let mut pb = b;

    loop {
        let (ta, ra) = next_token(pa);
        let (tb, rb) = next_token(pb);

        match (ta, tb) {
            // Both exhausted = they matched all the way
            (t1, t2) if t1.is_empty() && t2.is_empty() => return ra.is_empty() && rb.is_empty(),
            // One exhausted, other has more tokens = no overlap
            // (unless the other is `>`)
            (t, _) if t.is_empty() => return false,
            (_, t) if t.is_empty() => return false,
            // `>` on either side = overlap (matches anything remaining)
            (b">", _) | (_, b">") => return true,
            // `*` matches any single token = could overlap
            (b"*", _) | (_, b"*") => {}
            // Both literals must match
            (la, lb) if la == lb => {}
            // Different literals = no overlap
            _ => return false,
        }

        pa = ra;
        pb = rb;
    }
}

/// Split `&[u8]` at the first `.`, returning (token, rest).
/// If no `.`, returns (input, empty).
#[inline(always)]
fn next_token(s: &[u8]) -> (&[u8], &[u8]) {
    match memchr_dot(s) {
        Some(i) => (&s[..i], &s[i + 1..]),
        None => (s, &[]),
    }
}

/// Find first `.` in slice. Inlined, no deps.
#[inline(always)]
fn memchr_dot(s: &[u8]) -> Option<usize> {
    s.iter().position(|&b| b == b'.')
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── subject_matches ─────────────────────────────────────────────────

    #[test]
    fn exact_match() {
        assert!(subject_matches(b"orders.created", b"orders.created"));
    }

    #[test]
    fn exact_no_match() {
        assert!(!subject_matches(b"orders.created", b"orders.updated"));
    }

    #[test]
    fn star_one_token() {
        assert!(subject_matches(b"orders.*", b"orders.created"));
        assert!(subject_matches(b"orders.*", b"orders.updated"));
    }

    #[test]
    fn star_not_multi() {
        assert!(!subject_matches(b"orders.*", b"orders.a.b"));
    }

    #[test]
    fn star_middle() {
        assert!(subject_matches(b"orders.*.done", b"orders.created.done"));
        assert!(!subject_matches(b"orders.*.done", b"orders.created.fail"));
    }

    #[test]
    fn gt_one_or_more() {
        assert!(subject_matches(b"orders.>", b"orders.created"));
        assert!(subject_matches(b"orders.>", b"orders.a.b.c"));
    }

    #[test]
    fn gt_needs_at_least_one() {
        assert!(!subject_matches(b"orders.>", b"orders"));
    }

    #[test]
    fn gt_no_match_different_prefix() {
        assert!(!subject_matches(b"orders.>", b"payments.created"));
    }

    #[test]
    fn single_token() {
        assert!(subject_matches(b"orders", b"orders"));
        assert!(!subject_matches(b"orders", b"payments"));
    }

    #[test]
    fn length_mismatch() {
        assert!(!subject_matches(b"orders.created", b"orders"));
        assert!(!subject_matches(b"orders", b"orders.created"));
    }

    #[test]
    fn four_level() {
        assert!(subject_matches(b"msg.qr.*.premium", b"msg.qr.user1.premium"));
        assert!(!subject_matches(b"msg.qr.*.premium", b"msg.qr.user1.standard"));
    }

    #[test]
    fn wildcard_all() {
        assert!(subject_matches(b">", b"orders.created"));
        assert!(subject_matches(b">", b"a"));
        assert!(subject_matches(b">", b"a.b.c.d"));
    }

    // ── patterns_overlap ────────────────────────────────────────────────

    #[test]
    fn identical_overlap() {
        assert!(patterns_overlap(b"orders.created", b"orders.created"));
    }

    #[test]
    fn disjoint_no_overlap() {
        assert!(!patterns_overlap(b"orders.>", b"payments.>"));
    }

    #[test]
    fn nested_overlap() {
        // orders.> and orders.created.> overlap on "orders.created.x"
        assert!(patterns_overlap(b"orders.>", b"orders.created.>"));
    }

    #[test]
    fn star_overlap() {
        assert!(patterns_overlap(b"orders.*", b"orders.created"));
    }

    #[test]
    fn star_star_overlap() {
        assert!(patterns_overlap(b"orders.*", b"orders.*"));
    }

    #[test]
    fn different_literals_no_overlap() {
        assert!(!patterns_overlap(b"orders.created", b"orders.updated"));
    }

    #[test]
    fn gt_vs_star() {
        assert!(patterns_overlap(b"orders.>", b"orders.*"));
    }

    #[test]
    fn length_mismatch_no_overlap() {
        assert!(!patterns_overlap(b"orders.created", b"orders.created.done"));
    }

    #[test]
    fn premium_freemium_no_overlap() {
        // These are the valid consumer filters
        assert!(!patterns_overlap(b"orders.premium.>", b"orders.freemium.>"));
    }
}
