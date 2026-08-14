//! Subject matching — zero-allocation pattern evaluation.
//!
//! Rules:
//! - `.` separates tokens
//! - `*` matches exactly one token
//! - `>` matches one or more tokens (must be last)
//!
//! All inputs are `&[u8]` — no UTF-8 assumption.

/// Split `&[u8]` at the first `.`, returning (token, rest).
/// If no `.`, returns (input, empty).
#[inline(always)]
pub fn next_token(s: &[u8]) -> (&[u8], &[u8]) {
    match s.iter().position(|&b| b == b'.') {
        Some(i) => (&s[..i], &s[i + 1..]),
        None => (s, &[]),
    }
}

/// Check if `subject` matches `pattern`. Zero-allocation, single pass.
#[inline]
pub fn subject_matches(pattern: &[u8], subject: &[u8]) -> bool {
    let mut pat = pattern;
    let mut sub = subject;

    loop {
        let (ptok, prest) = next_token(pat);
        let (stok, srest) = next_token(sub);

        match (ptok, stok) {
            (b">", s) if !s.is_empty() => return true,
            (b"*", s) if !s.is_empty() => {}
            (p, s) if p == s && !p.is_empty() => {}
            (p, s) if p.is_empty() && s.is_empty() => return prest.is_empty() && srest.is_empty(),
            _ => return false,
        }

        pat = prest;
        sub = srest;
    }
}

/// Does `wide` match every subject `narrow` matches? Zero-allocation.
#[inline]
pub fn subject_covers(wide: &[u8], narrow: &[u8]) -> bool {
    let mut w = wide;
    let mut n = narrow;

    loop {
        let (wtok, wrest) = next_token(w);
        let (ntok, nrest) = next_token(n);

        match (wtok, ntok) {
            (b">", t) if !t.is_empty() => return true,
            // `*` spans one token, `>` spans one or more — never covered.
            (_, b">") => return false,
            (b"*", t) if !t.is_empty() => {}
            (a, b) if a == b && !a.is_empty() => {}
            (a, b) if a.is_empty() && b.is_empty() => return wrest.is_empty() && nrest.is_empty(),
            _ => return false,
        }

        w = wrest;
        n = nrest;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn bare_gt() {
        assert!(subject_matches(b">", b"anything"));
        assert!(subject_matches(b">", b"a.b.c.d"));
    }

    // ── subject_covers ───────────────────────────────────────────────

    #[test]
    fn covers_nested_under_gt() {
        assert!(subject_covers(b"orders.>", b"orders.premium.>"));
        assert!(subject_covers(b"orders.>", b"orders.basic.1"));
        assert!(subject_covers(b">", b"anything.at.all"));
    }

    #[test]
    fn covers_itself() {
        assert!(subject_covers(b"orders.premium.>", b"orders.premium.>"));
        assert!(subject_covers(b"orders.created", b"orders.created"));
        assert!(subject_covers(b"orders.*", b"orders.*"));
    }

    #[test]
    fn narrower_does_not_cover_wider() {
        assert!(!subject_covers(b"orders.premium.>", b"orders.>"));
        assert!(!subject_covers(b"orders.*", b"orders.>"));
        assert!(!subject_covers(b"orders.created", b"orders.*"));
    }

    #[test]
    fn disjoint_never_covers() {
        assert!(!subject_covers(b"orders.premium.>", b"orders.basic.>"));
        assert!(!subject_covers(b"orders.>", b"payments.>"));
    }

    #[test]
    fn star_spans_exactly_one_token() {
        assert!(subject_covers(b"orders.*", b"orders.created"));
        assert!(!subject_covers(b"orders.*", b"orders.a.b"));
        assert!(subject_covers(b"*.orders.>", b"eu.orders.new"));
    }

    /// The contract, stated as a property: whatever `narrow` accepts,
    /// `wide` must accept too.
    #[test]
    fn covers_agrees_with_subject_matches() {
        let subjects: &[&[u8]] = &[
            b"orders",
            b"orders.basic.1",
            b"orders.premium.1",
            b"orders.premium.eu.2",
            b"payments.new",
            b"eu.orders.new",
        ];
        let pats: &[&[u8]] = &[
            b">",
            b"orders.>",
            b"orders.*",
            b"orders.premium.>",
            b"orders.basic.>",
            b"orders.created",
            b"*.orders.>",
        ];

        for wide in pats {
            for narrow in pats {
                if !subject_covers(wide, narrow) {
                    continue;
                }
                for s in subjects {
                    if subject_matches(narrow, s) {
                        assert!(
                            subject_matches(wide, s),
                            "{:?} claims to cover {:?} but rejects {:?}",
                            String::from_utf8_lossy(wide),
                            String::from_utf8_lossy(narrow),
                            String::from_utf8_lossy(s),
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn four_level() {
        assert!(subject_matches(
            b"msg.qr.*.premium",
            b"msg.qr.user1.premium"
        ));
        assert!(!subject_matches(
            b"msg.qr.*.premium",
            b"msg.qr.user1.standard"
        ));
    }
}
