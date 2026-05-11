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
            (p, s) if p.is_empty() && s.is_empty() => {
                return prest.is_empty() && srest.is_empty()
            }
            _ => return false,
        }

        pat = prest;
        sub = srest;
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

    #[test]
    fn four_level() {
        assert!(subject_matches(b"msg.qr.*.premium", b"msg.qr.user1.premium"));
        assert!(!subject_matches(b"msg.qr.*.premium", b"msg.qr.user1.standard"));
    }
}
