//! Name and subject validation — pure functions, no allocations.
//!
//! Invariants:
//!   - Identifiers (stream, consumer, group): [a-zA-Z0-9_-], max 255 bytes.
//!   - Subjects: [a-zA-Z0-9_.*>-], tokens separated by '.', max 255 bytes.
//!     Wildcards: '*' matches one token, '>' matches rest (must be last).

/// Maximum length for any identifier (stream name, consumer name, group name).
pub const MAX_NAME_LEN: usize = 255;

/// Maximum length for a subject.
pub const MAX_SUBJECT_LEN: usize = 255;

/// Validation error — cheap to return, no heap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidateError {
    Empty,
    TooLong,
    InvalidChar(u8),
}

/// Validate an identifier: stream name, consumer name, or group name.
///
/// Rules:
///   - Not empty
///   - Max 255 bytes
///   - Only `[a-zA-Z0-9_-]`
///   - No dots, no spaces, no wildcards
#[inline]
pub fn validate_name(name: &[u8]) -> Result<(), ValidateError> {
    if name.is_empty() {
        return Err(ValidateError::Empty);
    }
    if name.len() > MAX_NAME_LEN {
        return Err(ValidateError::TooLong);
    }
    for &b in name {
        if !matches!(b, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_' | b'-') {
            return Err(ValidateError::InvalidChar(b));
        }
    }
    Ok(())
}

/// Validate a subject.
///
/// Rules:
///   - Not empty
///   - Max 255 bytes
///   - Only `[a-zA-Z0-9_.*>-]`
///   - Tokens separated by `.`
///   - `*` must be a full token (not mixed: `foo*` is invalid)
///   - `>` must be a full token AND the last token
#[inline]
pub fn validate_subject(subject: &[u8]) -> Result<(), ValidateError> {
    if subject.is_empty() {
        return Err(ValidateError::Empty);
    }
    if subject.len() > MAX_SUBJECT_LEN {
        return Err(ValidateError::TooLong);
    }
    for &b in subject {
        if !matches!(b, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_' | b'-' | b'.' | b'*' | b'>') {
            return Err(ValidateError::InvalidChar(b));
        }
    }

    // Token-level rules
    let mut tokens = subject.split(|&b| b == b'.');
    let mut last_token = b"" as &[u8];

    while let Some(token) = tokens.next() {
        if token.is_empty() {
            // Empty token: ".." or leading/trailing dot
            return Err(ValidateError::InvalidChar(b'.'));
        }
        // '*' must be alone in its token
        if token.contains(&b'*') && token.len() > 1 {
            return Err(ValidateError::InvalidChar(b'*'));
        }
        // '>' must be alone in its token
        if token.contains(&b'>') && token.len() > 1 {
            return Err(ValidateError::InvalidChar(b'>'));
        }
        last_token = token;
    }

    // '>' must be the last token
    if subject.contains(&b'>') && last_token != b">" {
        return Err(ValidateError::InvalidChar(b'>'));
    }

    Ok(())
}

/// Check if two subject patterns overlap — i.e., there exists a concrete
/// subject that would match both patterns.
///
/// Used to prevent two streams from capturing the same messages.
///
/// Examples:
///   - `"orders.>"` vs `"orders.new.>"` → overlap (both match `orders.new.x`)
///   - `"orders.>"` vs `"payments.>"` → no overlap
///   - `"orders.*"` vs `"orders.created"` → overlap
///   - `"*"` vs `"orders"` → overlap
///   - `">"` vs anything → overlap
#[inline]
pub fn subjects_overlap(a: &[u8], b: &[u8]) -> bool {
    let a_tokens: Vec<&[u8]> = a.split(|&c| c == b'.').collect();
    let b_tokens: Vec<&[u8]> = b.split(|&c| c == b'.').collect();
    tokens_overlap(&a_tokens, 0, &b_tokens, 0)
}

fn tokens_overlap(a: &[&[u8]], ai: usize, b: &[&[u8]], bi: usize) -> bool {
    // Both exhausted — full match
    if ai == a.len() && bi == b.len() {
        return true;
    }

    // One exhausted, other still has tokens — no match (unless '>' already consumed)
    if ai == a.len() || bi == b.len() {
        return false;
    }

    let at = a[ai];
    let bt = b[bi];

    // '>' matches everything remaining
    if at == b">" || bt == b">" {
        return true;
    }

    // '*' matches any single token
    if at == b"*" || bt == b"*" {
        return tokens_overlap(a, ai + 1, b, bi + 1);
    }

    // Literal match
    if at == bt {
        return tokens_overlap(a, ai + 1, b, bi + 1);
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Name validation ──────────────────────────────────────────────

    #[test]
    fn valid_names() {
        assert!(validate_name(b"orders").is_ok());
        assert!(validate_name(b"user_events").is_ok());
        assert!(validate_name(b"my-stream-v2").is_ok());
        assert!(validate_name(b"A").is_ok());
        assert!(validate_name(b"abc123").is_ok());
    }

    #[test]
    fn name_rejects_empty() {
        assert_eq!(validate_name(b""), Err(ValidateError::Empty));
    }

    #[test]
    fn name_rejects_dots() {
        assert_eq!(validate_name(b"orders.created"), Err(ValidateError::InvalidChar(b'.')));
    }

    #[test]
    fn name_rejects_spaces() {
        assert_eq!(validate_name(b"my stream"), Err(ValidateError::InvalidChar(b' ')));
    }

    #[test]
    fn name_rejects_wildcards() {
        assert_eq!(validate_name(b"orders*"), Err(ValidateError::InvalidChar(b'*')));
        assert_eq!(validate_name(b"orders>"), Err(ValidateError::InvalidChar(b'>')));
    }

    #[test]
    fn name_rejects_too_long() {
        let long = vec![b'a'; 256];
        assert_eq!(validate_name(&long), Err(ValidateError::TooLong));
        // 255 is ok
        let max = vec![b'a'; 255];
        assert!(validate_name(&max).is_ok());
    }

    // ── Subject validation ───────────────────────────────────────────

    #[test]
    fn valid_subjects() {
        assert!(validate_subject(b"orders.created").is_ok());
        assert!(validate_subject(b"orders.*").is_ok());
        assert!(validate_subject(b"orders.>").is_ok());
        assert!(validate_subject(b"a.b.c.d").is_ok());
        assert!(validate_subject(b"*").is_ok());
        assert!(validate_subject(b">").is_ok());
        assert!(validate_subject(b"orders.*.confirmed").is_ok());
    }

    #[test]
    fn subject_rejects_empty() {
        assert_eq!(validate_subject(b""), Err(ValidateError::Empty));
    }

    #[test]
    fn subject_rejects_mixed_wildcard() {
        assert_eq!(validate_subject(b"orders.foo*"), Err(ValidateError::InvalidChar(b'*')));
        assert_eq!(validate_subject(b"orders.foo>"), Err(ValidateError::InvalidChar(b'>')));
    }

    #[test]
    fn subject_rejects_gt_not_last() {
        assert_eq!(validate_subject(b">.orders"), Err(ValidateError::InvalidChar(b'>')));
    }

    #[test]
    fn subject_rejects_empty_token() {
        assert_eq!(validate_subject(b"orders..created"), Err(ValidateError::InvalidChar(b'.')));
        assert_eq!(validate_subject(b".orders"), Err(ValidateError::InvalidChar(b'.')));
        assert_eq!(validate_subject(b"orders."), Err(ValidateError::InvalidChar(b'.')));
    }

    #[test]
    fn subject_rejects_spaces() {
        assert_eq!(validate_subject(b"orders. created"), Err(ValidateError::InvalidChar(b' ')));
    }

    #[test]
    fn subject_rejects_too_long() {
        let long = vec![b'a'; 256];
        assert_eq!(validate_subject(&long), Err(ValidateError::TooLong));
    }

    // ── Overlap detection ────────────────────────────────────────────

    #[test]
    fn overlap_gt_nested() {
        // "orders.>" and "orders.new.>" both match "orders.new.x"
        assert!(subjects_overlap(b"orders.>", b"orders.new.>"));
    }

    #[test]
    fn overlap_gt_same() {
        assert!(subjects_overlap(b"orders.>", b"orders.>"));
    }

    #[test]
    fn no_overlap_different_prefix() {
        assert!(!subjects_overlap(b"orders.>", b"payments.>"));
    }

    #[test]
    fn overlap_star_literal() {
        // "orders.*" matches "orders.created"
        assert!(subjects_overlap(b"orders.*", b"orders.created"));
    }

    #[test]
    fn overlap_star_star() {
        assert!(subjects_overlap(b"orders.*", b"orders.*"));
    }

    #[test]
    fn no_overlap_star_deeper() {
        // "orders.*" only matches one token, "orders.new.x" has two after orders
        assert!(!subjects_overlap(b"orders.*", b"orders.new.x"));
    }

    #[test]
    fn overlap_gt_catches_all() {
        assert!(subjects_overlap(b">", b"anything.at.all"));
        assert!(subjects_overlap(b"anything", b">"));
    }

    #[test]
    fn overlap_exact_match() {
        assert!(subjects_overlap(b"orders.created", b"orders.created"));
    }

    #[test]
    fn no_overlap_exact_different() {
        assert!(!subjects_overlap(b"orders.created", b"orders.updated"));
    }

    #[test]
    fn overlap_star_vs_gt() {
        // "*" matches one token, ">" matches one or more — both match "orders"
        assert!(subjects_overlap(b"*", b">"));
    }

    #[test]
    fn no_overlap_different_depth() {
        assert!(!subjects_overlap(b"a.b", b"a.b.c"));
    }
}
