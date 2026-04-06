use std::collections::HashMap;
use std::time::Duration;
use criterion::{criterion_group, criterion_main, Criterion, Throughput, black_box};
use arbitro_common::subject::subject_matches;

// ── Linear Matcher ──────────────────────────────────────────────

struct LinearMatcher {
    patterns: Vec<Vec<u8>>,
}

impl LinearMatcher {
    fn new(patterns: &[&[u8]]) -> Self {
        Self {
            patterns: patterns.iter().map(|s| s.to_vec()).collect(),
        }
    }

    #[inline]
    fn match_count(&self, subject: &[u8]) -> usize {
        let mut count = 0;
        for pat in &self.patterns {
            if subject_matches(pat, subject) {
                count += 1;
            }
        }
        count
    }
}

// ── Trie Matcher (Precomputed Tree) ───────────────────────────

#[derive(Default)]
struct Node {
    literals: HashMap<Vec<u8>, usize>,
    wildcard_star: Option<usize>,
    wildcard_gt: Option<Vec<u32>>,
    subs: Vec<u32>,
}

struct TrieMatcher {
    nodes: Vec<Node>,
}

impl TrieMatcher {
    fn new(patterns: &[&[u8]]) -> Self {
        let mut nodes = vec![Node::default()];
        for (id, &pat) in patterns.iter().enumerate() {
            Self::insert(&mut nodes, 0, pat, id as u32);
        }
        Self { nodes }
    }

    fn insert(nodes: &mut Vec<Node>, node_idx: usize, pattern: &[u8], sub_id: u32) {
        if pattern.is_empty() {
            nodes[node_idx].subs.push(sub_id);
            return;
        }

        let (token, rest) = next_token(pattern);
        match token {
            b">" => {
                if nodes[node_idx].wildcard_gt.is_none() {
                    nodes[node_idx].wildcard_gt = Some(Vec::new());
                }
                nodes[node_idx].wildcard_gt.as_mut().unwrap().push(sub_id);
            }
            b"*" => {
                let star_idx = if let Some(idx) = nodes[node_idx].wildcard_star {
                    idx
                } else {
                    let new_idx = nodes.len();
                    nodes.push(Node::default());
                    nodes[node_idx].wildcard_star = Some(new_idx);
                    new_idx
                };
                Self::insert(nodes, star_idx, rest, sub_id);
            }
            _ => {
                let literal_idx = if let Some(&idx) = nodes[node_idx].literals.get(token) {
                    idx
                } else {
                    let new_idx = nodes.len();
                    nodes.push(Node::default());
                    nodes[node_idx].literals.insert(token.to_vec(), new_idx);
                    new_idx
                };
                Self::insert(nodes, literal_idx, rest, sub_id);
            }
        }
    }

    #[inline]
    fn match_count(&self, subject: &[u8]) -> usize {
        let mut count = 0;
        // Optimization: Use a fixed-size stack for wildcard branching if depth is small
        self.recursive_match(0, subject, &mut count);
        count
    }

    fn recursive_match(&self, node_idx: usize, subject: &[u8], count: &mut usize) {
        let node = &self.nodes[node_idx];

        if let Some(ref gt_subs) = node.wildcard_gt {
            if !subject.is_empty() {
                *count += gt_subs.len();
            }
        }

        if subject.is_empty() {
            *count += node.subs.len();
            return;
        }

        let (token, rest) = next_token(subject);

        if let Some(&child_idx) = node.literals.get(token) {
            self.recursive_match(child_idx, rest, count);
        }

        if let Some(star_idx) = node.wildcard_star {
            self.recursive_match(star_idx, rest, count);
        }
    }
}

// ── Iterative Trie Matcher ────────────────────────────────────

struct IterativeTrieMatcher {
    nodes: Vec<Node>,
}

impl IterativeTrieMatcher {
    fn new(patterns: &[&[u8]]) -> Self {
        let mut nodes = vec![Node::default()];
        for (id, &pat) in patterns.iter().enumerate() {
            TrieMatcher::insert(&mut nodes, 0, pat, id as u32);
        }
        Self { nodes }
    }

    #[inline]
    fn match_count(&self, subject: &[u8]) -> usize {
        let mut count = 0;
        // Optimization: Use a fixed-size stack on the stack, not the heap
        let mut stack = [(0usize, subject); 16];
        let mut stack_ptr = 0;
        
        stack[stack_ptr] = (0, subject);
        stack_ptr += 1;

        while stack_ptr > 0 {
            stack_ptr -= 1;
            let (node_idx, sub) = stack[stack_ptr];
            let node = &self.nodes[node_idx];

            if let Some(ref gt_subs) = node.wildcard_gt {
                if !sub.is_empty() {
                    count += gt_subs.len();
                }
            }

            if sub.is_empty() {
                count += node.subs.len();
                continue;
            }

            let (token, rest) = next_token(sub);

            // Literal path
            if let Some(&child_idx) = node.literals.get(token) {
                if stack_ptr < 16 {
                    stack[stack_ptr] = (child_idx, rest);
                    stack_ptr += 1;
                }
            }

            // Star path
            if let Some(star_idx) = node.wildcard_star {
                if stack_ptr < 16 {
                    stack[stack_ptr] = (star_idx, rest);
                    stack_ptr += 1;
                }
            }
        }
        count
    }
}

fn next_token(s: &[u8]) -> (&[u8], &[u8]) {
    match s.iter().position(|&b| b == b'.') {
        Some(i) => (&s[..i], &s[i + 1..]),
        None => (s, &[]),
    }
}

// ── Benchmark ───────────────────────────────────────────────────

fn bench_subject_match(c: &mut Criterion) {
    let subjects = [
        b"orders.created".as_slice(),
        b"orders.premium.meta".as_slice(),
        b"payments.updated.done".as_slice(),
        b"msg.user1.chat.private".as_slice(),
        b"logs.errors.critical.disk".as_slice(),
    ];

    for &n in &[1, 8, 32, 128, 512, 2048] {
        let mut group = c.benchmark_group(format!("match_scaling_{n}_subs"));
        group.throughput(Throughput::Elements(1));
        group.measurement_time(Duration::from_secs(3));

        // Generate N patterns
        let patterns: Vec<Vec<u8>> = (0..n)
            .map(|i| {
                if i % 10 == 0 {
                    b"orders.>".to_vec()
                } else if i % 7 == 0 {
                    b"*.created".to_vec()
                } else if i % 5 == 0 {
                    b"msg.user1.>".to_vec()
                } else {
                    format!("orders.user{i}.created").into_bytes()
                }
            })
            .collect();
        
        let pat_refs: Vec<&[u8]> = patterns.iter().map(|v| v.as_slice()).collect();

        let linear = LinearMatcher::new(&pat_refs);
        let trie = TrieMatcher::new(&pat_refs);
        let iterative = IterativeTrieMatcher::new(&pat_refs);

        // Verify correctness
        for &s in &subjects {
            let c1 = linear.match_count(s);
            let c2 = trie.match_count(s);
            let c3 = iterative.match_count(s);
            assert_eq!(c1, c2, "Trie mismatch for subject {:?}, n={}", String::from_utf8_lossy(s), n);
            assert_eq!(c1, c3, "Iterative mismatch for subject {:?}, n={}", String::from_utf8_lossy(s), n);
        }

        group.bench_function("linear", |b| {
            b.iter(|| {
                for &s in &subjects {
                    black_box(linear.match_count(s));
                }
            });
        });

        group.bench_function("trie_recursive", |b| {
            b.iter(|| {
                for &s in &subjects {
                    black_box(trie.match_count(s));
                }
            });
        });

        group.bench_function("trie_iterative", |b| {
            b.iter(|| {
                for &s in &subjects {
                    black_box(iterative.match_count(s));
                }
            });
        });

        group.finish();
    }
}

criterion_group!(benches, bench_subject_match);
criterion_main!(benches);
