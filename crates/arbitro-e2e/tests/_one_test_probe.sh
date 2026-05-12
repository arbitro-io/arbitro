#!/usr/bin/env bash
# Repeat a single test N times with --test-threads=1 and report flake rate.
RUNS="${1:-10}"
TEST="${2:-resubscribe_continues_from_cursor}"
pass=0
fail=0
for i in $(seq 1 "$RUNS"); do
    r=$(cargo test --quiet -p arbitro-e2e --test drain_invariants "$TEST" -- --test-threads=1 2>&1 | grep -E "^test result")
    if echo "$r" | grep -q "ok\."; then
        pass=$((pass+1))
    else
        fail=$((fail+1))
    fi
    echo "run $i: $r"
done
echo ""
echo "=== $pass pass / $fail fail of $RUNS ==="
