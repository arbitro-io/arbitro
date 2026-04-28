#!/bin/bash
set -u
cd /tmp/arbitro
rm -f sw_*.log
for c in 1 4 8 16 32 60 100 200; do
    BENCH_CONNS=$c timeout 60 ./mpsc_overhead-103e2b7996a44253 --bench > sw_${c}.log 2>&1
    echo "done conns=$c"
done
echo "----"
printf '%-7s %-12s %-14s %-14s %-14s %-14s\n' CONNS tokio_mpsc kit::Mpmc sharded_kit ChunkedPoC kit_speedup
for c in 1 4 8 16 32 60 100 200; do
    f=sw_${c}.log
    s8=$(grep 'S8 tokio mpsc shared' $f | awk '{print $5}')
    s9=$(grep 'S9 kit::Mpmc shared' $f  | awk '{print $5}')
    s10=$(grep 'S10 sharded' $f         | awk '{print $4}')
    s11=$(grep 'S11 ChunkedMpmc' $f     | awk '{print $4}')
    spd=$(awk -v a="$s8" -v b="$s9" 'BEGIN{ if(b>0) printf "%.2fx", a/b; else print "n/a" }')
    printf '%-7s %-12s %-14s %-14s %-14s %-14s\n' "$c" "${s8}ms" "${s9}ms" "${s10}ms" "${s11}ms" "$spd"
done
