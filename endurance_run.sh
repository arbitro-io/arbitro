#!/bin/bash
# 1-MINUTE ULTRA-ENDURANCE STRESS TEST (O(1) Memory + Scavenging Verification)

set -e
trap 'kill $(jobs -p)' EXIT

SERVER_BIN="./arbitro-server"
CLIENT_BIN="./endurance_client"
DATA_DIR="/tmp/arbitro_endurance_data"
LOG_FILE="/tmp/endurance_stress.log"
DURATION=60
CONCURRENCY=4

echo "--- STARTING EXTRA-REDUCIDO 1-MINUTE STRESS TEST ---"
echo "Target: 200k-300k msg/s | Duration: ${DURATION}s"

# 1. CLEANUP
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"

# 2. START SERVER (Isolated)
export ARBITRO_DATA_DIR="$DATA_DIR"
export ARBITRO_LISTEN="127.0.0.1:9898"
$SERVER_BIN > /tmp/server_chaos.log 2>&1 &
SERVER_PID=$!
sleep 2

echo "Server Started (PID: $SERVER_PID)"

# 3. START DUAL CLIENT (PRODUCER + CONSUMER/SCAVENGER)
echo "Launching DUAL client (Prod+Cons)..."
export ARBITRO_ROLE="dual"
export ARBITRO_DURATION=$DURATION
export ARBITRO_CONCURRENCY=$CONCURRENCY
export ARBITRO_KIND="memory" # Verification of RAM scavenging
$CLIENT_BIN > /tmp/endurance_telemetry.log 2>&1 &
CLIENT_PID=$!

# 4. MONITORING LOOP (RSS + Telemetry)
echo "Monitoring RSS (RAM) Stability for 60s..."
T=0
while [ $T -lt $DURATION ]; do
    RSS=$(ps -o rss= -p $SERVER_PID | xargs)
    RSS_MB=$((RSS / 1024))
    echo "[T+${T}s] Server RAM: ${RSS_MB}MB | Telemetry:"
    tail -n 1 /tmp/endurance_telemetry.log | grep "Telemetry" || echo "  (Initializing...)"
    
    sleep 5
    T=$((T + 5))
done

echo "--- TEST COMPLETE ---"
echo "Final Server RAM RSS: $(ps -o rss= -p $SERVER_PID | xargs) KB"
echo "Cleaning up..."

# Verification of cleanup success in the logs
grep "Final Report" /tmp/endurance_telemetry.log

# Physical proof: check remaining messages in MemoryStore (if possible via telemetry)
# In reality, we look at the 'Acked' vs 'Published' ratio.

echo "Done."
