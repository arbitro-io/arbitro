#!/bin/bash

# Configuration
DURATION=30
CONCURRENCY=2  # Total clients = 2 processes * 2 internal tasks = 4
LISTEN_ADDR="127.0.0.1:9899"
DATA_DIR="/tmp/arbitro_endurance_data"

echo "--- ORCHESTRATED ENDURANCE TEST (PROCESOS SEPARADOS) ---"

# Step 1: Build binaries
echo "Compilando servidor y cliente..."
cargo build --release --bin arbitro-server --bin endurance_client

# Step 2: Cleanup and Trap
cleanup() {
    echo -e "\nLimpiando procesos..."
    pkill -9 -f arbitro-server
    pkill -9 -f endurance_client
    rm -rf "$DATA_DIR"
    echo "Hecho."
}

# Trap to ensure cleanup on exit or error
trap cleanup EXIT

# Step 3: Setup environment
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"

# Step 4: Start Server
echo "Iniciando Servidor en $LISTEN_ADDR..."
export ARBITRO_LISTEN="$LISTEN_ADDR"
export ARBITRO_DATA_DIR="$DATA_DIR"
./target/release/arbitro-server > /tmp/server.log 2>&1 &
SERVER_PID=$!

sleep 2

# Step 5: Start Clients
echo "Lanzando $CONCURRENCY procesos de cliente..."
for i in $(seq 1 $CONCURRENCY); do
    export ARBITRO_ADDR="$LISTEN_ADDR"
    export ARBITRO_DURATION="$DURATION"
    export ARBITRO_CONCURRENCY=2 # each client process spawns 2 tasks
    ./target/release/endurance_client > "/tmp/client_$i.log" 2>&1 &
done

# Step 6: Monitor
echo "Monitoreando durante ${DURATION}s..."
for i in $(seq 1 6); do
    sleep 5
    echo "--- T+$(expr $i \* 5)s Telemetry Snapshot ---"
    grep "Local Msgs" /tmp/client_*.log | tail -n $CONCURRENCY
done

echo "Test Completado."
