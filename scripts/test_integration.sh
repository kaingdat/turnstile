#!/usr/bin/env bash
set -euo pipefail

CONTAINER_NAME="turnstile-test-kafka"
KAFKA_IMAGE="confluentinc/cp-kafka:7.5.0"
CLUSTER_ID="MkU3OEVBNTcwNTJENDM2Qk"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cleanup() {
    echo "Stopping Kafka container..."
    docker rm -f "$CONTAINER_NAME" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

docker rm -f "$CONTAINER_NAME" 2>/dev/null || true

echo "Starting Kafka (KRaft mode)..."
docker run -d \
    --name "$CONTAINER_NAME" \
    --hostname kafka \
    -p 9092:9092 \
    -e KAFKA_NODE_ID=1 \
    -e KAFKA_PROCESS_ROLES=broker,controller \
    -e "KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:29093" \
    -e "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,CONTROLLER:PLAINTEXT" \
    -e "KAFKA_LISTENERS=PLAINTEXT://kafka:29092,PLAINTEXT_HOST://0.0.0.0:9092,CONTROLLER://kafka:29093" \
    -e "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092" \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
    -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=true \
    -e "KAFKA_LOG_DIRS=/var/lib/kafka/data" \
    -e "CLUSTER_ID=${CLUSTER_ID}" \
    "$KAFKA_IMAGE" \
    bash -c "/etc/confluent/docker/configure && kafka-storage format -t \$CLUSTER_ID -c /etc/kafka/kafka.properties --ignore-formatted && /etc/confluent/docker/ensure && exec /etc/confluent/docker/launch"

echo "Waiting for Kafka to be ready..."
MAX_RETRIES=30
RETRY_INTERVAL=5

for i in $(seq 1 "$MAX_RETRIES"); do
    if docker exec "$CONTAINER_NAME" kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1; then
        echo "Kafka is ready (${i} attempts, $((i * RETRY_INTERVAL))s elapsed)"
        break
    fi
    if [ "$i" -eq "$MAX_RETRIES" ]; then
        echo "ERROR: Kafka failed to start within $((MAX_RETRIES * RETRY_INTERVAL))s"
        echo "--- Container logs ---"
        docker logs "$CONTAINER_NAME"
        exit 1
    fi
    echo "  Waiting... attempt ${i}/${MAX_RETRIES}"
    sleep "$RETRY_INTERVAL"
done

COVERAGE="${1:-}"

echo "Running integration tests..."
cd "$PROJECT_ROOT"

if [ "$COVERAGE" = "coverage" ]; then
    go test -tags=integration -v -timeout=20m -race \
        -coverprofile=coverage_integration.out \
        -covermode=atomic \
        -coverpkg=github.com/kaingdat/turnstile \
        ./tests/...
else
    go test -tags=integration -v -timeout=20m -race ./tests/...
fi
