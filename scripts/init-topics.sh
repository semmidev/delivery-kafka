#!/usr/bin/env bash
# =============================================================================
# init-topics.sh
# Buat semua Kafka topics dengan best practice settings untuk delivery app.
# Jalankan SETELAH cluster kafka sudah up: ./scripts/init-topics.sh
# =============================================================================
set -euo pipefail

BROKER="kafka-1:29092"
BOOTSTRAP="--bootstrap-server $BROKER"

# Tunggu Kafka siap
echo "⏳ Menunggu Kafka cluster siap..."
until docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh $BOOTSTRAP --list &>/dev/null; do
  sleep 2
  echo "   ...masih menunggu"
done
echo "✅ Kafka siap!"

# ─────────────────────────────────────────────────────────────────────────────
# Fungsi helper
# ─────────────────────────────────────────────────────────────────────────────
create_topic() {
  local name=$1
  local partitions=$2
  local replication=$3
  local retention_ms=$4
  local extra=${5:-""}

  echo ""
  echo "📦 Membuat topic: $name"
  echo "   partitions=$partitions | replication=$replication | retention=${retention_ms}ms"

  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh $BOOTSTRAP \
    --create \
    --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor "$replication" \
    --config retention.ms="$retention_ms" \
    --config min.insync.replicas=2 \
    --config compression.type=lz4 \
    $extra

  echo "   ✓ OK"
}

# ─────────────────────────────────────────────────────────────────────────────
# Topic naming convention: {domain}.{entity}.{event-type}
#   - lowercase, dot-separated, no spaces
#   - domain   : area bisnis (delivery, order, driver)
#   - entity   : objek (location, order, notification)
#   - event    : kata kerja past-tense (updated, created, changed)
# ─────────────────────────────────────────────────────────────────────────────

ONE_HOUR_MS=3600000
ONE_DAY_MS=86400000
SEVEN_DAYS_MS=604800000
THIRTY_DAYS_MS=2592000000

# --- Driver live location ---
# High-throughput, short retention. Partitioned by driver_id untuk ordering.
# 12 partitions: bisa handle banyak driver secara paralel.
create_topic \
  "delivery-driver-location-updated" \
  12 \
  3 \
  $ONE_DAY_MS \
  "--config segment.ms=$ONE_HOUR_MS"

# --- Order status changes ---
# Medium-throughput, longer retention (audit trail).
# Partitioned by order_id sehingga semua event 1 order masuk ke 1 partition → ordering terjaga.
create_topic \
  "delivery-order-status-changed" \
  6 \
  3 \
  $THIRTY_DAYS_MS

# --- Order created events ---
create_topic \
  "delivery-order-created" \
  6 \
  3 \
  $THIRTY_DAYS_MS

# --- Driver assigned ke order ---
create_topic \
  "delivery-order-driver-assigned" \
  6 \
  3 \
  $THIRTY_DAYS_MS

# --- Dead letter queue: pesan gagal diproses consumer ---
create_topic \
  "delivery-dlq-failed-events" \
  3 \
  3 \
  $SEVEN_DAYS_MS

echo ""
echo "═══════════════════════════════════════════════"
echo "✅ Semua topics berhasil dibuat!"
echo ""
echo "📋 Daftar topics:"
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh $BOOTSTRAP --list
echo "═══════════════════════════════════════════════"
