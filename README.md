# Delivery App — Kafka Live Tracking

Studi kasus Kafka untuk delivery app: real-time driver location + order status tracking.

## Stack
- **Kafka 3.9** — KRaft mode (tanpa ZooKeeper), 3 broker
- **Go** + [franz-go](https://github.com/twmb/franz-go)
- **Kafka UI** di `http://localhost:8080`

## Struktur

```
delivery-kafka/
├── docker-compose.yml       # 3-broker KRaft cluster + Kafka UI
├── scripts/
│   └── init-topics.sh       # Buat semua topics dengan best practice config
├── producer/
│   └── main.go              # Simulasi banyak driver (location + order status)
├── consumer/
│   └── main.go              # Consumer + REST API live tracking
└── Makefile                 # Shortcut semua perintah
```

## Cara Pakai

### 1. Jalankan Kafka Cluster
```bash
make up
```
Tunggu ~15 detik sampai cluster siap.

### 2. Buat Topics
```bash
make init-topics
```

### 3. Terminal 1 — Producer
```bash
make deps       # download dependencies (sekali saja)
make producer
```

### 4. Terminal 2 — Consumer + API
```bash
make consumer
```

### 5. Test API
```bash
# Status pesanan + posisi driver
curl http://localhost:8081/track/order/order-000123

# Posisi terbaru driver
curl http://localhost:8081/track/driver/driver-001

# Rute history driver (10 titik terakhir)
curl http://localhost:8081/track/driver/driver-001/history

# Health check
curl http://localhost:8081/healthz
```

---

## Arsitektur & Best Practices

### Topic Naming: `{domain}-{entity}-{event-type}`

> **Note**: Kita menggunakan hyphens/strip (`-`) alih-alih titik (`.`) atau underscore (`_`) pada nama topic. Sistem internal metrics Kafka menerjemahkan titik dan underscore menjadi underscore. Hal ini dapat menyebabkan bentrok pada nama metrics (misal `delivery.driver` dan `delivery_driver` keduanya menjadi `delivery_driver`), sehingga Kafka akan memunculkan warning `Due to limitations in metric names...`. Menggunakan hyphens menghindari warning ini.

```
delivery-driver-location-updated   # lokasi GPS driver
delivery-order-status-changed      # perubahan status pesanan
delivery-order-created             # pesanan baru
delivery-order-driver-assigned     # driver ditugaskan
delivery-dlq-failed-events         # dead letter queue
```

### Partition Key
| Topic | Key | Alasan |
|-------|-----|--------|
| `location-updated` | `driver_id` | Semua lokasi 1 driver → 1 partition → ordering terjaga |
| `order.status-changed` | `order_id` | Semua status 1 order → 1 partition → lifecycle urut |

### Message Format — Event Envelope
```json
{
  "event_id":   "1718123456789-42",
  "event_type": "driver.location.updated",
  "version":    1,
  "timestamp":  "2025-06-01T10:00:00Z",
  "source":     "driver-tracking-service",
  "payload": {
    "driver_id": "driver-001",
    "order_id":  "order-000123",
    "latitude":  -6.2088,
    "longitude": 106.8456,
    "bearing":   45.0,
    "speed_kmh": 35.2,
    "accuracy_m": 8.5
  }
}
```

### Producer Settings
- `acks=all` + `min.insync.replicas=2` → tidak ada data loss
- Idempotent producer → tidak ada duplikasi saat retry
- `linger=5ms` + batching → throughput tinggi
- Exponential backoff retry
- Async produce + error callback

### Consumer Settings
- **Consumer group** `tracking-api-service` → horizontal scalable (tambah instance = lebih banyak partition ditangani)
- **Manual offset commit** setelah processing → at-least-once guarantee
- **Static membership** (`instance.id`) → mengurangi rebalance saat rolling restart
- **Dead letter queue** → poison pill tidak block konsumsi

### Cluster Config
- `replication.factor=3` — toleran kehilangan 2 broker sekaligus
- `min.insync.replicas=2` — minimal 2 broker harus acknowledge
- `auto.create.topics.enable=false` — topic management eksplisit
- `unclean.leader.election=false` — tidak korbankan konsistensi untuk availability
- Kompresi `lz4` — fast compression, cocok untuk high-throughput

---

## Debug Commands

```bash
make list-topics          # lihat semua topics
make describe-location    # detail konfigurasi topic lokasi
make peek-location        # baca 5 pesan lokasi dari terminal
make consumer-groups      # cek lag consumer group
make logs-kafka           # log semua broker
```

## Kafka UI
Buka `http://localhost:8080` untuk melihat topics, consumer groups, messages secara visual.

---

## Testing High Availability (Broker Down Scenario)

Cluster ini dirancang menggunakan 3 broker dengan `replication.factor=3` dan `min.insync.replicas=2`. Ini berarti cluster bisa mentoleransi kegagalan tepat **1 broker** tanpa ada data loss atau downtime.

Anda bisa mensimulasikan kegagalan broker dengan Docker:

### 1. Hard Crash (Satu Broker Mati)
Mensimulasikan server mati atau restart.
```bash
docker stop kafka-1
```
- **Hasil:** Producer dan Consumer akan tetap berjalan. Kafka otomatis memilih leader baru untuk partisi yang sebelumnya ditangani oleh `kafka-1`. Data tetap aman karena `min.insync.replicas=2` masih terpenuhi oleh `kafka-2` dan `kafka-3`.
- **Recovery:** `docker start kafka-1`

### 2. Network Partition / JVM Freeze (Satu Broker Freeze)
Mensimulasikan kondisi di mana server masih hidup namun tidak merespons (misal masalah jaringan atau JVM Garbage Collection pause).
```bash
docker pause kafka-2
```
- **Hasil:** Cluster akan menganggap `kafka-2` mati setelah beberapa saat karena tidak ada heartbeat. Producer mungkin mengalami sedikit delay retry, tapi kemudian lanjut memproses data.
- **Recovery:** `docker unpause kafka-2`

### 3. Cluster Outage (Dua Broker Mati)
Mensimulasikan bencana di mana lebih dari satu broker mati bersamaan.
```bash
docker stop kafka-1 kafka-2
```
- **Hasil:** Producer akan menolak mengirim pesan baru dan memunculkan error. Ini adalah perilaku yang diinginkan (fail-safe) karena `acks=all` tidak bisa dipenuhi (hanya 1 broker yang tersisa, padahal butuh minimal 2). Kafka melindungi data Anda dengan menolak write daripada mengambil risiko data loss.
