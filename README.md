# Delivery App — Kafka Live Tracking

Studi kasus Kafka untuk delivery app: real-time driver location + order status tracking.

## Stack
- **Kafka 3.9** — KRaft mode (tanpa ZooKeeper), 3 broker
- **Go** + [franz-go](https://github.com/twmb/franz-go)
- **Kafka UI** di `http://localhost:8080`

## Struktur

```
delivery-kafka/
├── compose.yml              # 3-broker KRaft cluster + Kafka UI
├── go.work                  # Go Workspace untuk manage multi-module
├── schema/                  # Shared models & event envelopes
│   └── models.go            # Struct EventEnvelope, LocationPayload, dsb.
├── scripts/
│   └── init-topics.sh       # Buat semua topics dengan best practice config
├── producer/                # Simulasi banyak driver (location + order status)
│   ├── main.go              # Setup producer & root logic
│   ├── driver.go            # Simulasi pergerakan GPS & state driver
│   ├── kafka.go             # Inisialisasi Kafka client & record building
│   └── helpers.go           # ID generator
├── consumer/                # Consumer Kafka + REST API live tracking
│   ├── main.go              # Setup consumer & root logic
│   ├── api.go               # Huma v2 + Chi REST API routing
│   ├── kafka.go             # Kafka polling loop & DLQ handler
│   └── store.go             # In-memory TrackingStore & state
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

---

### Membedah Metrik High Availability (Studi Kasus Broker Down)

Ketika Anda menyimulasikan kegagalan (*node failure*), Anda bisa melihat perubahan langsung pada metrik di *dashboard* Kafbat UI. Berikut adalah arti dari setiap metrik tersebut:

#### 1. Dinamika Controller & KRaft
* **Controller Type: KRaft:** Kafka menggunakan algoritma konsensus internal KRaft (Kafka Raft), bukan Zookeeper.
* **Active Controller:** Jika *broker* yang kebetulan sedang bertugas sebagai *Controller* (mandor) mati, KRaft akan langsung mendeteksi dan secara otomatis menunjuk *broker* yang masih hidup sebagai *Active Controller* baru dalam hitungan milidetik.

#### 2. Membedah Angka Merah: URP, ISR, dan OSR
Sebagai contoh di *dashboard* sering terlihat total **83 Partisi**. Dari mana angka ini?
- **33 Partisi Buatan:** Berasal dari *topic* yang kita buat (`location-updated`=12, `status-changed`=6, `order-created`=6, `driver-assigned`=6, `dlq`=3).
- **50 Partisi Internal:** Kafka otomatis membuat *topic* `__consumer_offsets` dengan *default* 50 partisi untuk melacak posisi baca dari *consumer group*.

Dengan **Replication Factor = 3**, total salinan yang harus ada adalah 249 ($83 \times 3$). Jika 1 *broker* mati, maka 83 salinan ikut hilang dari peredaran.
* **In Sync Replicas (ISR):** Jumlah salinan data yang saat ini sehat dan sinkron dengan *Leader*. (Misal: sisa 166 dari target 249).
* **Out of Sync Replicas (OSR):** Jumlah replika yang tertinggal atau berada di *broker* yang mati (Misal: 83 replika).
* **URP (Under Replicated Partitions) - Indikator Alarm:** Jumlah partisi yang jumlah replika aktifnya **kurang dari** target *Replication Factor*. Karena targetnya 3 tapi cuma tersisa 2, maka semua 83 partisi Anda berstatus *Under Replicated*. Ini adalah peringatan bahwa *cluster* sedang beroperasi dengan jaring pengaman yang berkurang.

#### 3. Tabel Keseimbangan Beban (Leaders Skew)
* **Leaders:** Setiap partisi hanya memiliki 1 *Leader* untuk *Read/Write*. Kafka membagi beban ini ke sisa *broker* yang hidup secara adil.
* **Leaders Skew:** Persentase ketidakseimbangan beban antar *broker*. Jika angkanya mendekati 0-1%, artinya distribusi *Leader* nyaris sempurna dan tidak ada *broker* yang kewalahan.

### Analogi Brankas (Memahami URP)

Bayangkan Anda punya aturan: *"Setiap data transaksi wajib disimpan ke dalam **3 brankas** di gedung berbeda."*

1. Tiba-tiba gedung tempat **Brankas 3** mati lampu (*broker down*).
2. **Apakah datanya hilang?** **Tidak.** Kita masih bisa melayani transaksi pakai Brankas 1 dan Brankas 2. Sistem tidak *down*.
3. **Apakah aturan terpenuhi?** **Tidak.** Aturannya butuh 3, tapi sekarang cuma ada 2 yang hidup. Kondisi inilah yang disebut **Under Replicated (URP)**.

Kafka mewarnai metrik URP dengan warna merah sebagai peringatan dini (*early warning*): *"Kita masih bisa beroperasi, tapi jaring pengaman kita robek satu. Segera hidupkan kembali server yang mati sebelum server kedua ikut mati dan menyebabkan kehilangan data."*
