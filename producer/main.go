// producer/main.go
//
// Delivery App — Kafka Producer
//
// Mensimulasikan banyak driver yang terus-menerus mengirim:
//   1. delivery-driver-location-updated  → posisi GPS real-time
//   2. delivery-order-status-changed     → perubahan status pesanan
//
// Best practices yang diterapkan:
//   - Message key = entity ID (driver_id / order_id) → ordering per-entity dijamin
//   - Payload JSON dengan envelope {metadata, payload} → versioned & extensible
//   - Producer idempotent + acks=all + retry untuk durability
//   - Batching & linger untuk throughput tinggi
//   - Context-aware graceful shutdown
//   - Structured log ke stderr, data ke Kafka

package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// ─── Konstanta ────────────────────────────────────────────────────────────────

const (
	topicLocation    = "delivery-driver-location-updated"
	topicOrderStatus = "delivery-order-status-changed"

	numDrivers       = 20 // jumlah driver yang disimulasikan
	locationInterval = 3  // detik antar update lokasi per driver
	statusInterval   = 15 // detik antar status-change event per order
)

var brokers = []string{
	"localhost:9092",
	"localhost:9093",
	"localhost:9094",
}

// Status lifecycle pesanan
var orderStatuses = []string{
	"created",
	"driver_assigned",
	"picked_up",
	"on_the_way",
	"arrived",
	"delivered",
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	// Structured logging
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	slog.Info("🚀 Delivery Producer dimulai",
		"drivers", numDrivers,
		"brokers", brokers,
	)

	// Graceful shutdown via context
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Buat Kafka client
	client, err := newKafkaClient()
	if err != nil {
		slog.Error("gagal membuat Kafka client", "err", err)
		os.Exit(1)
	}
	defer func() {
		slog.Info("Flush pesan tersisa ke Kafka...")
		// FlushAndLeave memastikan semua pesan yang sudah di-buffer terkirim
		if err := client.Flush(context.Background()); err != nil {
			slog.Error("flush gagal", "err", err)
		}
		client.Close()
		slog.Info("Producer berhenti dengan bersih.")
	}()

	// Inisialisasi driver dengan posisi awal di sekitar Jakarta
	drivers := make([]*Driver, numDrivers)
	for i := range drivers {
		drivers[i] = &Driver{
			ID:          fmt.Sprintf("driver-%03d", i+1),
			OrderID:     generateOrderID(),
			Lat:         -6.2088 + (rand.Float64()-0.5)*0.1, // sekitar Jakarta
			Lng:         106.8456 + (rand.Float64()-0.5)*0.1,
			Bearing:     rand.Float64() * 360,
			StatusIndex: rand.Intn(len(orderStatuses) - 1),
		}
	}

	// Jalankan goroutine untuk setiap driver
	var wg sync.WaitGroup
	for _, d := range drivers {
		wg.Add(2)
		go runDriverLocationLoop(ctx, client, d, &wg)
		go runOrderStatusLoop(ctx, client, d, &wg)
	}

	slog.Info("✅ Semua driver goroutine aktif",
		"total_goroutines", numDrivers*2,
	)

	// Tunggu signal shutdown
	<-ctx.Done()
	slog.Info("Sinyal shutdown diterima, menunggu goroutine selesai...")
	wg.Wait()
}
