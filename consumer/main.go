// consumer/main.go
//
// Delivery App — Kafka Consumer + Live Tracking API
//
// Mengonsumsi event dari Kafka dan menyediakan REST API untuk:
//   GET /track/order/{order_id}            → status + posisi driver saat ini
//   GET /track/driver/{driver_id}          → posisi terbaru driver
//   GET /track/driver/{driver_id}/history  → 10 lokasi terakhir
//   GET /healthz                           → health check
//
// Best practices yang diterapkan:
//   - Consumer group dengan group.id spesifik → horizontal scalable
//   - Manual offset commit setelah processing berhasil (at-least-once)
//   - Concurrent fetch + sequential processing per partition
//   - In-memory store dengan RWMutex (swap ke Redis di production)
//   - Dead letter queue untuk pesan yang gagal diparse
//   - Graceful shutdown: commit offset sebelum keluar
//   - HTTP layer via Huma v2 (OpenAPI, validation, docs) + Chi router

package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ─── Konstanta ────────────────────────────────────────────────────────────────

const (
	topicLocation    = "delivery-driver-location-updated"
	topicOrderStatus = "delivery-order-status-changed"
	topicDLQ         = "delivery-dlq-failed-events"

	consumerGroup      = "tracking-api-service"
	maxLocationHistory = 10
	httpAddr           = ":8081"

	apiTitle   = "Delivery Tracking API"
	apiVersion = "1.0.0"
)

var brokers = []string{
	"localhost:9092",
	"localhost:9093",
	"localhost:9094",
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	slog.Info("🚀 Tracking Consumer + API dimulai",
		"group", consumerGroup,
		"brokers", brokers,
		"addr", httpAddr,
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store := newTrackingStore()

	// Main Kafka consumer client
	consumerClient, err := newKafkaClient()
	if err != nil {
		slog.Error("gagal membuat consumer client", "err", err)
		os.Exit(1)
	}
	defer consumerClient.Close()

	// Dedicated DLQ producer client
	dlqClient, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ClientID("tracking-dlq-producer"),
	)
	if err != nil {
		slog.Error("gagal membuat DLQ client", "err", err)
		os.Exit(1)
	}
	defer dlqClient.Close()

	var wg sync.WaitGroup

	// Kafka consumer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		runConsumerLoop(ctx, consumerClient, dlqClient, store)
		slog.Info("Consumer loop selesai")
	}()

	// HTTP server dengan Huma + Chi
	router := setupAPI(store)
	server := &http.Server{
		Addr:         httpAddr,
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		slog.Info("HTTP server listening", "addr", httpAddr,
			"docs", fmt.Sprintf("http://localhost%s/docs", httpAddr),
			"openapi", fmt.Sprintf("http://localhost%s/openapi.json", httpAddr),
		)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server error", "err", err)
		}
	}()

	// Tunggu sinyal shutdown
	<-ctx.Done()
	slog.Info("Sinyal shutdown diterima...")

	// Graceful HTTP shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("HTTP shutdown error", "err", err)
	}

	// Commit offset terakhir sebelum leave consumer group
	if err := consumerClient.CommitUncommittedOffsets(shutdownCtx); err != nil {
		slog.Error("final commit offset gagal", "err", err)
	}

	wg.Wait()
	slog.Info("✅ Consumer berhenti dengan bersih.")
}
