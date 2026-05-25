package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/sammidev/delivery-kafka/schema"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// ─── Kafka Consumer ───────────────────────────────────────────────────────────

func newKafkaClient() (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(topicLocation, topicOrderStatus),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxBytes(10_000_000),
		kgo.DialTimeout(10 * time.Second),
		kgo.ClientID("tracking-api-consumer"),
		kgo.InstanceID(fmt.Sprintf("instance-%d", os.Getpid())),

		// Security: SCRAM-SHA-256 Authentication
		kgo.SASL(scram.Auth{
			User: "consumer",
			Pass: "consumer-secret",
		}.AsSha256Mechanism()),
	}
	return kgo.NewClient(opts...)
}

func processRecord(ctx context.Context, record *kgo.Record, store *TrackingStore, dlqClient *kgo.Client, hub *Hub) error {
	var env schema.EventEnvelope
	if err := json.Unmarshal(record.Value, &env); err != nil {
		if err := sendToDLQ(ctx, dlqClient, record, fmt.Sprintf("unmarshal envelope gagal: %v", err)); err != nil {
			return fmt.Errorf("dlq write gagal: %w", err)
		}
		return nil
	}

	switch record.Topic {
	case topicLocation:
		var loc schema.LocationPayload
		if err := json.Unmarshal(env.Payload, &loc); err != nil {
			if err := sendToDLQ(ctx, dlqClient, record, fmt.Sprintf("unmarshal location payload gagal: %v", err)); err != nil {
				return fmt.Errorf("dlq write gagal: %w", err)
			}
			return nil
		}
		store.applyLocation(loc, env.Timestamp)
		slog.Debug("lokasi driver diproses", "driver_id", loc.DriverID, "lat", loc.Latitude, "lng", loc.Longitude)
		// Broadcast ke websocket
		hub.broadcast <- record.Value

	case topicOrderStatus:
		var status schema.OrderStatusPayload
		if err := json.Unmarshal(env.Payload, &status); err != nil {
			if err := sendToDLQ(ctx, dlqClient, record, fmt.Sprintf("unmarshal status payload gagal: %v", err)); err != nil {
				return fmt.Errorf("dlq write gagal: %w", err)
			}
			return nil
		}
		store.applyOrderStatus(status, env.Timestamp)
		slog.Info("status order diproses", "order_id", status.OrderID, "driver_id", status.DriverID, "status", status.CurrentStatus)

	default:
		slog.Warn("topic tidak dikenal", "topic", record.Topic)
	}

	return nil
}

func sendToDLQ(ctx context.Context, client *kgo.Client, original *kgo.Record, reason string) error {
	slog.Warn("kirim ke DLQ",
		"topic", original.Topic,
		"partition", original.Partition,
		"offset", original.Offset,
		"reason", reason,
	)

	dlqRecord := &kgo.Record{
		Topic: topicDLQ,
		Key:   original.Key,
		Value: original.Value,
		Headers: []kgo.RecordHeader{
			{Key: "original_topic", Value: []byte(original.Topic)},
			{Key: "original_partition", Value: []byte(fmt.Sprintf("%d", original.Partition))},
			{Key: "original_offset", Value: []byte(fmt.Sprintf("%d", original.Offset))},
			{Key: "failure_reason", Value: []byte(reason)},
			{Key: "failed_at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
		},
	}

	// Synchronous DLQ write to guarantee no data loss
	if err := client.ProduceSync(ctx, dlqRecord).FirstErr(); err != nil {
		slog.Error("gagal kirim ke DLQ", "err", err)
		return err
	}
	return nil
}

func runConsumerLoop(ctx context.Context, client *kgo.Client, dlqClient *kgo.Client, store *TrackingStore, hub *Hub) {
	for {
		fetches := client.PollFetches(ctx)

		if ctx.Err() != nil {
			return
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				slog.Error("fetch error", "topic", err.Topic, "partition", err.Partition, "err", err.Err)
			}
		}

		var wg sync.WaitGroup
		var hasError bool
		var errMu sync.Mutex

		// Process partitions concurrently. Ordering is maintained per-partition.
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			wg.Add(1)
			go func(p kgo.FetchTopicPartition) {
				defer wg.Done()
				for _, record := range p.Records {
					if err := processRecord(ctx, record, store, dlqClient, hub); err != nil {
						slog.Error("fatal: process record gagal", "err", err)
						errMu.Lock()
						hasError = true
						errMu.Unlock()
						// Stop processing this partition's remaining records to maintain ordering and prevent offset commit
						return
					}
				}
			}(p)
		})

		wg.Wait()

		if hasError {
			slog.Error("Menghentikan consumer karena kegagalan proses fatal (DLQ down). Offset TIDAK akan dicommit agar tidak ada data loss.")
			os.Exit(1)
		}

		if err := client.CommitUncommittedOffsets(ctx); err != nil {
			if ctx.Err() == nil {
				slog.Error("commit offset gagal", "err", err)
			}
		}
	}
}
