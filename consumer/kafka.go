package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/sammidev/delivery-kafka/schema"
	"github.com/twmb/franz-go/pkg/kgo"
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
	}
	return kgo.NewClient(opts...)
}

func processRecord(ctx context.Context, record *kgo.Record, store *TrackingStore, dlqClient *kgo.Client, hub *Hub) error {
	var env schema.EventEnvelope
	if err := json.Unmarshal(record.Value, &env); err != nil {
		sendToDLQ(ctx, dlqClient, record, fmt.Sprintf("unmarshal envelope gagal: %v", err))
		return nil
	}

	switch record.Topic {
	case topicLocation:
		var loc schema.LocationPayload
		if err := json.Unmarshal(env.Payload, &loc); err != nil {
			sendToDLQ(ctx, dlqClient, record, fmt.Sprintf("unmarshal location payload gagal: %v", err))
			return nil
		}
		store.applyLocation(loc, env.Timestamp)
		slog.Debug("lokasi driver diproses", "driver_id", loc.DriverID, "lat", loc.Latitude, "lng", loc.Longitude)
		// Broadcast ke websocket
		hub.broadcast <- record.Value

	case topicOrderStatus:
		var status schema.OrderStatusPayload
		if err := json.Unmarshal(env.Payload, &status); err != nil {
			sendToDLQ(ctx, dlqClient, record, fmt.Sprintf("unmarshal status payload gagal: %v", err))
			return nil
		}
		store.applyOrderStatus(status, env.Timestamp)
		slog.Info("status order diproses", "order_id", status.OrderID, "driver_id", status.DriverID, "status", status.CurrentStatus)

	default:
		slog.Warn("topic tidak dikenal", "topic", record.Topic)
	}

	return nil
}

func sendToDLQ(ctx context.Context, client *kgo.Client, original *kgo.Record, reason string) {
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

	client.Produce(ctx, dlqRecord, func(_ *kgo.Record, err error) {
		if err != nil {
			slog.Error("gagal kirim ke DLQ", "err", err)
		}
	})
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

		fetches.EachRecord(func(record *kgo.Record) {
			if err := processRecord(ctx, record, store, dlqClient, hub); err != nil {
				slog.Error("process record gagal (akan retry)", "err", err)
			}
		})

		if err := client.CommitUncommittedOffsets(ctx); err != nil {
			if ctx.Err() == nil {
				slog.Error("commit offset gagal", "err", err)
			}
		}
	}
}
