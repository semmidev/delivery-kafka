package main

import (
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/sammidev/delivery-kafka/schema"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// ─── Producer ─────────────────────────────────────────────────────────────────

func newKafkaClient() (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),

		// Security: SCRAM-SHA-256 Authentication
		kgo.SASL(scram.Auth{
			User: "producer",
			Pass: "producer-secret",
		}.AsSha256Mechanism()),

		// Durability: semua replika in-sync harus acknowledge
		kgo.RequiredAcks(kgo.AllISRAcks()),

		// Idempotent producer: mencegah duplikasi saat retry
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),

		// Throughput: batch pesan selama 5ms sebelum dikirim
		kgo.ProducerLinger(5 * time.Millisecond),

		// Batas ukuran batch per partition
		kgo.ProducerBatchMaxBytes(1_000_000), // 1 MiB

		// Retry otomatis untuk transient errors
		kgo.RetryBackoffFn(func(tries int) time.Duration {
			// Exponential backoff: 100ms, 200ms, 400ms, ... max 5s
			backoff := time.Duration(math.Pow(2, float64(tries))) * 100 * time.Millisecond
			if backoff > 5*time.Second {
				backoff = 5 * time.Second
			}
			return backoff
		}),

		// Timeout koneksi
		kgo.DialTimeout(10 * time.Second),

		// Identifikasi producer di Kafka logs
		kgo.ClientID("delivery-producer"),
	}

	return kgo.NewClient(opts...)
}

// buildRecord membuat kgo.Record siap kirim dari envelope
// Key = entity ID → memastikan semua event dari satu entitas masuk ke partition yang sama
func buildRecord(topic, key string, env schema.EventEnvelope) (*kgo.Record, error) {
	payload, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("marshal envelope: %w", err)
	}

	return &kgo.Record{
		Topic: topic,
		Key:   []byte(key), // partition key
		Value: payload,

		// Header: metadata ringan tanpa decode payload
		// Berguna untuk routing, filtering, observability
		Headers: []kgo.RecordHeader{
			{Key: "event_type", Value: []byte(env.EventType)},
			{Key: "version", Value: []byte(fmt.Sprintf("%d", env.Version))},
			{Key: "source", Value: []byte(env.Source)},
		},
	}, nil
}

// newEnvelope membuat EventEnvelope baru
func newEnvelope(eventType string, payloadData any) (schema.EventEnvelope, error) {
	rawPayload, err := json.Marshal(payloadData)
	if err != nil {
		return schema.EventEnvelope{}, err
	}

	return schema.EventEnvelope{
		EventID:   generateID(),
		EventType: eventType,
		Version:   1,
		Timestamp: time.Now().UTC(),
		Source:    "driver-tracking-service",
		Payload:   rawPayload,
	}, nil
}
