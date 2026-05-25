package main

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/sammidev/delivery-kafka/schema"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ─── Driver State ─────────────────────────────────────────────────────────────

// Driver merepresentasikan state sebuah driver dalam simulasi
type Driver struct {
	ID          string
	OrderID     string
	Lat         float64
	Lng         float64
	Bearing     float64
	StatusIndex int // indeks di orderStatuses
}

// Move menggerakkan driver secara simulasi (random walk)
func (d *Driver) Move() {
	// Ubah bearing sedikit secara acak (jalan tidak lurus)
	d.Bearing += (rand.Float64()*30 - 15)
	if d.Bearing < 0 {
		d.Bearing += 360
	}
	if d.Bearing >= 360 {
		d.Bearing -= 360
	}

	// Gerak ~30m per interval dalam arah bearing
	bearingRad := d.Bearing * math.Pi / 180
	d.Lat += math.Cos(bearingRad) * 0.00027 // ~30m dalam derajat lintang
	d.Lng += math.Sin(bearingRad) * 0.00027
}

// ─── Driver Goroutines ────────────────────────────────────────────────────────

// runDriverLocationLoop terus-menerus menghasilkan location events untuk satu driver
func runDriverLocationLoop(ctx context.Context, client *kgo.Client, driver *Driver, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(locationInterval * time.Second)
	defer ticker.Stop()

	log := slog.With("driver_id", driver.ID)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			driver.Move()

			loc := schema.LocationPayload{
				DriverID:  driver.ID,
				OrderID:   driver.OrderID,
				Latitude:  driver.Lat,
				Longitude: driver.Lng,
				Bearing:   driver.Bearing,
				SpeedKmh:  20 + rand.Float64()*40, // 20–60 km/h
				Accuracy:  5 + rand.Float64()*10,  // 5–15 meter
			}

			env, err := newEnvelope("driver.location.updated", loc)
			if err != nil {
				log.Error("build envelope gagal", "err", err)
				continue
			}

			// Key = driver_id: semua lokasi driver ini → partition yang sama → ordering terjaga
			record, err := buildRecord(topicLocation, driver.ID, env)
			if err != nil {
				log.Error("build record gagal", "err", err)
				continue
			}

			// Produce asynchronous — callback untuk error handling
			client.Produce(ctx, record, func(r *kgo.Record, err error) {
				if err != nil {
					log.Error("produce lokasi gagal",
						"topic", r.Topic,
						"partition", r.Partition,
						"err", err,
					)
					return
				}
				log.Debug("lokasi terkirim",
					"lat", loc.Latitude,
					"lng", loc.Longitude,
					"partition", r.Partition,
					"offset", r.Offset,
				)
			})
		}
	}
}

// runOrderStatusLoop terus-menerus menghasilkan order status events untuk satu driver
func runOrderStatusLoop(ctx context.Context, client *kgo.Client, driver *Driver, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(statusInterval * time.Second)
	defer ticker.Stop()

	// Offset random agar tidak semua driver update status bersamaan
	jitter := time.Duration(rand.Intn(statusInterval)) * time.Second
	select {
	case <-ctx.Done():
		return
	case <-time.After(jitter):
	}

	log := slog.With("driver_id", driver.ID, "order_id", driver.OrderID)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if driver.StatusIndex >= len(orderStatuses)-1 {
				// Pesanan selesai, mulai pesanan baru
				driver.StatusIndex = 0
				driver.OrderID = generateOrderID()
			}

			prevStatus := orderStatuses[driver.StatusIndex]
			driver.StatusIndex++
			currStatus := orderStatuses[driver.StatusIndex]

			statusPayload := schema.OrderStatusPayload{
				OrderID:        driver.OrderID,
				DriverID:       driver.ID,
				CustomerID:     fmt.Sprintf("customer-%03d", rand.Intn(500)+1),
				PreviousStatus: prevStatus,
				CurrentStatus:  currStatus,
			}

			env, err := newEnvelope("order.status.changed", statusPayload)
			if err != nil {
				log.Error("build envelope gagal", "err", err)
				continue
			}

			// Key = order_id: semua event satu pesanan → partition yang sama → ordering terjaga
			record, err := buildRecord(topicOrderStatus, driver.OrderID, env)
			if err != nil {
				log.Error("build record gagal", "err", err)
				continue
			}

			client.Produce(ctx, record, func(r *kgo.Record, err error) {
				if err != nil {
					log.Error("produce status gagal", "err", err)
					return
				}
				log.Info("status pesanan berubah",
					"previous", prevStatus,
					"current", currStatus,
					"partition", r.Partition,
					"offset", r.Offset,
				)
			})
		}
	}
}
