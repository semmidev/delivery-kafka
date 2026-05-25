package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humachi"
	_ "github.com/danielgtaylor/huma/v2/formats/cbor"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// ─── Huma I/O Types ───────────────────────────────────────────────────────────

// HealthOutput adalah response untuk /healthz.
type HealthOutput struct {
	Body struct {
		Status string `json:"status" example:"ok" doc:"Status layanan"`
	}
}

// TrackOrderInput adalah input untuk endpoint tracking pesanan.
type TrackOrderInput struct {
	OrderID string `path:"order_id" doc:"ID pesanan yang dilacak" example:"order-123"`
}

// TrackOrderOutput adalah response untuk endpoint tracking pesanan.
type TrackOrderOutput struct {
	Body struct {
		Order  *OrderState  `json:"order"            doc:"Status pesanan saat ini"`
		Driver *DriverState `json:"driver,omitempty" doc:"Posisi driver (jika tersedia)"`
	}
}

// TrackDriverInput adalah input untuk endpoint tracking driver.
type TrackDriverInput struct {
	DriverID string `path:"driver_id" doc:"ID driver yang dilacak" example:"driver-456"`
}

// TrackDriverOutput adalah response posisi terkini driver (tanpa history).
type TrackDriverOutput struct {
	Body *DriverState
}

// DriverHistoryOutput adalah response riwayat lokasi driver.
type DriverHistoryOutput struct {
	Body struct {
		DriverID string          `json:"driver_id" doc:"ID driver"`
		History  []LocationPoint `json:"history"   doc:"Daftar titik lokasi terakhir"`
		Count    int             `json:"count"     doc:"Jumlah titik dalam history"`
	}
}

// ListOrdersOutput adalah response untuk endpoint list orders.
type ListOrdersOutput struct {
	Body struct {
		Orders []*OrderState `json:"orders" doc:"Daftar pesanan"`
		Count  int           `json:"count"  doc:"Jumlah pesanan"`
	}
}

// ListDriversOutput adalah response untuk endpoint list drivers.
type ListDriversOutput struct {
	Body struct {
		Drivers []*DriverState `json:"drivers" doc:"Daftar driver"`
		Count   int            `json:"count"   doc:"Jumlah driver"`
	}
}

// ─── HTTP API (Huma + Chi) ────────────────────────────────────────────────────

func setupAPI(store *TrackingStore) *chi.Mux {
	router := chi.NewMux()

	// Middleware standar Chi
	router.Use(middleware.RequestID)
	router.Use(middleware.RealIP)
	router.Use(middleware.Recoverer)
	router.Use(middleware.Timeout(30 * time.Second))

	// Inisialisasi Huma di atas Chi
	config := huma.DefaultConfig(apiTitle, apiVersion)
	config.Info.Description = "Real-time delivery tracking API. Konsumsi event Kafka dan sajikan posisi driver serta status order."
	api := humachi.New(router, config)

	// ── GET /healthz ──────────────────────────────────────────────────────────
	huma.Register(api, huma.Operation{
		OperationID: "get-health",
		Method:      http.MethodGet,
		Path:        "/healthz",
		Summary:     "Health check",
		Description: "Mengembalikan status layanan. Gunakan untuk liveness/readiness probe.",
		Tags:        []string{"Monitoring"},
	}, func(ctx context.Context, _ *struct{}) (*HealthOutput, error) {
		out := &HealthOutput{}
		out.Body.Status = "ok"
		return out, nil
	})

	// ── GET /track/order/{order_id} ───────────────────────────────────────────
	huma.Register(api, huma.Operation{
		OperationID: "track-order",
		Method:      http.MethodGet,
		Path:        "/track/order/{order_id}",
		Summary:     "Lacak pesanan",
		Description: "Mengembalikan status terkini pesanan beserta posisi driver jika sedang aktif.",
		Tags:        []string{"Tracking"},
	}, func(ctx context.Context, input *TrackOrderInput) (*TrackOrderOutput, error) {
		order, ok := store.getOrder(input.OrderID)
		if !ok {
			return nil, huma.Error404NotFound(
				fmt.Sprintf("order %q tidak ditemukan", input.OrderID),
			)
		}

		out := &TrackOrderOutput{}
		out.Body.Order = order

		if order.DriverID != "" {
			if driver, ok := store.getDriver(order.DriverID); ok {
				out.Body.Driver = driver
			}
		}

		return out, nil
	})

	// ── GET /track/driver/{driver_id} ─────────────────────────────────────────
	huma.Register(api, huma.Operation{
		OperationID: "track-driver",
		Method:      http.MethodGet,
		Path:        "/track/driver/{driver_id}",
		Summary:     "Posisi terkini driver",
		Description: "Mengembalikan posisi terbaru driver. Riwayat lokasi tidak disertakan; gunakan endpoint /history untuk itu.",
		Tags:        []string{"Tracking"},
	}, func(ctx context.Context, input *TrackDriverInput) (*TrackDriverOutput, error) {
		driver, ok := store.getDriver(input.DriverID)
		if !ok {
			return nil, huma.Error404NotFound(
				fmt.Sprintf("driver %q tidak ditemukan", input.DriverID),
			)
		}

		// Hapus history — endpoint ini hanya untuk posisi terkini
		driver.LocationHistory = nil

		return &TrackDriverOutput{Body: driver}, nil
	})

	// ── GET /track/driver/{driver_id}/history ─────────────────────────────────
	huma.Register(api, huma.Operation{
		OperationID: "get-driver-history",
		Method:      http.MethodGet,
		Path:        "/track/driver/{driver_id}/history",
		Summary:     "Riwayat lokasi driver",
		Description: fmt.Sprintf("Mengembalikan hingga %d titik lokasi terakhir driver.", maxLocationHistory),
		Tags:        []string{"Tracking"},
	}, func(ctx context.Context, input *TrackDriverInput) (*DriverHistoryOutput, error) {
		driver, ok := store.getDriver(input.DriverID)
		if !ok {
			return nil, huma.Error404NotFound(
				fmt.Sprintf("driver %q tidak ditemukan", input.DriverID),
			)
		}

		out := &DriverHistoryOutput{}
		out.Body.DriverID = input.DriverID
		out.Body.History = driver.LocationHistory
		out.Body.Count = len(driver.LocationHistory)

		return out, nil
	})

	// ── GET /track/orders ──────────────────────────────────────────────────────
	huma.Register(api, huma.Operation{
		OperationID: "list-orders",
		Method:      http.MethodGet,
		Path:        "/track/orders",
		Summary:     "Daftar semua pesanan",
		Description: "Mengembalikan daftar semua pesanan yang ada di memory.",
		Tags:        []string{"Tracking"},
	}, func(ctx context.Context, _ *struct{}) (*ListOrdersOutput, error) {
		orders := store.getAllOrders()
		out := &ListOrdersOutput{}
		out.Body.Orders = orders
		out.Body.Count = len(orders)
		return out, nil
	})

	// ── GET /track/drivers ─────────────────────────────────────────────────────
	huma.Register(api, huma.Operation{
		OperationID: "list-drivers",
		Method:      http.MethodGet,
		Path:        "/track/drivers",
		Summary:     "Daftar semua driver",
		Description: "Mengembalikan daftar semua posisi driver (tanpa history) yang ada di memory.",
		Tags:        []string{"Tracking"},
	}, func(ctx context.Context, _ *struct{}) (*ListDriversOutput, error) {
		drivers := store.getAllDrivers()
		
		// Hapus history agar tidak kepanjangan
		for _, d := range drivers {
			d.LocationHistory = nil
		}

		out := &ListDriversOutput{}
		out.Body.Drivers = drivers
		out.Body.Count = len(drivers)
		return out, nil
	})

	return router
}
