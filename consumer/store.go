package main

import (
	"sync"
	"time"

	"github.com/sammidev/delivery-kafka/schema"
)

// ─── In-Memory State Store ────────────────────────────────────────────────────

// DriverState adalah snapshot terbaru kondisi seorang driver.
type DriverState struct {
	DriverID        string          `json:"driver_id"         doc:"ID unik driver"`
	CurrentOrderID  string          `json:"order_id"          doc:"Order yang sedang dikerjakan"`
	Latitude        float64         `json:"latitude"          doc:"Posisi lintang terkini"`
	Longitude       float64         `json:"longitude"         doc:"Posisi bujur terkini"`
	Bearing         float64         `json:"bearing"           doc:"Arah hadap (0-360 derajat)"`
	SpeedKmh        float64         `json:"speed_kmh"         doc:"Kecepatan dalam km/h"`
	LastUpdated     time.Time       `json:"last_updated"      doc:"Waktu pembaruan terakhir"`
	LocationHistory []LocationPoint `json:"location_history"  doc:"Riwayat posisi (maks 10 titik)"`
}

// LocationPoint adalah satu titik dalam riwayat lokasi driver.
type LocationPoint struct {
	Latitude  float64   `json:"latitude"  doc:"Lintang"`
	Longitude float64   `json:"longitude" doc:"Bujur"`
	Timestamp time.Time `json:"timestamp" doc:"Waktu pencatatan"`
}

// OrderState adalah status terkini sebuah pesanan.
type OrderState struct {
	OrderID       string        `json:"order_id"        doc:"ID unik pesanan"`
	CustomerID    string        `json:"customer_id"     doc:"ID pelanggan"`
	DriverID      string        `json:"driver_id"       doc:"ID driver yang mengantar"`
	CurrentStatus string        `json:"current_status"  doc:"Status pesanan saat ini"`
	StatusHistory []StatusEvent `json:"status_history"  doc:"Riwayat perubahan status"`
	LastUpdated   time.Time     `json:"last_updated"    doc:"Waktu pembaruan terakhir"`
}

// StatusEvent mencatat satu transisi status pesanan.
type StatusEvent struct {
	From      string    `json:"from"      doc:"Status sebelumnya"`
	To        string    `json:"to"        doc:"Status baru"`
	Timestamp time.Time `json:"timestamp" doc:"Waktu transisi"`
}

// TrackingStore menyimpan state semua driver dan order di memory.
type TrackingStore struct {
	mu      sync.RWMutex
	drivers map[string]*DriverState
	orders  map[string]*OrderState
}

func newTrackingStore() *TrackingStore {
	return &TrackingStore{
		drivers: make(map[string]*DriverState),
		orders:  make(map[string]*OrderState),
	}
}

func (s *TrackingStore) applyLocation(loc schema.LocationPayload, eventTime time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	d, ok := s.drivers[loc.DriverID]
	if !ok {
		d = &DriverState{DriverID: loc.DriverID}
		s.drivers[loc.DriverID] = d
	}

	d.CurrentOrderID = loc.OrderID
	d.Latitude = loc.Latitude
	d.Longitude = loc.Longitude
	d.Bearing = loc.Bearing
	d.SpeedKmh = loc.SpeedKmh
	d.LastUpdated = eventTime

	d.LocationHistory = append(d.LocationHistory, LocationPoint{
		Latitude:  loc.Latitude,
		Longitude: loc.Longitude,
		Timestamp: eventTime,
	})
	if len(d.LocationHistory) > maxLocationHistory {
		d.LocationHistory = d.LocationHistory[len(d.LocationHistory)-maxLocationHistory:]
	}
}

func (s *TrackingStore) applyOrderStatus(status schema.OrderStatusPayload, eventTime time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	o, ok := s.orders[status.OrderID]
	if !ok {
		o = &OrderState{
			OrderID:    status.OrderID,
			CustomerID: status.CustomerID,
		}
		s.orders[status.OrderID] = o
	}

	o.DriverID = status.DriverID
	o.CurrentStatus = status.CurrentStatus
	o.LastUpdated = eventTime
	o.StatusHistory = append(o.StatusHistory, StatusEvent{
		From:      status.PreviousStatus,
		To:        status.CurrentStatus,
		Timestamp: eventTime,
	})
}

func (s *TrackingStore) getDriver(driverID string) (*DriverState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.drivers[driverID]
	if !ok {
		return nil, false
	}
	cp := *d
	return &cp, true
}

func (s *TrackingStore) getOrder(orderID string) (*OrderState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	o, ok := s.orders[orderID]
	if !ok {
		return nil, false
	}
	cp := *o
	return &cp, true
}

func (s *TrackingStore) getAllDrivers() []*DriverState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	list := make([]*DriverState, 0, len(s.drivers))
	for _, d := range s.drivers {
		cp := *d
		list = append(list, &cp)
	}
	return list
}

func (s *TrackingStore) getAllOrders() []*OrderState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	list := make([]*OrderState, 0, len(s.orders))
	for _, o := range s.orders {
		cp := *o
		list = append(list, &cp)
	}
	return list
}
