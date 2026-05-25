package schema

import (
	"encoding/json"
	"time"
)

// EventEnvelope: semua pesan dibungkus dengan metadata standar.
// Ini memungkinkan consumer melakukan schema evolution tanpa breaking changes.
type EventEnvelope struct {
	EventID   string          `json:"event_id"`   // UUID unik per event
	EventType string          `json:"event_type"` // misal: "driver.location.updated"
	Version   int             `json:"version"`    // schema version untuk backward compat
	Timestamp time.Time       `json:"timestamp"`  // waktu event dibuat (producer-side)
	Source    string          `json:"source"`     // nama service yang menghasilkan event
	Payload   json.RawMessage `json:"payload"`    // data spesifik event
}

// LocationPayload adalah payload untuk topic delivery.driver.location-updated
type LocationPayload struct {
	DriverID  string  `json:"driver_id"`
	OrderID   string  `json:"order_id"`   // order yang sedang diantarkan (bisa kosong)
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Bearing   float64 `json:"bearing"`    // arah perjalanan dalam derajat (0-360)
	SpeedKmh  float64 `json:"speed_kmh"`
	Accuracy  float64 `json:"accuracy_m"` // akurasi GPS dalam meter
}

// OrderStatusPayload adalah payload untuk topic delivery.order.status-changed
type OrderStatusPayload struct {
	OrderID        string `json:"order_id"`
	DriverID       string `json:"driver_id"`
	CustomerID     string `json:"customer_id"`
	PreviousStatus string `json:"previous_status"`
	CurrentStatus  string `json:"current_status"`
	Reason         string `json:"reason,omitempty"` // alasan perubahan status (opsional)
}
