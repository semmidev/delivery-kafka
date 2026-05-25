package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// ─── Helpers ──────────────────────────────────────────────────────────────────

var idCounter uint64
var idMu sync.Mutex

func generateID() string {
	idMu.Lock()
	defer idMu.Unlock()
	idCounter++
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), idCounter)
}

func generateOrderID() string {
	return fmt.Sprintf("order-%06d", rand.Intn(999999)+1)
}
