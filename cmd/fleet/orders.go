package main

import (
	"fmt"
	"math/rand"
	"time"
)

type Order struct {
	ID             string  `json:"id"`
	Type           string  `json:"type"`
	Side           string  `json:"side,omitempty"`
	Price          float64 `json:"price,omitempty"`
	Quantity       int     `json:"quantity,omitempty"`
	Timestamp      int64   `json:"timestamp"`
	CancelTargetID string  `json:"cancel_target_id,omitempty"`
}

// generateOrder creates diverse, randomized realistic trading orders
func generateOrder(index int) Order {
	// Local RNG initialized per function call prevents global lock contention
	// under extreme concurrency in the bot fleet.
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(index)))

	// Distribution: ~10% CANCEL, ~45% MARKET, ~45% LIMIT
	roll := rng.Float32()
	orderType := "LIMIT"
	if roll > 0.90 {
		orderType = "CANCEL"
	} else if roll > 0.45 {
		orderType = "MARKET"
	}

	timestamp := time.Now().UnixNano()

	// If CANCEL, we only need to target a previously generated order
	if orderType == "CANCEL" {
		targetIndex := 1
		if index > 1 {
			targetIndex = rng.Intn(index-1) + 1
		}
		return Order{
			ID:             fmt.Sprintf("ORD-%d", index),
			Type:           orderType,
			Timestamp:      timestamp,
			CancelTargetID: fmt.Sprintf("ORD-%d", targetIndex),
		}
	}

	side := "BUY"
	if rng.Float32() > 0.5 {
		side = "SELL"
	}

	// Price float between 10.00 and 500.00, rounded to 2 decimal places
	price := 10.0 + rng.Float64()*490.0
	price = float64(int(price*100)) / 100

	// Quantity between 1 and 100
	quantity := rng.Intn(100) + 1

	return Order{
		ID:        fmt.Sprintf("ORD-%d", index),
		Type:      orderType,
		Side:      side,
		Price:     price,
		Quantity:  quantity,
		Timestamp: timestamp,
	}
}
