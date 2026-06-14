package main

import (
	"encoding/json"
	"log"
	"sync"

	"iicpc-platform/pkg/events"
)

// TargetResponse defines the expected JSON echo from the contestant's container
type TargetResponse struct {
	OrderID        string  `json:"order_id"`
	Type           string  `json:"type"` // "LIMIT", "MARKET", "CANCEL"
	Side           string  `json:"side"` // "BUY" or "SELL"
	OrderedQty     int     `json:"ordered_qty"`
	FilledQty      int     `json:"filled_qty"`
	Price          float64 `json:"price"`            // The requested limit price
	ExecutionPrice float64 `json:"execution_price"`  // The actual filled price
	Status         string  `json:"status,omitempty"` // Used for CANCEL confirmations
}

// FilledOrder tracks historical executions to enforce price-time priority
type FilledOrder struct {
	OrderID   string
	Price     float64
	Side      string
	Timestamp int64
}

// OrderBook maintains state across validations to enforce temporal rules
type OrderBook struct {
	mu    sync.RWMutex
	fills map[string][]FilledOrder
}

// NewOrderBook initializes the in-memory validation state
func NewOrderBook() *OrderBook {
	return &OrderBook{
		fills: make(map[string][]FilledOrder),
	}
}

// ValidateOrder ensures the contestant's trading engine followed the rules
func (ob *OrderBook) ValidateOrder(event events.OrderEvent) bool {
	// 1. Status Code Validation
	if !event.IsSuccessful {
		log.Printf("DEBUG FAIL: Order %s failed. IsSuccessful=false. Body: %s", event.OrderID, event.ActualResponseBody)
		return false
	}
	if event.StatusCode != event.ExpectedStatus {
		log.Printf("DEBUG FAIL: Order %s failed. Expected HTTP %d, got %d", event.OrderID, event.ExpectedStatus, event.StatusCode)
		return false
	}

	// 2. Parse the response body
	var resp TargetResponse
	if err := json.Unmarshal([]byte(event.ActualResponseBody), &resp); err != nil {
		log.Printf("DEBUG FAIL: Order %s failed. Invalid JSON: %v", event.OrderID, err)
		return false
	}

	// Route specific logic for CANCEL events
	if event.Type == "CANCEL" {
		if resp.Status != "cancelled" {
			log.Printf("DEBUG FAIL: CANCEL Order %s did not return status 'cancelled'", event.OrderID)
			return false
		}
		return true // Cancel logic stops here
	}

	// 3. Rule: Cannot fill more than requested
	if resp.FilledQty > resp.OrderedQty || resp.FilledQty < 0 {
		log.Printf("DEBUG FAIL: Order %s math error. Ordered: %d, Filled: %d", event.OrderID, resp.OrderedQty, resp.FilledQty)
		return false
	}

	// 4. Rule: Correct price bounds for LIMIT orders
	if resp.Type == "LIMIT" && resp.FilledQty > 0 {
		if resp.Side == "BUY" && resp.ExecutionPrice > resp.Price {
			log.Printf("DEBUG FAIL: BUY execution (%.4f) worse than Limit (%.4f)", resp.ExecutionPrice, resp.Price)
			return false
		} else if resp.Side == "SELL" && resp.ExecutionPrice < resp.Price {
			log.Printf("DEBUG FAIL: SELL execution (%.4f) worse than Limit (%.4f)", resp.ExecutionPrice, resp.Price)
			return false
		}
	}

	// 5. Rule: Price-Time Priority Validation (FIFO within same price level)
	if resp.FilledQty > 0 {
		ob.mu.Lock()
		defer ob.mu.Unlock()

		history := ob.fills[event.SubmissionID]
		for _, priorFill := range history {
			if priorFill.Side == resp.Side && priorFill.Price == resp.ExecutionPrice {
				if event.Timestamp < priorFill.Timestamp {
					log.Printf("DEBUG FAIL: Order %s violated price-time priority against older fill %s", event.OrderID, priorFill.OrderID)
					return false
				}
			}
		}

		// Record successful fill for future priority checks
		ob.fills[event.SubmissionID] = append(history, FilledOrder{
			OrderID:   event.OrderID,
			Price:     resp.ExecutionPrice,
			Side:      resp.Side,
			Timestamp: event.Timestamp,
		})
	}

	return true
}
