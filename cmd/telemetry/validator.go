package main

import (
	"encoding/json"
	"log"

	"iicpc-platform/pkg/events"
)

// TargetResponse defines the expected JSON echo from the contestant's container.
type TargetResponse struct {
	OrderID        string  `json:"order_id"`
	Type           string  `json:"type"` // LIMIT, MARKET, CANCEL
	Side           string  `json:"side"` // BUY or SELL
	OrderedQty     int     `json:"ordered_qty"`
	FilledQty      int     `json:"filled_qty"`
	Price          float64 `json:"price"`
	ExecutionPrice float64 `json:"execution_price"`
	Status         string  `json:"status,omitempty"`
}

// OrderBook remains for compatibility with existing code.
// No internal state is currently required.
type OrderBook struct{}

// NewOrderBook creates a validator instance.
func NewOrderBook() *OrderBook {
	return &OrderBook{}
}

// ValidateOrder validates correctness of a contestant response.
func (ob *OrderBook) ValidateOrder(event events.OrderEvent) bool {

	// ------------------------------------------------------------------
	// 1. HTTP Success Validation
	// ------------------------------------------------------------------
	if !event.IsSuccessful {
		log.Printf(
			"DEBUG FAIL: Order %s failed. IsSuccessful=false. Body: %s",
			event.OrderID,
			event.ActualResponseBody,
		)
		return false
	}

	if event.StatusCode != event.ExpectedStatus {
		log.Printf(
			"DEBUG FAIL: Order %s failed. Expected HTTP %d, got %d",
			event.OrderID,
			event.ExpectedStatus,
			event.StatusCode,
		)
		return false
	}

	// ------------------------------------------------------------------
	// 2. JSON Validation
	// ------------------------------------------------------------------
	var resp TargetResponse

	if err := json.Unmarshal(
		[]byte(event.ActualResponseBody),
		&resp,
	); err != nil {
		log.Printf(
			"DEBUG FAIL: Order %s invalid JSON: %v",
			event.OrderID,
			err,
		)
		return false
	}

	// ------------------------------------------------------------------
	// 3. Basic Identity Validation
	// ------------------------------------------------------------------
	if resp.OrderID != event.OrderID {
		log.Printf(
			"DEBUG FAIL: Response order_id mismatch. Expected=%s Got=%s",
			event.OrderID,
			resp.OrderID,
		)
		return false
	}

	// ------------------------------------------------------------------
	// 4. Cancel Validation
	// ------------------------------------------------------------------
	if event.Type == "CANCEL" {
		if resp.Status != "cancelled" {
			log.Printf(
				"DEBUG FAIL: CANCEL order %s did not return status='cancelled'",
				event.OrderID,
			)
			return false
		}

		return true
	}

	// ------------------------------------------------------------------
	// 5. Quantity Validation
	// ------------------------------------------------------------------
	if resp.OrderedQty < 0 {
		log.Printf(
			"DEBUG FAIL: Order %s negative ordered quantity",
			event.OrderID,
		)
		return false
	}

	if resp.FilledQty < 0 {
		log.Printf(
			"DEBUG FAIL: Order %s negative filled quantity",
			event.OrderID,
		)
		return false
	}

	if resp.FilledQty > resp.OrderedQty {
		log.Printf(
			"DEBUG FAIL: Order %s overfilled. Ordered=%d Filled=%d",
			event.OrderID,
			resp.OrderedQty,
			resp.FilledQty,
		)
		return false
	}

	// ------------------------------------------------------------------
	// 6. Side Validation
	// ------------------------------------------------------------------
	if resp.Side != "BUY" && resp.Side != "SELL" {
		log.Printf(
			"DEBUG FAIL: Order %s invalid side=%s",
			event.OrderID,
			resp.Side,
		)
		return false
	}

	// ------------------------------------------------------------------
	// 7. Price Validation
	// ------------------------------------------------------------------
	if resp.Type == "LIMIT" && resp.FilledQty > 0 {

		if resp.Side == "BUY" &&
			resp.ExecutionPrice > resp.Price {

			log.Printf(
				"DEBUG FAIL: BUY execution %.4f worse than limit %.4f",
				resp.ExecutionPrice,
				resp.Price,
			)

			return false
		}

		if resp.Side == "SELL" &&
			resp.ExecutionPrice < resp.Price {

			log.Printf(
				"DEBUG FAIL: SELL execution %.4f worse than limit %.4f",
				resp.ExecutionPrice,
				resp.Price,
			)

			return false
		}
	}

	// ------------------------------------------------------------------
	// 8. Status Consistency Validation
	// ------------------------------------------------------------------
	if resp.FilledQty == 0 &&
		resp.Status == "FILLED" {

		log.Printf(
			"DEBUG FAIL: Order %s marked FILLED with zero fill",
			event.OrderID,
		)

		return false
	}

	if resp.FilledQty == resp.OrderedQty &&
		resp.OrderedQty > 0 &&
		resp.Status == "REJECTED" {

		log.Printf(
			"DEBUG FAIL: Order %s marked REJECTED despite full fill",
			event.OrderID,
		)

		return false
	}

	return true
}
