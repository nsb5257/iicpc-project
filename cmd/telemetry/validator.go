package main

import (
	"encoding/json"
	"log"
	"sort"
	"sync"

	"iicpc-platform/pkg/events"
)

type TargetResponse struct {
	OrderID        string  `json:"order_id"`
	Type           string  `json:"type"`
	Side           string  `json:"side"`
	OrderedQty     int     `json:"ordered_qty"`
	FilledQty      int     `json:"filled_qty"`
	Price          float64 `json:"price"`
	ExecutionPrice float64 `json:"execution_price"`
	Status         string  `json:"status,omitempty"`
}

type limitEntry struct {
	orderID  string
	price    float64
	qty      int
	sequence int64
}

type OrderBook struct {
	mu       sync.Mutex
	bids     map[string]limitEntry
	asks     map[string]limitEntry
	sequence int64
}

func NewOrderBook() *OrderBook {
	return &OrderBook{
		bids: make(map[string]limitEntry),
		asks: make(map[string]limitEntry),
	}
}

func (ob *OrderBook) ValidateOrder(event events.OrderEvent) bool {
	if !event.IsSuccessful {
		log.Printf("DEBUG FAIL: Order %s failed. IsSuccessful=false. Body: %s", event.OrderID, event.ActualResponseBody)
		return false
	}
	if event.StatusCode != event.ExpectedStatus {
		log.Printf("DEBUG FAIL: Order %s Expected HTTP %d, got %d", event.OrderID, event.ExpectedStatus, event.StatusCode)
		return false
	}

	var resp TargetResponse
	if err := json.Unmarshal([]byte(event.ActualResponseBody), &resp); err != nil {
		log.Printf("DEBUG FAIL: Order %s invalid JSON: %v", event.OrderID, err)
		return false
	}

	if resp.OrderID != event.OrderID {
		log.Printf("DEBUG FAIL: order_id mismatch. Expected=%s Got=%s", event.OrderID, resp.OrderID)
		return false
	}

	if event.Type == "CANCEL" {
		if resp.Status != "cancelled" {
			log.Printf("DEBUG FAIL: CANCEL %s did not return status='cancelled'", event.OrderID)
			return false
		}
		ob.mu.Lock()
		delete(ob.bids, event.OrderID)
		delete(ob.asks, event.OrderID)
		ob.mu.Unlock()
		return true
	}

	if resp.OrderedQty < 0 || resp.FilledQty < 0 {
		return false
	}
	if resp.FilledQty > resp.OrderedQty {
		log.Printf("DEBUG FAIL: Order %s overfilled. Ordered=%d Filled=%d", event.OrderID, resp.OrderedQty, resp.FilledQty)
		return false
	}

	if resp.Side != "BUY" && resp.Side != "SELL" {
		log.Printf("DEBUG FAIL: Order %s invalid side=%s", event.OrderID, resp.Side)
		return false
	}

	if resp.Type == "LIMIT" && resp.FilledQty > 0 {
		if resp.Side == "BUY" && resp.ExecutionPrice > resp.Price {
			log.Printf("DEBUG FAIL: BUY execution %.4f worse than limit %.4f", resp.ExecutionPrice, resp.Price)
			return false
		}
		if resp.Side == "SELL" && resp.ExecutionPrice < resp.Price {
			log.Printf("DEBUG FAIL: SELL execution %.4f worse than limit %.4f", resp.ExecutionPrice, resp.Price)
			return false
		}
	}

	if resp.FilledQty == 0 && resp.Status == "FILLED" {
		return false
	}
	if resp.FilledQty == resp.OrderedQty && resp.OrderedQty > 0 && resp.Status == "REJECTED" {
		return false
	}

	if resp.Type == "LIMIT" && resp.FilledQty > 0 {
		if !ob.validatePriceTimePriority(resp) {
			return false
		}
	}

	if resp.Type == "LIMIT" && resp.FilledQty < resp.OrderedQty && resp.Status != "REJECTED" {
		ob.mu.Lock()
		ob.sequence++
		entry := limitEntry{
			orderID:  resp.OrderID,
			price:    resp.Price,
			qty:      resp.OrderedQty - resp.FilledQty,
			sequence: ob.sequence,
		}
		if resp.Side == "BUY" {
			ob.bids[resp.OrderID] = entry
		} else {
			ob.asks[resp.OrderID] = entry
		}
		ob.mu.Unlock()
	}

	if resp.FilledQty == resp.OrderedQty {
		ob.mu.Lock()
		delete(ob.bids, resp.OrderID)
		delete(ob.asks, resp.OrderID)
		ob.mu.Unlock()
	}

	return true
}

func (ob *OrderBook) validatePriceTimePriority(resp TargetResponse) bool {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	if resp.Side == "BUY" {
		var betterAsks []limitEntry
		for _, ask := range ob.asks {
			if ask.price < resp.ExecutionPrice {
				betterAsks = append(betterAsks, ask)
			}
		}
		if len(betterAsks) > 0 {
			sort.Slice(betterAsks, func(i, j int) bool {
				if betterAsks[i].price != betterAsks[j].price {
					return betterAsks[i].price < betterAsks[j].price
				}
				return betterAsks[i].sequence < betterAsks[j].sequence
			})
			best := betterAsks[0]
			log.Printf(
				"DEBUG FAIL (price-time): BUY %s executed at %.4f but resting ASK %s at %.4f (seq %d) was skipped",
				resp.OrderID, resp.ExecutionPrice, best.orderID, best.price, best.sequence,
			)
			return false
		}
	} else {
		var betterBids []limitEntry
		for _, bid := range ob.bids {
			if bid.price > resp.ExecutionPrice {
				betterBids = append(betterBids, bid)
			}
		}
		if len(betterBids) > 0 {
			sort.Slice(betterBids, func(i, j int) bool {
				if betterBids[i].price != betterBids[j].price {
					return betterBids[i].price > betterBids[j].price
				}
				return betterBids[i].sequence < betterBids[j].sequence
			})
			best := betterBids[0]
			log.Printf(
				"DEBUG FAIL (price-time): SELL %s executed at %.4f but resting BID %s at %.4f (seq %d) was skipped",
				resp.OrderID, resp.ExecutionPrice, best.orderID, best.price, best.sequence,
			)
			return false
		}
	}
	return true
}
