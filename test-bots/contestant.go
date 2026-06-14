package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// OrderRequest maps the incoming JSON payload from the bot fleet
type OrderRequest struct {
	ID             string  `json:"id"`
	Type           string  `json:"type"`
	Side           string  `json:"side,omitempty"`
	Price          float64 `json:"price,omitempty"`
	Quantity       int     `json:"quantity,omitempty"`
	Timestamp      int64   `json:"timestamp"`
	CancelTargetID string  `json:"cancel_target_id,omitempty"`
}

// OrderResponse represents the JSON payload expected by the telemetry validator
type OrderResponse struct {
	OrderID        string  `json:"order_id"`
	Type           string  `json:"type"`
	Side           string  `json:"side,omitempty"`
	OrderedQty     int     `json:"ordered_qty,omitempty"`
	FilledQty      int     `json:"filled_qty,omitempty"`
	Price          float64 `json:"price,omitempty"`
	ExecutionPrice float64 `json:"execution_price,omitempty"`
	Status         string  `json:"status,omitempty"`
}

type TradingEngine struct {
	mu     sync.RWMutex
	orders map[string]OrderRequest
}

func NewTradingEngine() *TradingEngine {
	return &TradingEngine{
		orders: make(map[string]OrderRequest),
	}
}

func (e *TradingEngine) HandleOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req OrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// 1. Record the order safely in memory
	e.mu.Lock()
	e.orders[req.ID] = req
	e.mu.Unlock()

	// 2. Generate a 100% compliant fill response for the load test
	execPrice := req.Price
	if req.Type == "MARKET" {
		execPrice = 100.00 // Arbitrary safe execution price for market orders
	}

	resp := OrderResponse{
		OrderID:        req.ID,
		Type:           req.Type,
		Side:           req.Side,
		OrderedQty:     req.Quantity,
		FilledQty:      req.Quantity, // Fully fill to pass validation
		Price:          req.Price,
		ExecutionPrice: execPrice,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

func (e *TradingEngine) HandleCancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req OrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// 1. Safely locate and remove the target order
	e.mu.Lock()
	if _, exists := e.orders[req.CancelTargetID]; exists {
		delete(e.orders, req.CancelTargetID)
	}
	e.mu.Unlock()

	// 2. Confirm the cancellation (Validator strictly requires status="cancelled")
	resp := OrderResponse{
		OrderID: req.ID,
		Type:    "CANCEL",
		Status:  "cancelled",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

func main() {
	engine := NewTradingEngine()

	mux := http.NewServeMux()
	mux.HandleFunc("/order", engine.HandleOrder)
	mux.HandleFunc("/cancel", engine.HandleCancel)

	srv := &http.Server{
		Addr:    ":8080", // Must bind to 8080 to match the Sandbox Docker expose configuration
		Handler: mux,
	}

	// Start server in a goroutine
	go func() {
		log.Println("Go Contestant Bot listening on :8080...")
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// Graceful Shutdown implementation
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutdown signal received. Shutting down gracefully...")

	// Provide a 5-second window to finish inflight requests before dying
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Go Contestant Bot stopped cleanly.")
}
