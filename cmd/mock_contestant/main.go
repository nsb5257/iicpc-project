package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// The structure the bots will send
type Order struct {
	ID        string  `json:"id"`
	Type      string  `json:"type"`
	Side      string  `json:"side"`
	Price     float64 `json:"price"`
	Quantity  int     `json:"quantity"`
	Timestamp int64   `json:"timestamp"`
}

func orderHandler(w http.ResponseWriter, r *http.Request) {
	var order Order
	err := json.NewDecoder(r.Body).Decode(&order)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Simulate the contestant's code "thinking" (e.g., checking an orderbook)
	// We randomly sleep between 5ms and 35ms to generate realistic latency metrics
	thinkingTime := time.Duration(rand.Intn(30)+5) * time.Millisecond
	time.Sleep(thinkingTime)

	// Acknowledge the order successfully
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"filled"}`))
}

func main() {
	// 1. Setup the routing
	mux := http.NewServeMux()
	mux.HandleFunc("/order", orderHandler)
	
	// 2. Add Kubernetes Health Probes
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ready"}`))
	})

	// 3. Configure a hardened HTTP server with strict timeouts
	server := &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  5 * time.Second,   // Max time to read the incoming request
		WriteTimeout: 10 * time.Second,  // Max time to write the response
		IdleTimeout:  15 * time.Second,  // Max time to keep a connection alive
	}

	// 4. Setup Graceful Shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	go func() {
		sig := <-sigChan
		log.Printf("Received signal: %v. Allowing in-flight orders to finish before shutting down...", sig)
		
		// Give the server up to 5 seconds to finish processing current orders
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		
		if err := server.Shutdown(ctx); err != nil {
			log.Printf("Forced shutdown: %v", err)
		}
	}()

	fmt.Println("Mock Contestant Engine running on :8080")
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("HTTP Server failed: %v", err)
	}
}