package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go" // The Kafka library
)

type Order struct {
	ID        string  `json:"id"`
	Type      string  `json:"type"`
	Side      string  `json:"side"`
	Price     float64 `json:"price"`
	Quantity  int     `json:"quantity"`
	Timestamp int64   `json:"timestamp"`
}

// OrderEvent is what we send to Redpanda
type OrderEvent struct {
	OrderID      string `json:"order_id"`
	SentAt       int64  `json:"sent_at"`
	AckAt        int64  `json:"ack_at"`
	LatencyMs    int64  `json:"latency_ms"`
	StatusCode   int    `json:"status_code"`
	IsSuccessful bool   `json:"is_successful"`
}

// worker now requires a Kafka writer
func worker(id int, jobs <-chan Order, tokens <-chan time.Time, wg *sync.WaitGroup, targetURL string, kafkaWriter *kafka.Writer) {
	defer wg.Done()
	client := &http.Client{Timeout: 5 * time.Second}

	for order := range jobs {
		<-tokens

		jsonData, err := json.Marshal(order)
		if err != nil {
			log.Printf("[Bot %d] Failed to marshal order %s: %v", id, order.ID, err)
			continue
		}

		req, err := http.NewRequest("POST", targetURL, bytes.NewBuffer(jsonData))
		if err != nil {
			log.Printf("[Bot %d] Failed to create request: %v", id, err)
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		sentTime := time.Now()
		resp, err := client.Do(req)
		ackTime := time.Now()

		statusCode := 0
		isSuccess := false
		if err == nil {
			statusCode = resp.StatusCode
			isSuccess = (statusCode == 200 || statusCode == 201)
			resp.Body.Close()
		}

		latency := ackTime.Sub(sentTime).Milliseconds()

		event := OrderEvent{
			OrderID:      order.ID,
			SentAt:       sentTime.UnixNano(),
			AckAt:        ackTime.UnixNano(),
			LatencyMs:    latency,
			StatusCode:   statusCode,
			IsSuccessful: isSuccess,
		}

		eventBytes, err := json.Marshal(event)
		if err != nil {
			log.Printf("[Bot %d] Failed to marshal telemetry event: %v", id, err)
			continue
		}

		// Implement retry with exponential backoff
		retryDelay := 100 * time.Millisecond
		for attempt := 0; attempt < 3; attempt++ {
			err = kafkaWriter.WriteMessages(context.Background(),
				kafka.Message{
					Key:   []byte(order.ID),
					Value: eventBytes,
				},
			)
			if err == nil {
				log.Printf("[Bot %d] Sent order %s | Latency: %dms", id, order.ID, latency)
				break
			}
			log.Printf("[Bot %d] Kafka write failed (attempt %d/3): %v", id, attempt+1, err)
			if attempt < 2 {
				time.Sleep(retryDelay)
				retryDelay *= 2
			}
		}
	}
}

func main() {
	const numBots = 50
	const numOrders = 1000
	const targetTPS = 100

	targetURL := os.Getenv("TARGET_URL")
	if targetURL == "" {
		targetURL = "http://localhost:8080/order"
	}

	redpandaHost := os.Getenv("REDPANDA_HOST")
	if redpandaHost == "" {
		redpandaHost = "localhost:9092"
	}

	// Initialize the Kafka (Redpanda) Writer
	kafkaWriter := &kafka.Writer{
		Addr:         kafka.TCP(redpandaHost),
		Topic:        "order-events", // (Ensure this matches your topic name)
		Balancer:     &kafka.LeastBytes{},
		Async:        false,            // Force Go to wait for Redpanda's "ACK" (Acknowledgement)
		MaxAttempts:  5,                // If Redpanda fails, retry up to 5 times automatically
		WriteTimeout: 10 * time.Second, // Don't hang forever if the network drops
		ReadTimeout:  10 * time.Second,
	}
	// Make sure we close the connection when the program exits
	defer kafkaWriter.Close()

	jobs := make(chan Order, 500)
	tokens := make(chan time.Time, 10)
	ticker := time.NewTicker(time.Second / time.Duration(targetTPS))

	go func() {
		for t := range ticker.C {
			select {
			case tokens <- t:
			default:
			}
		}
	}()

	var wg sync.WaitGroup

	go func() {
		http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		http.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		log.Println("Health server listening on :8081")
		if err := http.ListenAndServe(":8081", nil); err != nil {
			log.Printf("Health server failed: %v", err)
		}
	}()

	fmt.Println("Spawning Bot Fleet with Telemetry...")
	for w := 1; w <= numBots; w++ {
		wg.Add(1)
		go worker(w, jobs, tokens, &wg, targetURL, kafkaWriter)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("Received signal: %v. Shutting down gracefully...", sig)
		kafkaWriter.Close()
		os.Exit(0)
	}()

	for i := 1; i <= numOrders; i++ {
		jobs <- Order{
			ID:        fmt.Sprintf("ORD-%d", i),
			Type:      "LIMIT",
			Side:      "SELL",
			Price:     150.25,
			Quantity:  5,
			Timestamp: time.Now().UnixNano(),
		}
	}

	close(jobs)
	wg.Wait()
	ticker.Stop()
	fmt.Println("Load test complete.")
}
