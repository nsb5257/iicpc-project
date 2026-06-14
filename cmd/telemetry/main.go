package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"iicpc-platform/pkg/events"

	"github.com/segmentio/kafka-go"
)

func main() {
	log.Println("Starting Telemetry Service...")

	db := initDB()
	defer db.Close()

	orderBook := NewOrderBook()

	redpandaHost := os.Getenv("REDPANDA_HOST")
	if redpandaHost == "" {
		redpandaHost = "localhost:9092"
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{redpandaHost},
		GroupID:  "telemetry-group",
		Topic:    "order-events",
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	defer reader.Close()

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})
	go func() {
		log.Println("Telemetry HTTP Server listening on :8083...")
		if err := http.ListenAndServe(":8083", nil); err != nil {
			log.Fatalf("Telemetry HTTP server failed: %v", err)
		}
	}()

	batch := make([]ValidatedOrder, 0, 1000)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	log.Println("Telemetry engine active. Consuming messages...")

	for {
		select {
		case <-ticker.C:
			if len(batch) > 0 {
				if err := flushBatchToDB(db, batch); err != nil {
					log.Printf("Failed to flush batch to DB: %v", err)
				} else {
					batch = batch[:0]
				}
			}
		default:
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			m, err := reader.ReadMessage(ctx)
			cancel()

			if err != nil {
				continue
			}

			var event events.OrderEvent
			if err := json.Unmarshal(m.Value, &event); err != nil {
				log.Printf("Failed to unmarshal OrderEvent: %v", err)
				continue
			}

			isCorrect := orderBook.ValidateOrder(event)

			batch = append(batch, ValidatedOrder{
				Event:     event,
				IsCorrect: isCorrect,
			})

			if len(batch) >= 1000 {
				if err := flushBatchToDB(db, batch); err != nil {
					log.Printf("Failed to flush batch to DB: %v", err)
				} else {
					batch = batch[:0]
				}
			}
		}
	}
}
