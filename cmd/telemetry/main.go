package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

type OrderEvent struct {
	OrderID      string `json:"order_id"`
	SentAt       int64  `json:"sent_at"`
	AckAt        int64  `json:"ack_at"`
	LatencyMs    int64  `json:"latency_ms"`
	StatusCode   int    `json:"status_code"`
	IsSuccessful bool   `json:"is_successful"`
}

func initDB() *sql.DB {
	host := os.Getenv("DB_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("DB_PORT")
	if port == "" {
		port = "5432"
	}
	user := os.Getenv("DB_USER")
	if user == "" {
		user = "iicpc_admin"
	}
	password := os.Getenv("DB_PASSWORD")
	if password == "" {
		log.Fatal("DB_PASSWORD environment variable not set")
	}
	dbName := os.Getenv("DB_NAME")
	if dbName == "" {
		dbName = "metrics_db"
	}
	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		user, password, host, port, dbName,
	)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Failed to connect to DB:", err)
	}
	// Configure connection pool for production stability
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	createTableSQL := `
	CREATE TABLE IF NOT EXISTS order_metrics (
		time TIMESTAMPTZ NOT NULL,
		order_id TEXT NOT NULL,
		latency_ms BIGINT NOT NULL,
		is_successful BOOLEAN NOT NULL
	);`
	db.Exec(createTableSQL)

	createHypertableSQL := `SELECT create_hypertable('order_metrics', 'time', if_not_exists => TRUE);`
	db.Exec(createHypertableSQL)

	return db
}

func flushBatchToDB(db *sql.DB, batch []OrderEvent) error {
	if len(batch) == 0 {
		return nil
	}

	txn, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer txn.Rollback()

	stmt, err := txn.Prepare(pq.CopyIn("order_metrics", "time", "order_id", "latency_ms", "is_successful"))
	if err != nil {
		return fmt.Errorf("prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, e := range batch {
		timestamp := time.Unix(0, e.SentAt)
		if _, err := stmt.Exec(timestamp, e.OrderID, e.LatencyMs, e.IsSuccessful); err != nil {
			return fmt.Errorf("exec row %s: %w", e.OrderID, err)
		}
	}

	if _, err := stmt.Exec(); err != nil {
		return fmt.Errorf("flush copy: %w", err)
	}

	if err := txn.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	fmt.Printf("Successfully bulk inserted %d rows.\n", len(batch))
	return nil
}

func main() {
	db := initDB()
	defer db.Close()

	// Ping connection immediately to fail-fast if DB is unavailable
	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	redpandaHost := os.Getenv("REDPANDA_HOST")
	if redpandaHost == "" {
		redpandaHost = "localhost:9092"
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{redpandaHost},
		GroupID:  "telemetry-ingester-group",
		Topic:    "order-events",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	// Graceful shutdown listener
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("Received signal: %v. Shutting down gracefully...", sig)
		reader.Close()
		db.Close()
		os.Exit(0)
	}()

	// 1. Create an internal Go channel to bridge the reader and the batcher
	msgChan := make(chan OrderEvent, 2000)

	// 2. Start a background worker that ONLY reads from Kafka
	go func() {
		retryDelay := 100 * time.Millisecond
		maxDelay := 5 * time.Second
		for {
			msg, err := reader.ReadMessage(context.Background())
			if err != nil {
				log.Printf("Kafka read error: %v. Retrying in %v...", err, retryDelay)
				time.Sleep(retryDelay)
				retryDelay *= 2
				if retryDelay > maxDelay {
					retryDelay = maxDelay
				}
				continue
			}

			retryDelay = 100 * time.Millisecond // Reset on success

			var event OrderEvent
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				log.Printf("Failed to unmarshal Kafka message: %v. Skipping.", err)
				continue
			}

			// Push the parsed event into our internal channel
			msgChan <- event
		}
	}()

	fmt.Println("Telemetry Ingester listening (Time + Size batching enabled)...")

	// Health check server for K8s Probes
	go func() {
		http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		http.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
			if err := db.Ping(); err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
		})
		log.Println("Health server listening on :8081")
		if err := http.ListenAndServe(":8081", nil); err != nil {
			log.Printf("Health server failed: %v", err)
		}
	}()

	batchSize := 1000
	batch := make([]OrderEvent, 0, batchSize)

	// 3. Create a timer that "ticks" every 5 seconds
	flushTimer := time.NewTicker(5 * time.Second)
	defer flushTimer.Stop()

	// 4. The main coordination loop
	for {
		// 'select' waits for WHICHEVER happens first: a message arrives, OR the timer ticks
		select {
		case event := <-msgChan:
			batch = append(batch, event)

			// Size-based flush
			if len(batch) >= batchSize {
				if err := flushBatchToDB(db, batch); err != nil {
					log.Printf("Failed to flush batch: %v", err)
				} else {
					batch = batch[:0]
				}

				// 5. CRITICAL: Restart the clock!
				flushTimer.Reset(5 * time.Second)
			}

		case <-flushTimer.C:
			// Time-based flush
			if len(batch) > 0 {
				fmt.Println("5 seconds passed. Flushing straggler events...")
				if err := flushBatchToDB(db, batch); err != nil {
					log.Printf("Failed to flush batch: %v", err)
				} else {
					batch = batch[:0]
				}
			}
		}
	}
}
