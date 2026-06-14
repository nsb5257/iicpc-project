package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"iicpc-platform/pkg/events"

	"github.com/lib/pq"
)

// ValidatedOrder pairs the raw event with its correctness state for DB ingestion
type ValidatedOrder struct {
	Event     events.OrderEvent
	IsCorrect bool
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
		password = "supersecretpassword"
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

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	createTableSQL := `
	CREATE TABLE IF NOT EXISTS order_metrics (
		time TIMESTAMPTZ NOT NULL,
		run_id TEXT NOT NULL,
		order_id TEXT NOT NULL,
		submission_id TEXT NOT NULL,
		latency_ms BIGINT NOT NULL,
		is_successful BOOLEAN NOT NULL,
		is_correct BOOLEAN NOT NULL
	);`
	if _, err := db.Exec(createTableSQL); err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	createHypertableSQL := `SELECT create_hypertable('order_metrics', 'time', if_not_exists => TRUE);`
	if _, err := db.Exec(createHypertableSQL); err != nil {
		log.Printf("Hypertable creation status: %v", err)
	}

	createIndexSQL := `
	CREATE INDEX IF NOT EXISTS idx_metrics_run_time 
	ON order_metrics (run_id, time DESC) 
	WHERE is_successful = TRUE AND is_correct = TRUE;`
	if _, err := db.Exec(createIndexSQL); err != nil {
		log.Printf("Failed to create index: %v", err)
	}

	return db
}

func flushBatchToDB(db *sql.DB, batch []ValidatedOrder) error {
	if len(batch) == 0 {
		return nil
	}

	txn, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer txn.Rollback()

	stmt, err := txn.Prepare(pq.CopyIn("order_metrics", "time", "run_id", "order_id", "submission_id", "latency_ms", "is_successful", "is_correct"))
	if err != nil {
		return fmt.Errorf("prepare statement: %w", err)
	}

	for _, item := range batch {
		timestamp := time.Unix(0, item.Event.SentAt)
		if _, err := stmt.Exec(
			timestamp,
			item.Event.RunID,
			item.Event.OrderID,
			item.Event.SubmissionID,
			item.Event.LatencyMs,
			item.Event.IsSuccessful,
			item.IsCorrect,
		); err != nil {
			return fmt.Errorf("exec row %s: %w", item.Event.OrderID, err)
		}
	}

	if _, err := stmt.Exec(); err != nil {
		_ = stmt.Close()
		return fmt.Errorf("flush copy: %w", err)
	}
	if err := stmt.Close(); err != nil {
		return fmt.Errorf("close copy statement: %w", err)
	}

	if err := txn.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	log.Printf("Successfully bulk inserted %d rows.", len(batch))
	return nil
}
