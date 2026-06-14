package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

// getEnvOrDefault safely retrieves an environment variable or falls back to a default
func getEnvOrDefault(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists && value != "" {
		return value
	}
	return fallback
}

func main() {
	log.Println("Starting Leaderboard Service...")

	// 1. Initialize TimescaleDB
	dbHost := getEnvOrDefault("DB_HOST", "localhost")
	dbPort := getEnvOrDefault("DB_PORT", "5432")
	dbUser := getEnvOrDefault("DB_USER", "iicpc_admin")
	dbPassword := os.Getenv("DB_PASSWORD")
	if dbPassword == "" {
		dbPassword = "supersecretpassword" // Safe local fallback matching docker-compose
	}
	dbName := getEnvOrDefault("DB_NAME", "metrics_db")

	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		dbUser, dbPassword, dbHost, dbPort, dbName,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Cannot reach TimescaleDB: %v", err)
	}
	log.Println("Successfully connected to TimescaleDB.")

	// 2. Initialize Redis Client
	redisHost := getEnvOrDefault("REDIS_HOST", "localhost:6379")
	rdb := redis.NewClient(&redis.Options{
		Addr: redisHost,
	})
	defer rdb.Close()

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("Cannot reach Redis: %v", err)
	}
	log.Println("Successfully connected to Redis.")

	// 3. Start Scoring Engine Loop (defined in scoring.go)
	go runScoringEngine(db, rdb)
	go listenAndBroadcast(rdb)

	// 4. Start HTTP / WebSocket Server
	// We bind to 8085 to avoid colliding with the Sandbox on 8081
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	http.HandleFunc("/ws", handleWebSocket)

	port := ":8085"
	log.Printf("Leaderboard Service HTTP listening on %s", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("HTTP Server failed: %v", err)
	}
}
