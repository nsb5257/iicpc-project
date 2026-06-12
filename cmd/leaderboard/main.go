// leaderboard/main.go
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
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

type LeaderboardStats struct {
	TeamName   string  `json:"team_name"`
	TPS        float64 `json:"tps"`
	P90Latency float64 `json:"p90_latency_ms"`
	Score      float64 `json:"composite_score"`
}

type ScoreSnapshot struct {
	mu    sync.RWMutex
	tps   float64
	p90   float64
	score float64
}

var scoreSnap = &ScoreSnapshot{}

// 1. WebSocket Upgrader: Upgrades standard HTTP connections to persistent WebSockets
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all connections for local development
	},
}

// 2. Client Registry: Keeps track of all currently connected web browsers
var clients = make(map[*websocket.Conn]bool)
var clientsMutex sync.Mutex

func main() {
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

	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, password, host, port, dbName)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	defer db.Close()

	redisHost := os.Getenv("REDIS_HOST")
	if redisHost == "" {
		redisHost = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisHost})
	defer rdb.Close()

	// Health Validation (Fail-Fast)
	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("Failed to ping Redis: %v", err)
	}

	go runScoringEngine(db, rdb)
	go listenAndBroadcast(rdb)

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})
	http.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if err := db.Ping(); err != nil {
			http.Error(w, `{"error":"db_down"}`, http.StatusServiceUnavailable)
			return
		}
		if err := rdb.Ping(context.Background()).Err(); err != nil {
			http.Error(w, `{"error":"redis_down"}`, http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ready"}`))
	})

	http.HandleFunc("/ws", handleWebSocket)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})

	server := &http.Server{Addr: ":8081"}

	// Graceful Shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("Received signal: %v. Shutting down gracefully...", sig)

		clientsMutex.Lock()
		for client := range clients {
			client.Close()
		}
		clients = make(map[*websocket.Conn]bool)
		clientsMutex.Unlock()

		server.Shutdown(context.Background())
	}()

	fmt.Println("Leaderboard Web UI running on :8081")
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("HTTP Server failed: %v", err)
	}
}

// This is the exact same scoring logic we wrote previously
func runScoringEngine(db *sql.DB, rdb *redis.Client) {
	ctx := context.Background()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		query := `
		SELECT 
			'Contestant Alpha' as team_name,
			COALESCE(COUNT(*) / GREATEST(EXTRACT(EPOCH FROM (MAX(time) - MIN(time))), 1), 0) as tps,
			COALESCE(percentile_cont(0.90) WITHIN GROUP (ORDER BY latency_ms), 0) as p90_latency
		FROM order_metrics
		WHERE is_successful = TRUE 
		AND time >= (SELECT MIN(time) FROM order_metrics);`
		var currentTPS, currentP90 float64
		if err := db.QueryRow(query).Scan(&currentTPS, &currentP90); err != nil {
			continue
		}

		// Only calculate a new score if the load test is actively running
		if currentTPS > 0 {
			effectiveP90 := currentP90
			if effectiveP90 < 1 {
				effectiveP90 = 1
			}
			currentScore := (currentTPS * 100) / effectiveP90

			// Update our thread-safe state
			scoreSnap.mu.Lock()
			scoreSnap.tps = currentTPS
			scoreSnap.p90 = currentP90
			scoreSnap.score = currentScore
			scoreSnap.mu.Unlock()
		}

		scoreSnap.mu.RLock()
		currentBestScore := scoreSnap.score
		currentBestTPS := scoreSnap.tps
		currentBestP90 := scoreSnap.p90
		scoreSnap.mu.RUnlock()

		// If bestScore is still 0, it means the test hasn't started yet. Skip broadcasting.
		if currentBestScore == 0 {
			continue
		}

		// Broadcast the "Last Known Good State"
		stats := LeaderboardStats{
			TeamName:   "Contestant Alpha",
			TPS:        currentBestTPS,
			P90Latency: currentBestP90,
			Score:      currentBestScore,
		}

		jsonData, _ := json.Marshal(stats)
		if err := rdb.Publish(ctx, "live-scores", jsonData).Err(); err != nil {
			log.Printf("Redis publish error: %v", err)
		}
	}
}

// listenAndBroadcast acts as our radio receiver
func listenAndBroadcast(rdb *redis.Client) {
	ctx := context.Background()
	pubsub := rdb.Subscribe(ctx, "live-scores")
	defer pubsub.Close()

	// Wait for messages from Redis
	for {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			log.Printf("Redis Receive Error: %v", err)
			continue
		}

		// Safely copy clients to avoid holding the lock during slow network writes
		clientsMutex.Lock()
		clientsCopy := make([]*websocket.Conn, 0, len(clients))
		for client := range clients {
			clientsCopy = append(clientsCopy, client)
		}
		clientsMutex.Unlock()

		for _, client := range clientsCopy {
			// Prevent a slow client from blocking the whole broadcast loop
			client.SetWriteDeadline(time.Now().Add(5 * time.Second))
			err := client.WriteMessage(websocket.TextMessage, []byte(msg.Payload))
			client.SetWriteDeadline(time.Time{})

			if err != nil {
				client.Close()

				clientsMutex.Lock()
				delete(clients, client)
				clientsMutex.Unlock()
			}
		}
	}
}

// handleWebSocket manages new browser connections
func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket Upgrade Error: %v", err)
		return
	}

	// Ensure cleanup executes no matter how the connection drops
	defer func() {
		clientsMutex.Lock()
		delete(clients, ws)
		clientsMutex.Unlock()
		ws.Close()
	}()

	clientsMutex.Lock()
	clients[ws] = true // Register the new client
	clientsMutex.Unlock()
	fmt.Println("New browser connected to live feed!")

	// Keep the connection alive until the client closes it
	for {
		if _, _, err := ws.ReadMessage(); err != nil {
			break // Exit loop if client disconnects
		}
	}
}
