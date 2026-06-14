package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

// RunRequest exactly matches the required cross-file contract
type RunRequest struct {
	RunID        string `json:"run_id,omitempty"`
	Endpoint     string `json:"endpoint"`
	SubmissionID string `json:"submission_id"`
	NumBots      int    `json:"num_bots"`
	NumOrders    int    `json:"num_orders"`
	TPS          int    `json:"tps"`
}

var kafkaWriter *kafka.Writer

// ensureTopic connects to the Kafka cluster and forces topic creation with specific partitions
func ensureTopic(broker string, topic string, partitions int) error {
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}

	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     partitions,
			ReplicationFactor: 1,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		// Log the error but don't fail, as it often means the topic already exists
		log.Printf("Topic creation status: %v", err)
	}
	return nil
}

const (
	maxNumBots   = 1000
	maxNumOrders = 1000000
	maxTPS       = 50000
)

func runHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req RunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.Endpoint == "" || req.SubmissionID == "" {
		http.Error(w, "Missing or invalid parameters", http.StatusBadRequest)
		return
	}
	if req.NumBots <= 0 || req.NumBots > maxNumBots {
		http.Error(w, fmt.Sprintf("num_bots must be between 1 and %d", maxNumBots), http.StatusBadRequest)
		return
	}
	if req.NumOrders <= 0 || req.NumOrders > maxNumOrders {
		http.Error(w, fmt.Sprintf("num_orders must be between 1 and %d", maxNumOrders), http.StatusBadRequest)
		return
	}
	if req.TPS <= 0 || req.TPS > maxTPS {
		http.Error(w, fmt.Sprintf("tps must be between 1 and %d", maxTPS), http.StatusBadRequest)
		return
	}
	if req.RunID == "" {
		req.RunID = fmt.Sprintf("%s-%d", req.SubmissionID, time.Now().UnixNano())
	}

	// Offload the heavy lifting to a background goroutine so we return quickly
	go func(rq RunRequest) {
		if err := startLoadTest(rq); err != nil {
			log.Printf("Load test failed for %s: %v", rq.RunID, err)
		}
	}(req)

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"status":"test_started"}`))
}

func main() {
	redpandaHost := os.Getenv("REDPANDA_HOST")
	if redpandaHost == "" {
		redpandaHost = "localhost:9092"
	}

	// 1. Fulfill Critical Requirement: Ensure 4 partitions exist on startup
	if err := ensureTopic(redpandaHost, "order-events", 4); err != nil {
		log.Printf("Warning: Failed to ensure topic partitions: %v", err)
	}

	// 2. Initialize Kafka Writer
	kafkaWriter = &kafka.Writer{
		Addr:         kafka.TCP(redpandaHost),
		Topic:        "order-events",
		Balancer:     &kafka.LeastBytes{},
		Async:        false,
		MaxAttempts:  5,
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
	}
	defer kafkaWriter.Close()

	// 3. Setup the /run server on :8080
	runMux := http.NewServeMux()
	runMux.HandleFunc("/run", runHandler)

	runServer := &http.Server{
		Addr:    ":8080",
		Handler: runMux,
	}

	go func() {
		log.Println("Fleet HTTP server listening on :8080. Waiting for /run POST requests...")
		if err := runServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Run Server failed: %v", err)
		}
	}()

	// 4. Setup the health check server on :8084
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	healthMux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	healthServer := &http.Server{
		Addr:    ":8084",
		Handler: healthMux,
	}

	go func() {
		log.Println("Fleet Health server listening on :8084...")
		if err := healthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Health Server failed: %v", err)
		}
	}()

	// 5. Graceful Shutdown for BOTH servers
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down Fleet gracefully...")
	runServer.Shutdown(context.Background())
	healthServer.Shutdown(context.Background())
}
