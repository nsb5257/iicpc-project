package main

import (
	"context"
	"encoding/json"
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

	if req.Endpoint == "" || req.SubmissionID == "" || req.NumBots <= 0 || req.NumOrders <= 0 || req.TPS <= 0 {
		http.Error(w, "Missing or invalid parameters", http.StatusBadRequest)
		return
	}

	// Offload the heavy lifting to a background goroutine so we return quickly
	go startLoadTest(req)

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
